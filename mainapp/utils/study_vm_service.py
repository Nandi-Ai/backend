import os
import logging
import botocore
import multiprocessing
from jinja2 import Template
from concurrent.futures.thread import ThreadPoolExecutor

from mainapp import settings
from mainapp.exceptions import (
    InvalidEc2Status,
    Route53Error,
    Ec2Error,
    InvalidChangeBatchError,
    NoSuchHostedZoneError,
    InvalidInputError,
    PriorRequestNotCompleteError,
    LaunchTemplateFailedError,
)
from mainapp.models import Study
from mainapp.utils.decorators import (
    with_ec2_resource,
    with_ec2_client,
    with_route53_client,
)
from mainapp.utils.aws_utils import (
    Route53Actions,
    AWS_EC2_STARTING,
    AWS_EC2_RUNNING,
    AWS_EC2_STOPPING,
    AWS_EC2_STOPPED,
    AWS_EC2_SHUTTING_DOWN,
    AWS_EC2_TERMINATED,
    get_instance,
    delete_route53,
    create_route53,
)
from mainapp.utils.monitoring import handle_event, MonitorEvents
from mainapp.utils.status_monitoring_event_map import status_monitoring_event_map

logger = logging.getLogger(__name__)
executor = ThreadPoolExecutor(max_workers=multiprocessing.cpu_count() * 2 - 1)

ROUTE53_ACTION_MAPPING = {
    Route53Actions.DELETE: {"log_message": "Deleting", "method": delete_route53},
    Route53Actions.CREATE: {"log_message": "Creating", "method": create_route53},
}


USER_DATA_TEMPLATE_PATH = os.path.join(os.path.dirname(__file__), "user_data.jinja")
ALLOWED_STATUSES = [enum[1] for enum in Study.possible_statuses_for_study]
STATUS_ARGS = {
    "start": {
        "instance_method": "start",
        "expected_statuses": [AWS_EC2_STOPPED],
        "blocker_statuses": [AWS_EC2_SHUTTING_DOWN, AWS_EC2_STOPPING],
        "killer_statuses": [AWS_EC2_STARTING, AWS_EC2_RUNNING],
        "toggle_status": Study.VM_STARTING,
    },
    "stop": {
        "instance_method": "stop",
        "expected_statuses": [AWS_EC2_RUNNING],
        "blocker_statuses": [AWS_EC2_STARTING],
        "killer_statuses": [AWS_EC2_SHUTTING_DOWN, AWS_EC2_STOPPING, AWS_EC2_STOPPED],
        "toggle_status": Study.VM_STOPPING,
    },
    "terminate": {
        "instance_method": "terminate",
        "expected_statuses": [
            AWS_EC2_RUNNING,
            AWS_EC2_STARTING,
            AWS_EC2_STOPPED,
            AWS_EC2_STOPPED,
        ],
        "blocker_statuses": [],
        "killer_statuses": [],
        "toggle_status": Study.STUDY_DELETED,
    },
}

INSTANCE_WAIT_MAPPING = {"start": "wait_until_stopped", "stop": "wait_until_running"}

STUDY_STATUS_BASED_ON_INSTANCE_NAME = {
    AWS_EC2_RUNNING: Study.VM_ACTIVE,
    AWS_EC2_STARTING: Study.VM_STARTING,
    AWS_EC2_STOPPING: Study.VM_STOPPING,
    AWS_EC2_STOPPED: Study.VM_STOPPED,
    AWS_EC2_SHUTTING_DOWN: Study.VM_STOPPING,
    AWS_EC2_TERMINATED: Study.STUDY_DELETED,
}


def update_study_state(study, status):
    monitor_event = status_monitoring_event_map.get(status, None)
    if monitor_event:
        handle_event(monitor_event, {"study": study})
    logger.info(f"Updating study {study.name} instance status to be {status}")
    if status not in ALLOWED_STATUSES:
        raise InvalidEc2Status(status)
    study.status = status
    study.save()


def wait_until_stopped(instance, study):
    try:
        """
        Waits until this Instance is stopped. This method calls EC2.Waiter.instance_stopped.wait() which polls.
        EC2.Client.describe_instances() every 15 seconds until a successful state is reached.
        An error is returned after 40 failed checks.
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2.html#EC2.Instance.wait_until_stopped
        """
        instance.wait_until_stopped()
        update_study_state(study, Study.VM_STOPPED)
    except Exception as e:
        logger.exception("Error trying to wait until instance stopped", e)
        update_study_state(study, Study.ST_ERROR)


def get_and_update_study_instance(boto3_client, study, status_filter):
    """
    get the study instance, and update the database accordingly to the VM status
    :param boto3_client: boto3 client with access to the study instance
    :param study: Study Django model
    :param status_filter: A list of ec2 instance statuses to filter by
    :return: aws ec2 instance object
    """
    execution_token = study.execution.execution_user.email.split("@")[0]
    instance = get_instance(boto3_client, execution_token, status_filter)
    state_name = instance.state.get("Name")
    if not state_name:
        raise InvalidEc2Status(state_name)
    update_study_state(study, STUDY_STATUS_BASED_ON_INSTANCE_NAME[state_name])
    return instance


@with_ec2_resource
def toggle_study_vm(
    boto3_client,
    org_name,
    study,
    instance_method,
    expected_statuses,
    blocker_statuses,
    killer_statuses,
    toggle_status,
):
    statuses_to_filter = expected_statuses + blocker_statuses + killer_statuses
    instance = get_and_update_study_instance(boto3_client, study, statuses_to_filter)
    state_name = instance.state.get("Name")
    if state_name in killer_statuses:
        logger.info(
            f"Aborting changing study {study.id} ({study.name}) instance {study.execution.execution_user.email} "
            f"state to {toggle_status} as of killer_statuses"
        )
        return
    if state_name in blocker_statuses:
        logger.info(
            f"Sleeping before changing study {study.id} ({study.name}) instance {study.execution.execution_user.email} "
            f"state to {toggle_status} as of blocker_statuses"
        )
        getattr(instance, INSTANCE_WAIT_MAPPING.get(instance_method))()
    getattr(instance, instance_method)()
    update_study_state(study, toggle_status)

    if toggle_status == Study.VM_STOPPING:
        # TODO switch to EVENT-DRIVEN. if server is shutdown during the process the status won't be updated!
        executor.submit(wait_until_stopped, instance, study)


def delete_study(study):
    org_name = study.organization.name

    try:
        change_resource_record_sets(
            execution=study.execution.execution_user.email,
            org_name=study.organization.name,
            action=Route53Actions.DELETE,
        )
    except Route53Error as e:
        logger.warning(str(e))

    try:
        toggle_study_vm(
            org_name=study.organization.name, study=study, **STATUS_ARGS["terminate"]
        )
    except Ec2Error as e:
        logger.warning(str(e))

    handle_event(MonitorEvents.EVENT_STUDY_DELETED, {"study": study})


@with_ec2_client
def setup_study_workspace(boto3_client, org_name, execution_token, study_id):
    organization_value = settings.ORG_VALUES[org_name]
    org_region = organization_value["AWS_REGION"]
    org_fs_server = organization_value["FS_SERVER"]
    lynx_org_value = settings.ORG_VALUES[settings.LYNX_ORGANIZATION]

    with open(USER_DATA_TEMPLATE_PATH) as user_data_template_file:
        user_data_template = Template(user_data_template_file.read())
        user_data = user_data_template.render(
            execution_token=execution_token,
            org_region=org_region,
            fs_server=org_fs_server,
            lynx_account=lynx_org_value["ACCOUNT_NUMBER"],
            lynx_region=lynx_org_value["AWS_REGION"],
            backend=settings.BACKEND_URL,
            notebook_image=settings.NOTEBOOK_IMAGE,
            study_id=study_id,
        )

    try:
        boto3_client.run_instances(
            LaunchTemplate={"LaunchTemplateName": "Jupyter-Notebook"},
            TagSpecifications=[
                {
                    "ResourceType": "instance",
                    "Tags": [{"Key": "Name", "Value": f"jupyter-{execution_token}"}],
                },
                {
                    "ResourceType": "volume",
                    "Tags": [
                        {"Key": "Name", "Value": f"ebs-{execution_token}"},
                        {"Key": "Volume-Type", "Value": "jupyter-ebs"},
                    ],
                },
            ],
            UserData=user_data,
            MinCount=1,
            MaxCount=1,
            BlockDeviceMappings=[
                {
                    "DeviceName": "/dev/sdf",
                    "Ebs": {
                        "DeleteOnTermination": True,
                        "VolumeSize": 100,
                        "VolumeType": "standard",
                        "KmsKeyId": "alias/aws/ebs",
                        "Encrypted": True,
                    },
                }
            ],
        )
    except botocore.exceptions.ClientError as ce:
        logger.error(f"Failed to launch instance from template due to error: {ce}")
        raise LaunchTemplateFailedError("Jupyter-Notebook")


@with_route53_client
def change_resource_record_sets(boto3_client, org_name, execution, action):
    organization_value = settings.ORG_VALUES[org_name]
    org_hosted_zone = organization_value["HOSTED_ZONE_ID"]
    record_name = f'{execution.split("@")[0]}.{organization_value["DOMAIN"]}'
    logger.info(
        f"{ROUTE53_ACTION_MAPPING[action]['log_message']} DNS record {record_name} in organization {org_name}"
    )

    try:
        response = boto3_client.list_resource_record_sets(
            HostedZoneId=org_hosted_zone,
            StartRecordType="A",
            StartRecordName=record_name,
        )
        ROUTE53_ACTION_MAPPING[action]["method"](
            boto3_client=boto3_client,
            existing_records=response["ResourceRecordSets"],
            record_name=record_name,
            org_name=org_name,
            hosted_zone=org_hosted_zone,
        )
    except botocore.exceptions.ClientError as error:
        logger.error(
            f"Error: '{error}' " f"on record {record_name} in organization {org_name}"
        )
        boto_error = error.response.get("Error", {}).get("Code", "NoResponseCode")
        if boto_error == "NoSuchHostedZone":
            raise NoSuchHostedZoneError(org_hosted_zone)
        if boto_error == "InvalidChangeBatch":
            raise InvalidChangeBatchError(record_name)
        if boto_error == "InvalidInput":
            raise InvalidInputError(record_name)
        if boto_error == "PriorRequestNotComplete":
            raise PriorRequestNotCompleteError()
        raise Route53Error(f"Error {boto_error}: Failed record deletion")
