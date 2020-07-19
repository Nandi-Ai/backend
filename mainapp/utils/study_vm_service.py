import logging
import multiprocessing
from concurrent.futures.thread import ThreadPoolExecutor
from time import sleep

from mainapp.exceptions import (
    TooManyInstancesError,
    InstanceNotFound,
    InstanceTerminated,
    InvalidEc2Status,
    BucketNotFound,
    PolicyNotFound,
    RoleNotFound,
    Route53Error,
    Ec2Error,
)
from mainapp.models import Study
from mainapp.utils.decorators import with_ec2_resource
from mainapp.utils.elasticsearch_service import ElasticsearchService, MonitorEvents
from mainapp.utils.lib import delete_route53

logger = logging.getLogger(__name__)

executor = ThreadPoolExecutor(max_workers=multiprocessing.cpu_count() * 2 - 1)

AWS_EC2_STARTING = "pending"
AWS_EC2_RUNNING = "running"
AWS_EC2_STOPPING = "stopping"
AWS_EC2_SHUTTING_DOWN = "shutting-down"
AWS_EC2_STOPPED = "stopped"
AWS_EC2_TERMINATED = "terminated"
ALLOWED_STATUSES = [enum[1] for enum in Study.possible_statuses_for_study]
STATUS_ARGS = {
    "start": {
        "instance_method": "start",
        "blocker_statuses": [AWS_EC2_SHUTTING_DOWN, AWS_EC2_STOPPING],
        "killer_statuses": [AWS_EC2_STARTING, AWS_EC2_RUNNING],
        "toggle_status": Study.VM_STARTING,
    },
    "stop": {
        "instance_method": "stop",
        "blocker_statuses": [AWS_EC2_STARTING],
        "killer_statuses": [AWS_EC2_SHUTTING_DOWN, AWS_EC2_STOPPING, AWS_EC2_STOPPED],
        "toggle_status": Study.VM_STOPPING,
    },
    "terminate": {
        "instance_method": "terminate",
        "blocker_statuses": [],
        "killer_statuses": [],
        "toggle_status": Study.STUDY_DELETED,
    },
}


def update_study_state(study, status):
    logger.info(f"Updating study {study.name} instance status to be {status}")
    if status not in ALLOWED_STATUSES:
        raise InvalidEc2Status(status)
    study.status = status
    study.save()


def get_instance(boto_resource, execution_token):
    inst_name = f"jupyter-{execution_token}"
    instances = list(
        boto_resource.instances.filter(
            Filters=[{"Name": "tag:Name", "Values": [inst_name]}]
        )
    )
    if not instances:
        raise InstanceNotFound(inst_name)
    elif len(instances) > 1:
        raise TooManyInstancesError(inst_name)
    elif instances[0].state["Name"] == AWS_EC2_TERMINATED:
        raise InstanceTerminated(inst_name)

    return instances[0]


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


@with_ec2_resource
def toggle_study_vm(
    boto3_client,
    org_name,
    study,
    instance_method,
    blocker_statuses,
    killer_statuses,
    toggle_status,
):
    execution_token = study.execution.execution_user.email.split("@")[0]
    instance = get_instance(boto3_client, execution_token)
    state_name = instance.state.get("Name")
    if not state_name:
        raise InvalidEc2Status(state_name)
    if state_name in killer_statuses:
        return
    if state_name in blocker_statuses:
        sleep(30)
    getattr(instance, instance_method)()
    update_study_state(study, toggle_status)

    if toggle_status == Study.VM_STOPPING:
        # TODO switch to EVENT-DRIVEN. if server is shutdown during the process the status won't be updated!
        executor.submit(wait_until_stopped, instance, study)


def delete_study(study):
    org_name = study.organization.name

    try:
        study.delete_bucket(org_name=org_name)
    except BucketNotFound as e:
        logger.warning(
            f"Bucket {e.bucket_name} was not found for study {study.name}:{study.id} at delete bucket operation"
        )
    except PolicyNotFound as e:
        logger.warning(
            f"Policy {e.policy} was not found for study {study.name}:{study.id} at delete bucket operation"
        )
    except RoleNotFound as e:
        logger.warning(
            f"Role {e.role} was not found for study {study.name}:{study.id} at delete bucket operation"
        )

    try:
        delete_route53(
            execution=study.execution.execution_user.email,
            org_name=study.organization.name,
        )
    except Route53Error as e:
        logger.warning(str(e))

    try:
        toggle_study_vm(org_name=study.organization.name, study=study)
    except Ec2Error as e:
        logger.warning(str(e))

    ElasticsearchService.write_monitoring_event(
        event_type=MonitorEvents.EVENT_STUDY_DELETED,
        study_id=study.id,
        study_name=study.name,
        environment_name=study.organization.name,
    )
    logger.info(
        f"Study Event: {MonitorEvents.EVENT_STUDY_DELETED.value} "
        f"on study {study.name}:{study.id} "
        f"in org {study.organization}"
    )
