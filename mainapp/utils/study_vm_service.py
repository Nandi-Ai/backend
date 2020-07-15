import logging
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
from mainapp.models import Study, Execution, User
from mainapp.utils.decorators import with_ec2_resource
from mainapp.utils.elasticsearch_service import ElasticsearchService, MonitorEvents
from mainapp.utils.lib import delete_route53

logger = logging.getLogger(__name__)

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


def update_study_state(execution_user, status):
    study = Study.objects.get(
        execution=Execution.objects.get(
            execution_user=User.objects.get(email=execution_user)
        )
    )
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


@with_ec2_resource
def toggle_study_vm(
    boto3_client,
    org_name,
    execution,
    instance_method,
    blocker_statuses,
    killer_statuses,
    toggle_status,
):
    execution_token = execution.split("@")[0]
    instance = get_instance(boto3_client, execution_token)
    state_name = instance.state.get("Name")
    if not state_name:
        raise InvalidEc2Status(state_name)
    if state_name in killer_statuses:
        return
    if state_name in blocker_statuses:
        sleep(30)
    getattr(instance, instance_method)()
    update_study_state(execution, toggle_status)


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
        toggle_study_vm(
            org_name=study.organization.name,
            execution=study.execution.execution_user.email,
            **STATUS_ARGS["terminate"],
        )
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
