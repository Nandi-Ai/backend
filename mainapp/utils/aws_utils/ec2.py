from mainapp.exceptions import (
    InstanceNotFound,
    TooManyInstancesError,
    InstanceTerminated,
)


AWS_EC2_STARTING = "pending"
AWS_EC2_RUNNING = "running"
AWS_EC2_STOPPING = "stopping"
AWS_EC2_SHUTTING_DOWN = "shutting-down"
AWS_EC2_STOPPED = "stopped"
AWS_EC2_TERMINATED = "terminated"


def get_instance(boto_resource, execution_token, status_filter=None):
    inst_name = f"jupyter-{execution_token}"
    filters = [{"Name": "tag:Name", "Values": [inst_name]}]

    if status_filter:
        filters.append({"Name": "instance-state-name", "Values": status_filter})
    instances = list(boto_resource.instances.filter(Filters=filters))
    if not instances:
        raise InstanceNotFound(inst_name)
    elif len(instances) > 1:
        raise TooManyInstancesError(inst_name)
    elif instances[0].state["Name"] == AWS_EC2_TERMINATED:
        raise InstanceTerminated(inst_name)

    return instances[0]
