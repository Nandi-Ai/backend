class Ec2Error(Exception):
    def __init__(self, msg):
        super().__init__(msg)


class TooManyInstancesError(Exception):
    def __init__(self, inst_name):
        super().__init__(f"Too many instances returned for name='{inst_name}'")


class InstanceNotFound(Exception):
    def __init__(self, inst_name):
        super().__init__(f"Instance name '{inst_name}' not found")


class InstanceTerminated(Exception):
    def __init__(self, inst_name):
        super().__init__(
            f"Instance name '{inst_name}' is terminated, no operations are valid"
        )


class InvalidEc2Status(Exception):
    def __init__(self, status):
        super().__init__(f"{status} is an invalid EC2 instance status")
