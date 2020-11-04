class PolicyNotFound(Exception):
    def __init__(self, policy):
        super().__init__(f"Bucket {policy} was not found")
        self.policy = policy


class RoleNotFound(Exception):
    def __init__(self, role):
        super().__init__(f"Bucket {role} was not found")
        self.role = role


class PutPolicyError(Exception):
    def __init__(self, role_name, policy_name, error=None):
        super().__init__(
            f"Could not add inline policy {policy_name} for role {role_name}", error
        )


class CreateRoleError(Exception):
    def __init__(self, role_name, error=None):
        super().__init__(f"Could not create role {role_name}", error)
