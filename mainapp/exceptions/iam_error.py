class PutPolicyError(Exception):
    def __init__(self, role_name, policy_name, error=None):
        super().__init__(
            f"Could not add inline policy {policy_name} for role {role_name}", error
        )


class CreateRoleError(Exception):
    def __init__(self, role_name, error=None):
        super().__init__(f"Could not create role {role_name}", error)
