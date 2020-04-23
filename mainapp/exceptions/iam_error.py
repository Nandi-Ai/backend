class PolicyNotFound(Exception):
    def __init__(self, policy):
        super().__init__(f"Bucket {policy} was not found")
        self.policy = policy


class RoleNotFound(Exception):
    def __init__(self, role):
        super().__init__(f"Bucket {role} was not found")
        self.role = role
