class PermissionException(Exception):
    def __init__(self, user_permission, dataset_permission):
        super().__init__(
            f"The user can not add this dataset to the study because he has {user_permission}"
            f" but needs {dataset_permission} "
        )


class InvalidDataset(Exception):
    def __init__(self, dataset_id):
        super().__init__(f"Dataset instance {dataset_id} does not exist")
