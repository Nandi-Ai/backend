class InvalidOrganizationSettings(Exception):
    def __init__(self, org_name):
        super().__init__(f"A setting entry for organization {org_name} was not found")
        self.org_name = org_name


class InvalidOrganizationOrgValues(Exception):
    def __init__(self):
        super().__init__("Missing ORG_VALUES in settings")


class MissingOrganizationSettingKey(Exception):
    def __init__(self, org_name, key):
        super().__init__(
            f"The {key} key is missing from the setting entry for organization {org_name}"
        )
        self.org_name = org_name
        self.key = key
