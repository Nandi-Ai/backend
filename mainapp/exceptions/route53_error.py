class Route53Error(Exception):
    def __init__(self, msg):
        super().__init__(msg)


class DnsRecordNotFound(Route53Error):
    def __init__(self, record_name, org_name):
        super().__init__(
            f"Route53 DNS record '{record_name}' not found in organization {org_name}"
        )


class DnsRecordExists(Route53Error):
    def __init__(self, record_name, org_name):
        super().__init__(
            f"Route53 record '{record_name}' already exists in organization {org_name}"
        )


class NoSuchHostedZoneError(Route53Error):
    def __init__(self, hosted_zone):
        super().__init__(
            f"Route53 could not find hosted zone '{hosted_zone}' not found"
        )


class InvalidChangeBatchError(Route53Error):
    def __init__(self, record_name):
        super().__init__(f"Route53 record change error on '{record_name}'")


class InvalidInputError(Route53Error):
    def __init__(self, record_name):
        super().__init__(f"Route53 invalid input for '{record_name}'")


class PriorRequestNotCompleteError(Route53Error):
    def __init__(self):
        super().__init__("Route53 previous request did not finish")
