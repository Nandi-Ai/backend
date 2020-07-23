from enum import Enum

from mainapp.exceptions import (
    DnsRecordExistsError,
    DnsRecordNotFoundError,
    Ec2Error,
    Route53Error,
)
from mainapp.utils.aws_service import create_ec2_resource
from mainapp.utils.aws_utils.ec2 import get_instance


class Route53Actions(Enum):
    DELETE = "DELETE"
    CREATE = "CREATE"


def delete_route53(boto3_client, existing_records, record_name, org_name, hosted_zone):
    if not existing_records or record_name != existing_records[0]["Name"][:-1]:
        raise DnsRecordNotFoundError(record_name, org_name)

    boto3_client.change_resource_record_sets(
        HostedZoneId=hosted_zone,
        ChangeBatch={
            "Changes": [
                {
                    "Action": Route53Actions.DELETE.value,
                    "ResourceRecordSet": existing_records[0],
                }
            ]
        },
    )


def create_route53(boto3_client, existing_records, record_name, org_name, hosted_zone):
    if existing_records and record_name == existing_records[0]["Name"][:-1]:
        raise DnsRecordExistsError(record_name, org_name)

    try:
        instance_ip = get_instance(
            boto_resource=create_ec2_resource(org_name),
            execution_token=record_name.split(".")[0],
        ).private_ip_address
    except Ec2Error as error:
        raise Route53Error(str(error))

    boto3_client.change_resource_record_sets(
        HostedZoneId=hosted_zone,
        ChangeBatch={
            "Changes": [
                {
                    "Action": Route53Actions.CREATE.value,
                    "ResourceRecordSet": {
                        "Name": record_name,
                        "Type": "A",
                        "TTL": 300,
                        "ResourceRecords": [{"Value": instance_ip}],
                    },
                }
            ]
        },
    )
