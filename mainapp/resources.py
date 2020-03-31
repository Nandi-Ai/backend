from mainapp.utils.decorators import organization_dependent


@organization_dependent
def create_base_trust_relationship(org_settings, org_name):
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "AWS": f"arn:aws:iam::{org_settings['ACCOUNT_NUMBER']}:root"
                },
                "Action": "sts:AssumeRole",
                "Condition": {},
            }
        ],
    }
