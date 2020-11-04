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


def generate_dataset_permission_access_policy(bucket, location):
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:ListBucketVersions",
                    "s3:ListBucket",
                    "s3:GetObjectVersion",
                ],
                "Resource": [f"arn:aws:s3:::{location}/*", f"arn:aws:s3:::{bucket}"],
            }
        ],
    }
