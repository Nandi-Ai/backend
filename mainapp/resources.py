from mainapp import settings

base_trust_relationship_doc = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::" + settings.AWS['AWS_ACCOUNT_NUMBER'] + ":root"
            },
            "Action": "sts:AssumeRole",
            "Condition": {}
        }
    ]
}
