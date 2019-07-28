from mainapp import settings

base_trust_relationship_doc = {
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::"+settings.aws_account_number+":root"
      },
      "Action": "sts:AssumeRole",
      "Condition": {}
    }
  ]
}

base_s3_policy = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "s3:*",
            "Resource": []
        }
    ]
}