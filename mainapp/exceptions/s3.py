class BucketNotFound(Exception):
    def __init__(self, bucket_name):
        super().__init__(f"Bucket {bucket_name} was not found")
        self.bucket_name = bucket_name
