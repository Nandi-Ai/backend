class BaseImageDeIdError(Exception):
    pass


class LambdaInvocationError(BaseImageDeIdError):
    def __init__(
        self,
        image_name,
        image_s3_obj,
        destination_location,
        method_name,
        method_id,
        error=None,
    ):
        super().__init__(
            f"Lambda invocation for image_name {image_name} image_object {image_s3_obj} Failed "
            f"at bucket destination {destination_location} "
            f"for method {method_name}:{method_id}",
            error,
        )


class UploadBatchProcessError(BaseImageDeIdError):
    def __init__(self, destination_bucket, error=None):
        super().__init__(
            f"Could not upload batch process file to se bucket {destination_bucket}",
            error,
        )


class BaseImageDeIdHelperError(Exception):
    pass


class EmptyBucketError(BaseImageDeIdHelperError):
    def __init__(self, destination_bucket, error=None):
        super().__init__(
            f"No Content was found in the bucket {destination_bucket}", error
        )


class UpdateJobProcessError(BaseImageDeIdHelperError):
    def __init__(self, source_bucket, error=None):
        super().__init__(
            f"Could not update Job Process process for bucket {source_bucket}", error
        )


class NoObjectContentError(BaseImageDeIdHelperError):
    def __init__(self, key, error=None):
        super().__init__(f"No Content was found at specified key {key}", error)
