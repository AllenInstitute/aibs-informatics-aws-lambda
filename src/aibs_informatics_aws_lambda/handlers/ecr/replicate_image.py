from aibs_informatics_aws_utils.ecr import (
    ECRImageReplicator,
    ReplicateImageRequest,
    ReplicateImageResponse,
)

from aibs_informatics_aws_lambda.common.handler import LambdaHandler


class ImageReplicatorHandler(LambdaHandler[ReplicateImageRequest, ReplicateImageResponse]):
    def handle(self, request: ReplicateImageRequest) -> ReplicateImageResponse:
        return ECRImageReplicator().process_request(request)


lambda_handler = ImageReplicatorHandler.get_handler()
