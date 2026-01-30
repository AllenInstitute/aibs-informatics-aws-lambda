"""ECR image replication handlers.

Provides Lambda handlers for replicating Docker images
between ECR repositories.
"""

from aibs_informatics_aws_utils.ecr import (
    ECRImageReplicator,
    ReplicateImageRequest,
    ReplicateImageResponse,
)

from aibs_informatics_aws_lambda.common.handler import LambdaHandler


class ImageReplicatorHandler(LambdaHandler[ReplicateImageRequest, ReplicateImageResponse]):
    """Handler for replicating ECR images between repositories.

    Wraps the ECRImageReplicator to provide Lambda integration.
    """

    def handle(self, request: ReplicateImageRequest) -> ReplicateImageResponse:
        """Replicate an ECR image to a target repository.

        Args:
            request (ReplicateImageRequest): Request containing source and target
                image specifications.

        Returns:
            Response containing the replication result.
        """
        return ECRImageReplicator().process_request(request)


lambda_handler = ImageReplicatorHandler.get_handler()
