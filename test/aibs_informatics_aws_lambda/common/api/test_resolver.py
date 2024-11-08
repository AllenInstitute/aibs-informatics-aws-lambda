from test.base import BaseTest
from typing import Optional

from aws_lambda_powertools.event_handler.api_gateway import Router
from aws_lambda_powertools.shared.constants import METRICS_NAMESPACE_ENV
from aws_lambda_powertools.utilities.data_classes import APIGatewayProxyEvent
from moto import mock_sts

from aibs_informatics_aws_lambda.common.api.resolver import ApiResolverBuilder
from aibs_informatics_aws_lambda.common.models import DefaultLambdaContext


@mock_sts
class ApiResolverBuilderTests(BaseTest):
    def setUp(self) -> None:
        super().setUp()
        self.set_aws_credentials()
        self.builder = ApiResolverBuilder()
        self.set_env_vars((METRICS_NAMESPACE_ENV, "namespace"))

    def test__add_handlers__adds_module(self):
        from .handlers import module as handlers_module

        self.builder.add_handlers(target_module=handlers_module)
        assert len(self.builder.app._route_keys) == 2
        assert self.builder.app._route_keys == ["GET/health", "GET/get"]

    def test__add_handlers__adds_pkg(self):
        from .handlers import pkg as handlers_pkg

        self.builder.add_handlers(target_module=handlers_pkg)
        assert len(self.builder.app._route_keys) == 2
        assert self.builder.app._route_keys == ["GET/health", "GET/get"]

    def test__add_handlers__adds_module_and_pkg(self):
        from .handlers import module as handlers_module
        from .handlers import pkg as handlers_pkg

        pkg_router = Router()

        self.builder.add_handlers(target_module=handlers_module, prefix="/module")
        self.builder.add_handlers(target_module=handlers_pkg, router=pkg_router, prefix="/pkg")
        assert len(self.builder.app._route_keys) == 4
        assert self.builder.app._route_keys == [
            "GET/module/health",
            "GET/module/get",
            "GET/pkg/health",
            "GET/pkg/get",
        ]

    def test__add_handlers__collisions_are_silently_handled(self):
        from .handlers import module as handlers_module
        from .handlers import pkg as handlers_pkg

        pkg_router = Router()

        self.builder.add_handlers(target_module=handlers_module)
        self.builder.add_handlers(target_module=handlers_pkg, router=pkg_router)
        assert len(self.builder.app._route_keys) == 4
        assert self.builder.app._route_keys == [
            "GET/health",
            "GET/get",
            "GET/health",
            "GET/get",
        ]

    def test__resolve__succeeds(self):
        from .handlers import module as handlers_module

        self.builder.add_handlers(target_module=handlers_module)
        event = self.create_event(path="/health", method="GET", body="{}")
        event["headers"]["X-Client-Version"] = "1.0.0"
        context = DefaultLambdaContext()
        lambda_handler = self.builder.get_lambda_handler()
        response = lambda_handler(event, context)
        assert response["statusCode"] == 200

    def test__resolve__handles_not_found_error(self):
        from .handlers import module as handlers_module

        self.builder.add_handlers(target_module=handlers_module)
        event = self.create_event(path="/does_not_exist", method="GET", body="{}")
        context = DefaultLambdaContext()
        lambda_handler = self.builder.get_lambda_handler()
        response = lambda_handler(event, context)
        assert response["statusCode"] == 418

    def test__resolve__updates_logging(self):
        from .handlers import module as handlers_module

        self.builder.add_handlers(target_module=handlers_module)
        event = self.create_event(path="/health", method="GET", body="{}")
        event["headers"]["X-Log-Level"] = "DEBUG"
        event["headers"]["X-Client-Version"] = "1.0.0"
        context = DefaultLambdaContext()
        lambda_handler = self.builder.get_lambda_handler()
        response = lambda_handler(event, context)
        assert response["statusCode"] == 200
        assert self.builder.logger.level == 10

    def test__resolve__handles_invalid_logging_level(self):
        from .handlers import module as handlers_module

        self.builder.add_handlers(target_module=handlers_module)
        event = self.create_event(path="/health", method="GET", body="{}")
        event["headers"]["X-Log-Level"] = "DOES_NOT_DEBUG"
        event["headers"]["X-Client-Version"] = "1.0.0"
        context = DefaultLambdaContext()
        lambda_handler = self.builder.get_lambda_handler()
        response = lambda_handler(event, context)
        assert response["statusCode"] == 200

    def test__resolve__validation_always_fails(self):
        class ApiResolverBuilderWithValidation(ApiResolverBuilder):
            def validate_event(self, event: APIGatewayProxyEvent) -> None:
                raise ValueError("why not")

        builder = ApiResolverBuilderWithValidation()
        from .handlers import module as handlers_module

        builder.add_handlers(target_module=handlers_module)
        event = self.create_event(path="/health", method="GET", body="{}")
        context = DefaultLambdaContext()
        lambda_handler = builder.get_lambda_handler()
        response = lambda_handler(event, context)
        assert response["statusCode"] == 401

    def test__resolve__handles_handler_failure(self):
        from .handlers import module as handlers_module

        self.builder.add_handlers(target_module=handlers_module)
        event = self.create_event(path="/health", method="GET", body="{'raise_exception': true}")
        context = DefaultLambdaContext()
        lambda_handler = self.builder.get_lambda_handler()
        response = lambda_handler(event, context)
        assert response["statusCode"] == 400

    def create_event(
        self,
        path: str,
        method: str,
        query_string_parameters: Optional[str] = None,
        body: str = '"{}"',
    ):
        return {
            "resource": "/{proxy+}",
            "path": path,
            "httpMethod": method,
            "headers": {
                "Accept-Encoding": "identity",
                "CloudFront-Forwarded-Proto": "https",
                "CloudFront-Is-Desktop-Viewer": "true",
                "CloudFront-Is-Mobile-Viewer": "false",
                "CloudFront-Is-SmartTV-Viewer": "false",
                "CloudFront-Is-Tablet-Viewer": "false",
                "CloudFront-Viewer-ASN": "40272",
                "CloudFront-Viewer-Country": "US",
                "Content-Type": "application/json",
                "Host": "xm133vrxwl.execute-api.us-west-2.amazonaws.com",
                "User-Agent": "python-urllib3/1.26.14",
                "Via": "1.1 12341234123412341234123412341234.cloudfront.net (CloudFront)",
                "X-Amz-Cf-Id": "123412341234123412341234123412341234123412341234123412==",
                "x-amz-date": "20230207T022815Z",
                "X-Amz-Security-Token": "X-Amz-Security-Token",
                "X-Amzn-Trace-Id": "Root=1-63e1b73f-40448450599fe21e0e5dd262",
                "X-Forwarded-For": "163.123.189.8, 15.158.4.70",
                "X-Forwarded-Port": "443",
                "X-Forwarded-Proto": "https",
                "x-client-version": "1.0.0",
            },
            "multiValueHeaders": {
                "Accept-Encoding": ["identity"],
                "CloudFront-Forwarded-Proto": ["https"],
                "CloudFront-Is-Desktop-Viewer": ["true"],
                "CloudFront-Is-Mobile-Viewer": ["false"],
                "CloudFront-Is-SmartTV-Viewer": ["false"],
                "CloudFront-Is-Tablet-Viewer": ["false"],
                "CloudFront-Viewer-ASN": ["40272"],
                "CloudFront-Viewer-Country": ["US"],
                "Content-Type": ["application/json"],
                "Host": ["xm133vrxwl.execute-api.us-west-2.amazonaws.com"],
                "User-Agent": ["python-urllib3/1.26.14"],
                "Via": ["1.1 79880188a81becf1687ba18c0e064230.cloudfront.net (CloudFront)"],
                "X-Amz-Cf-Id": ["h5k3NgQ0YtFLyIbMa9wf63rntLOUri21szsY39pWOCT9ultVdi9hlA=="],
                "x-amz-date": ["20230207T022815Z"],
                "X-Amz-Security-Token": ["X-Amz-Security-Token"],
                "X-Amzn-Trace-Id": ["Root=1-63e1b73f-40448450599fe21e0e5dd262"],
                "X-Forwarded-For": ["163.123.189.8, 15.158.4.70"],
                "X-Forwarded-Port": ["443"],
                "X-Forwarded-Proto": ["https"],
            },
            "queryStringParameters": query_string_parameters,
            "multiValueQueryStringParameters": None,
            "pathParameters": {"proxy": path},
            "stageVariables": None,
            "requestContext": {
                "resourceId": "wgfu1v",
                "resourcePath": "/{proxy+}",
                "httpMethod": method,
                "extendedRequestId": "f8mR9E3APHcFmcw=",
                "requestTime": "07/Feb/2023:02:28:15 +0000",
                "path": f"/prod/{path}",
                "accountId": "123456789012",
                "protocol": "HTTP/1.1",
                "stage": "prod",
                "domainPrefix": "xm133vrxwl",
                "requestTimeEpoch": 1675736895553,
                "requestId": "cd5ff468-b2c5-4dce-87c1-3ab6c11f4f0b",
                "identity": {
                    "cognitoIdentityPoolId": None,
                    "accountId": "123456789012",
                    "cognitoIdentityId": None,
                    "caller": "AROAAROAAROAAROAAROAA:marmotdev@alleninstitute.org",
                    "sourceIp": "111.111.111.1",
                    "principalOrgId": "o-mkweediums9",
                    "accessKey": "ASIAQYDX6AJTQXIVWC4S",
                    "cognitoAuthenticationType": None,
                    "cognitoAuthenticationProvider": None,
                    "userArn": "arn:aws:sts::123456789012:assumed-role/AWSReservedSSO_AWSAdministratorAccess_1234123412341234/marmotdev@alleninstitute.org",
                    "userAgent": "python-urllib3/1.26.14",
                    "user": "AROAAROAAROAAROAAROAA:marmotdev@alleninstitute.or",
                },
                "domainName": "api123123.execute-api.us-west-2.amazonaws.com",
                "apiId": "api123123",
            },
            "body": body,
            "isBase64Encoded": False,
        }
