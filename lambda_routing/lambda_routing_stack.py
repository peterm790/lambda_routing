from constructs import Construct
from aws_cdk import (
    Stack,
    aws_lambda as _lambda,
    aws_s3 as s3,
    aws_iam as iam,
    Duration
)
from aws_cdk.aws_lambda_event_sources import S3EventSource


class LambdaRoutingStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        policy = iam.Policy(self, "MyPolicy",
                    policy_name="AllowS3ReadAccess",
                    statements=[
                        iam.PolicyStatement(
                            actions=["s3:GetObject"],
                            resources=["arn:aws:s3:::*"]
                        )
                    ])

        # Create an IAM role that allows access to the S3 bucket
        role = iam.Role(self, "MyRole", assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"))

        # Attach the policy to the role
        policy.attach_to_role(role)

        lambda_routing = _lambda.DockerImageFunction(
            scope = self,
            id = "WeatherRoute_ERA5_Lambda",
            function_name = "WeatherRoute_lambda",
            role = role,
            code = _lambda.DockerImageCode.from_image_asset(directory = "lambda/"),
            timeout = Duration.minutes(15),
            memory_size=3008
            )
        
        
        bucket = s3.Bucket(self, "WeatherRoute_Bucket")
        bucket.grant_read_write(lambda_routing)


        lambda_routing.add_event_source(S3EventSource(bucket,
                events=[s3.EventType.OBJECT_CREATED],
                filters=[s3.NotificationKeyFilter(prefix="inputs/")]
        ))

        # Grant the function permission to write to CloudWatch Logs
        lambda_routing.add_to_role_policy(
            iam.PolicyStatement(
                actions=["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
                resources=["arn:aws:logs:*:*:*"]
            )
        )
