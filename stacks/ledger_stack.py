"""
FinancialLedgerStack

Provisions:
  - Encrypted S3 audit archive bucket with Glacier lifecycle transition
  - Single-table DynamoDB ledger with a GSI and DynamoDB Streams enabled
  - Secrets Manager secret for the webhook HMAC signing key
  - HTTP API (API Gateway v2) with a throttled webhook route
  - Ingestion Lambda (signature verification + atomic double-entry write)
  - CDC Archiver Lambda (DynamoDB Streams -> Parquet -> S3), with a DLQ
"""
from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    Aws,
    aws_dynamodb as ddb,
    aws_s3 as s3,
    aws_lambda as _lambda,
    aws_lambda_python_alpha as lambda_python,
    aws_lambda_event_sources as event_sources,
    aws_apigatewayv2 as apigwv2,
    aws_apigatewayv2_integrations as apigwv2_integrations,
    aws_secretsmanager as secretsmanager,
    aws_sqs as sqs,
    aws_iam as iam,
    aws_logs as logs,
)
from constructs import Construct


class LedgerStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ------------------------------------------------------------------
        # 1. S3 Audit Archive Bucket
        # ------------------------------------------------------------------
        audit_bucket = s3.Bucket(
            self,
            "LedgerAuditArchiveBucket",
            bucket_name=f"ledger-audit-archive-{Aws.ACCOUNT_ID}-{Aws.REGION}",
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            versioned=True,
            removal_policy=RemovalPolicy.RETAIN,
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="GlacierTransitionAndExpiry",
                    enabled=True,
                    transitions=[
                        s3.Transition(
                            storage_class=s3.StorageClass.GLACIER,
                            transition_after=Duration.days(30),
                        )
                    ],
                    expiration=Duration.days(7 * 365),
                    noncurrent_version_expiration=Duration.days(90),
                )
            ],
        )

        # ------------------------------------------------------------------
        # 2. DynamoDB Single-Table Ledger
        # ------------------------------------------------------------------
        ledger_table = ddb.Table(
            self,
            "FinancialLedgerTable",
            table_name="FinancialLedger",
            partition_key=ddb.Attribute(name="PK", type=ddb.AttributeType.STRING),
            sort_key=ddb.Attribute(name="SK", type=ddb.AttributeType.STRING),
            billing_mode=ddb.BillingMode.PAY_PER_REQUEST,
            stream=ddb.StreamViewType.NEW_AND_OLD_IMAGES,
            point_in_time_recovery=True,
            removal_policy=RemovalPolicy.RETAIN,
        )
        ledger_table.add_global_secondary_index(
            index_name="GSI1",
            partition_key=ddb.Attribute(name="GSI1PK", type=ddb.AttributeType.STRING),
            sort_key=ddb.Attribute(name="GSI1SK", type=ddb.AttributeType.STRING),
            projection_type=ddb.ProjectionType.ALL,
        )

        # ------------------------------------------------------------------
        # 3. Secrets Manager - webhook signing secret
        # ------------------------------------------------------------------
        webhook_secret = secretsmanager.Secret(
            self,
            "WebhookSigningSecret",
            secret_name="ledger/webhook-signing-secret",
            description="HMAC-SHA256 signing key used to verify inbound payment webhooks.",
            generate_secret_string=secretsmanager.SecretStringGenerator(
                secret_string_template='{"key": ""}',
                generate_string_key="signing_key",
                exclude_punctuation=True,
                password_length=48,
            ),
        )

        # ------------------------------------------------------------------
        # 4. DLQ for the CDC stream consumer
        # ------------------------------------------------------------------
        cdc_dlq = sqs.Queue(
            self,
            "CdcArchiverDLQ",
            queue_name="ledger-cdc-archiver-dlq",
            retention_period=Duration.days(14),
            encryption=sqs.QueueEncryption.SQS_MANAGED,
        )

        # ------------------------------------------------------------------
        # 5. Ingestion Lambda
        # ------------------------------------------------------------------
        powertools_layer = _lambda.LayerVersion.from_layer_version_arn(
            self,
            "PowertoolsLayer",
            layer_version_arn=(
                f"arn:{Aws.PARTITION}:lambda:{Aws.REGION}:017000801446:"
                "layer:AWSLambdaPowertoolsPythonV2:79"
            ),
        )

        ingestion_fn = lambda_python.PythonFunction(
            self,
            "IngestionFunction",
            entry="src/lambdas",
            index="ingestion.py",
            handler="handler",
            runtime=_lambda.Runtime.PYTHON_3_12,
            timeout=Duration.seconds(10),
            memory_size=256,
            tracing=_lambda.Tracing.ACTIVE,
            layers=[powertools_layer],
            environment={
                "TABLE_NAME": ledger_table.table_name,
                "SECRET_ARN": webhook_secret.secret_arn,
                "POWERTOOLS_SERVICE_NAME": "ledger-ingestion",
                "POWERTOOLS_METRICS_NAMESPACE": "FinancialLedger",
                "LOG_LEVEL": "INFO",
            },
            log_retention=logs.RetentionDays.ONE_YEAR,
        )

        # Least-privilege IAM for the ingestion function
        ledger_table.grant(
            ingestion_fn,
            "dynamodb:TransactWriteItems",
            "dynamodb:GetItem",
            "dynamodb:PutItem",
            "dynamodb:UpdateItem",
        )
        webhook_secret.grant_read(ingestion_fn)

        # ------------------------------------------------------------------
        # 6. CDC Archiver Lambda
        # ------------------------------------------------------------------
        # Use AWS's officially published "SDK for pandas" managed layer
        # (bundles pandas + numpy + pyarrow) instead of building our own —
        # a self-built layer with these packages exceeds Lambda's 250MB
        # unzipped layer size limit. Pin to a specific version for
        # reproducible deploys; see:
        # https://aws-sdk-pandas.readthedocs.io/en/stable/layers.html
        # for the correct ARN if you deploy to a different region.
        pyarrow_layer = _lambda.LayerVersion.from_layer_version_arn(
            self,
            "PyarrowLayer",
            layer_version_arn=(
                f"arn:{Aws.PARTITION}:lambda:{Aws.REGION}:336392948345:"
                "layer:AWSSDKPandas-Python312:29"
            ),
        )

        cdc_fn = lambda_python.PythonFunction(
            self,
            "CdcArchiverFunction",
            entry="src/lambdas",
            index="cdc_archiver.py",
            handler="handler",
            runtime=_lambda.Runtime.PYTHON_3_12,
            timeout=Duration.seconds(60),
            memory_size=1024,
            tracing=_lambda.Tracing.ACTIVE,
            layers=[powertools_layer, pyarrow_layer],
            environment={
                "AUDIT_BUCKET_NAME": audit_bucket.bucket_name,
                "POWERTOOLS_SERVICE_NAME": "ledger-cdc-archiver",
                "POWERTOOLS_METRICS_NAMESPACE": "FinancialLedger",
                "LOG_LEVEL": "INFO",
            },
            log_retention=logs.RetentionDays.ONE_YEAR,
        )

        audit_bucket.grant_put(cdc_fn)

        cdc_fn.add_event_source(
            event_sources.DynamoEventSource(
                ledger_table,
                starting_position=_lambda.StartingPosition.LATEST,
                batch_size=100,
                max_batching_window=Duration.seconds(30),
                bisect_batch_on_error=True,
                retry_attempts=3,
                on_failure=event_sources.SqsDlq(cdc_dlq),
                report_batch_item_failures=True,
            )
        )

        # DescribeStream / GetRecords / GetShardIterator are granted automatically
        # by add_event_source's DynamoEventSource via CDK's grant_stream_read call,
        # but we make the intent explicit for the least-privilege review below:
        ledger_table.grant_stream_read(cdc_fn)

        # ------------------------------------------------------------------
        # 7. API Gateway HTTP API (v2)
        # ------------------------------------------------------------------
        http_api = apigwv2.HttpApi(
            self,
            "LedgerHttpApi",
            api_name="financial-ledger-api",
            description="Low-latency ingestion endpoint for payment provider webhooks.",
            create_default_stage=False,
        )

        integration = apigwv2_integrations.HttpLambdaIntegration(
            "IngestionIntegration", handler=ingestion_fn
        )

        http_api.add_routes(
            path="/v1/webhooks/payment",
            methods=[apigwv2.HttpMethod.POST],
            integration=integration,
        )

        access_log_group = logs.LogGroup(
            self,
            "HttpApiAccessLogs",
            retention=logs.RetentionDays.THREE_MONTHS,
            removal_policy=RemovalPolicy.DESTROY,
        )

        default_stage = apigwv2.HttpStage(
            self,
            "DefaultStage",
            http_api=http_api,
            stage_name="$default",
            auto_deploy=True,
            throttle=apigwv2.ThrottleSettings(rate_limit=500, burst_limit=1000),
        )

        # Wire access logging via the L1 escape hatch (not yet exposed on the L2 HttpStage)
        cfn_stage = default_stage.node.default_child
        cfn_stage.access_log_settings = apigwv2.CfnStage.AccessLogSettingsProperty(
            destination_arn=access_log_group.log_group_arn,
            format=(
                '{"requestId":"$context.requestId","ip":"$context.identity.sourceIp",'
                '"requestTime":"$context.requestTime","httpMethod":"$context.httpMethod",'
                '"routeKey":"$context.routeKey","status":"$context.status",'
                '"protocol":"$context.protocol","responseLength":"$context.responseLength",'
                '"integrationErrorMessage":"$context.integrationErrorMessage"}'
            ),
        )
        access_log_group.grant_write(iam.ServicePrincipal("apigateway.amazonaws.com"))

        # ------------------------------------------------------------------
        # Outputs
        # ------------------------------------------------------------------
        self.audit_bucket = audit_bucket
        self.ledger_table = ledger_table
        self.webhook_secret = webhook_secret
        self.http_api = http_api
