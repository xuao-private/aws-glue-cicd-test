from typing import Dict
import aws_cdk as cdk
from aws_cdk import (
    Stack,
    aws_iam as iam,
    aws_codebuild as codebuild
)
from constructs import Construct
from aws_cdk.pipelines import CodePipeline, CodePipelineSource, CodeBuildStep
from aws_glue_cdk_baseline.glue_app_stage import GlueAppStage
 
GITHUB_REPO = "xuao-private/aws-glue-cicd-test"
GITHUB_BRANCH = "main"
GITHUB_CONNECTION_ARN = "arn:aws:codeconnections:ap-northeast-1:288378107057:connection/92d6be0e-c606-4b63-a879-edf3f90e0d65"

class PipelineStack(Stack):
 
    def __init__(self, scope: Construct, construct_id: str, config: Dict, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
 
        source = CodePipelineSource.connection(
            GITHUB_REPO,
            GITHUB_BRANCH,
            connection_arn=GITHUB_CONNECTION_ARN
        )
 
        pipeline = CodePipeline(self, "GluePipeline",
            pipeline_name="GluePipeline",
            self_mutation=False,
            cross_account_keys=True,
            docker_enabled_for_synth=True,
            synth=CodeBuildStep("CdkSynth",
                input=source,
                install_commands=[
                    "pip install -r requirements.txt",
                    "pip install -r requirements-dev.txt",
                    "npm install -g aws-cdk",
                ],
                commands=[
                    "cdk synth",
                ],
                partial_build_spec=codebuild.BuildSpec.from_object({
                    "phases": {
                        "install": {
                            "runtime-versions": {
                                "nodejs": "18"
                            }
                        }
                    }
                })
            )
        )
 
        # ========== 开发环境 ==========
        dev_stage = GlueAppStage(self, "DevStage", 
            config=config, 
            stage="dev", 
            env=cdk.Environment(
                account=str(config['devAccount']['awsAccountId']),
                region=config['devAccount']['awsRegion']
            ))
        dev_stage_in_pipeline = pipeline.add_stage(dev_stage)

        # ========== 生产环境 ==========
        prod_stage = GlueAppStage(self, "ProdStage", 
            config=config, 
            stage="prod", 
            env=cdk.Environment(
                account=str(config['prodAccount']['awsAccountId']),
                region=config['prodAccount']['awsRegion']
            ))
        prod_stage_in_pipeline = pipeline.add_stage(prod_stage)
 
        # ========== 开发环境作业同步 ==========
        dev_sync = CodeBuildStep("DevGlueJobSync",
            input=source,
            env={
                "JOB_NAME_PREFIX": "dev-",  # 传递前缀
                "TARGET_ENV": "dev"
            },
            commands=[
                "python $(pwd)/aws_glue_cdk_baseline/job_scripts/generate_mapping.py",
                "python aws_glue_cdk_baseline/job_scripts/sync.py "
                   "--dst-profile dev-account "
                   "--dst-region {0} "
                   "--deserialize-from-file aws_glue_cdk_baseline/resources/resources.json "
                   "--config-path mapping.json "
                   "--targets job "
                   "--skip-prompt".format(
                       config['devAccount']['awsRegion']
                   ),
            ],
            role_policy_statements=[
                iam.PolicyStatement(
                    actions=["sts:AssumeRole"],
                    resources=["*"]
                )
            ]
        )
        dev_stage_in_pipeline.add_post(dev_sync)

        # ========== 生产环境作业同步 ==========
        prod_sync = CodeBuildStep("ProdGlueJobSync",
            input=source,
            env={
                "JOB_NAME_PREFIX": "prod-",  # 传递前缀
                "TARGET_ENV": "prod"
            },
            commands=[
                "python $(pwd)/aws_glue_cdk_baseline/job_scripts/generate_mapping.py",
                "python aws_glue_cdk_baseline/job_scripts/sync.py "
                   "--dst-role-arn arn:aws:iam::{0}:role/GlueCrossAccountRole-prod "
                   "--dst-region {1} "
                   "--deserialize-from-file aws_glue_cdk_baseline/resources/resources.json "
                   "--config-path mapping.json "
                   "--targets job "
                   "--skip-prompt".format(
                       config['prodAccount']['awsAccountId'],
                       config['prodAccount']['awsRegion']
                   ),
            ],
            role_policy_statements=[
                iam.PolicyStatement(
                    actions=["sts:AssumeRole"],
                    resources=["*"]
                )
            ]
        )
        prod_stage_in_pipeline.add_post(prod_sync)