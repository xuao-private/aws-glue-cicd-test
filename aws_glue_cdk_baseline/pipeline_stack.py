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
        # 保存 pipeline 添加 stage 后返回的对象
        dev_stage_in_pipeline = pipeline.add_stage(dev_stage)

        # ========== 预发布环境（新增） ==========
        # 检查配置中是否有 stg 环境
        if 'stg' in config and 'stgAccount' in config:
            stg_stage = GlueAppStage(self, "StgStage", 
                config=config, 
                stage="stg", 
                env=cdk.Environment(
                    account=str(config['stgAccount']['awsAccountId']),
                    region=config['stgAccount']['awsRegion']
                ))
            # 保存 pipeline 添加 stage 后返回的对象
            stg_stage_in_pipeline = pipeline.add_stage(stg_stage)

        # ========== 生产环境 ==========
        prod_stage = GlueAppStage(self, "ProdStage", 
            config=config, 
            stage="prod", 
            env=cdk.Environment(
                account=str(config['prodAccount']['awsAccountId']),
                region=config['prodAccount']['awsRegion']
            ))
        # 保存 pipeline 添加 stage 后返回的对象
        prod_stage_in_pipeline = pipeline.add_stage(prod_stage)
 
        # ========== Glue 作业参数同步（每个环境独立） ==========
        # 注意：现在 sync.py 只同步参数，不创建作业
        
        # 开发环境的参数同步
        dev_sync = CodeBuildStep("DevGlueJobSync",
            input=source,
            env={
                "STAGE": "dev"
            },
            commands=[
                "python $(pwd)/aws_glue_cdk_baseline/job_scripts/generate_mapping.py",
                "python aws_glue_cdk_baseline/job_scripts/sync.py "
                   "--dst-role-arn arn:aws:iam::{0}:role/GlueCrossAccountRole-dev "
                   "--dst-region {1} "
                   "--deserialize-from-file aws_glue_cdk_baseline/resources/resources.json "
                   "--config-path mapping.json "
                   "--targets job "
                   "--skip-prompt".format(
                       config['devAccount']['awsAccountId'],
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
        
        # 添加到 DevStage 之后 - 使用 pipeline 返回的对象
        dev_stage_in_pipeline.add_post(dev_sync)

        # 预发布环境的参数同步
        if 'stg' in config and 'stgAccount' in config:
            stg_sync = CodeBuildStep("StgGlueJobSync",
                input=source,
                env={
                    "STAGE": "stg"
                },
                commands=[
                    "python $(pwd)/aws_glue_cdk_baseline/job_scripts/generate_mapping.py",
                    "python aws_glue_cdk_baseline/job_scripts/sync.py "
                       "--dst-role-arn arn:aws:iam::{0}:role/GlueCrossAccountRole-stg "
                       "--dst-region {1} "
                       "--deserialize-from-file aws_glue_cdk_baseline/resources/resources.json "
                       "--config-path mapping.json "
                       "--targets job "
                       "--skip-prompt".format(
                           config['stgAccount']['awsAccountId'],
                           config['stgAccount']['awsRegion']
                       ),
                ],
                role_policy_statements=[
                    iam.PolicyStatement(
                        actions=["sts:AssumeRole"],
                        resources=["*"]
                    )
                ]
            )
            # 添加到 StgStage 之后 - 使用 pipeline 返回的对象
            stg_stage_in_pipeline.add_post(stg_sync)

        # 生产环境的参数同步
        prod_sync = CodeBuildStep("ProdGlueJobSync",
            input=source,
            env={
                "STAGE": "prod"
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
        # 添加到 ProdStage 之后 - 使用 pipeline 返回的对象
        prod_stage_in_pipeline.add_post(prod_sync)