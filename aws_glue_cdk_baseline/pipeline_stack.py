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

class PipelineStack(Stack):
 
    def __init__(self, scope: Construct, construct_id: str, config: Dict, env_type: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        print(f"{env_type} 環境の Pipeline をデプロイ")
        
        # config から GitHub 設定を取得
        GITHUB_REPO = config['github']['repo']
        GITHUB_BRANCH = config['github']['branch']
        GITHUB_CONNECTION_ARN = config['github']['connection_arn']
        
        print(f"GitHub リポジトリ: {GITHUB_REPO}")
        print(f"監視ブランチ: {GITHUB_BRANCH}")
 
        source = CodePipelineSource.connection(
            GITHUB_REPO,
            GITHUB_BRANCH,
            connection_arn=GITHUB_CONNECTION_ARN
        )
 
        pipeline = CodePipeline(self, "GluePipeline",
            pipeline_name=f"GluePipeline-{env_type}",  # 環境ごとに pipeline を区別
            self_mutation=False,
            cross_account_keys=False,  # 同一アカウントでのデプロイのためクロスアカウント不要
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
 
        #  現在の環境の stage のみをデプロイ
        stage = GlueAppStage(self, f"{env_type.capitalize()}Stage", 
            config=config, 
            stage=env_type, 
            env=cdk.Environment(
                account=str(config['pipelineAccount']['awsAccountId']),
                region=config['pipelineAccount']['awsRegion']
            ))
        stage_in_pipeline = pipeline.add_stage(stage)
        
        #  現在の環境のジョブ同期
        sync_step = CodeBuildStep(f"{env_type.capitalize()}GlueJobSync",
            input=source,
            env={
                "JOB_NAME_PREFIX": f"{env_type}-",  # プレフィックスを渡す
                "TARGET_ENV": env_type
            },
            commands=[
                "python $(pwd)/aws_glue_cdk_baseline/job_scripts/generate_mapping.py",
                "python aws_glue_cdk_baseline/job_scripts/sync.py "
                   "--dst-region {0} "
                   "--deserialize-from-file aws_glue_cdk_baseline/resources/resources.json "
                   "--config-path mapping.json "
                   "--targets job "
                   "--skip-prompt".format(
                       config['pipelineAccount']['awsRegion']
                   ),
            ]
            # 注意：role_policy_statements は削除済み（クロスアカウント不要のため）
        )
        stage_in_pipeline.add_post(sync_step)