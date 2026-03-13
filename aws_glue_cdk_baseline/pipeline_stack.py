from typing import Dict
import aws_cdk as cdk
from aws_cdk import (
    Stack,
    aws_iam as iam,
    aws_codebuild as codebuild
)
from constructs import Construct
from aws_cdk.pipelines import CodePipeline, CodePipelineSource, CodeBuildStep

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
            pipeline_name=f"{env_type}-glue-pipeline-test-bydl",
            self_mutation=False,
            cross_account_keys=False,
            docker_enabled_for_synth=True,
            synth=CodeBuildStep("CdkSynth",
                input=source,
                install_commands=[
                    "pip install -r requirements.txt",
                    "npm install -g aws-cdk",
                ],
                commands=[
                    "cdk synth -c envType={0}".format(env_type),
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
        
        # ジョブ同期ステップ（直接 pipeline に追加）
        sync_step = CodeBuildStep(f"{env_type.capitalize()}GlueJobSync",
            input=source,
            env={
                "JOB_NAME_PREFIX": f"{env_type}-",
                "TARGET_ENV": env_type
            },
            commands=[
                "python $(pwd)/aws_glue_cdk_baseline/job_scripts/generate_mapping.py",
                "python $(pwd)/aws_glue_cdk_baseline/job_scripts/sync.py "
                   f"--dst-region {config['pipelineAccount']['awsRegion']} "
                   "--deserialize-from-file resources/ "
                   "--config-path mapping.json "
                   "--targets job "
                   "--skip-prompt"
            ],
            role_policy_statements=[
                iam.PolicyStatement(
                    actions=[
                        "glue:GetJob",
                        "glue:GetJobs",
                        "glue:CreateJob",
                        "glue:UpdateJob",
                        "glue:DeleteJob",
                        "glue:StartJobRun",
                        "glue:GetJobRun",
                        "glue:GetJobRuns"
                    ],
                    resources=["*"]
                ),
                iam.PolicyStatement(
                    actions=[
                        "iam:PassRole"
                    ],
                    resources=[
                        f"arn:aws:iam::{config['pipelineAccount']['awsAccountId']}:role/service-role/*"
                    ]
                ),
                iam.PolicyStatement(
                    actions=[
                        "s3:GetObject",
                        "s3:PutObject",
                        "s3:ListBucket",
                        "s3:GetBucketLocation"
                    ],
                    resources=["*"]
                )
            ]
        )
        
        # Wave を追加して同期ステップを実行
        pipeline.add_wave("GlueJobSync").add_post(sync_step)