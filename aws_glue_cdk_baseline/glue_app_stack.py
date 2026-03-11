from typing import Dict
from aws_cdk import (
    Stack,
    aws_glue_alpha as glue,
    aws_iam as iam,
    aws_s3_assets as s3_assets,
    AssetHashType
)
from constructs import Construct

class GlueAppStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, config:Dict, stage:str, job_name_prefix:str = "", **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # 获取当前环境配置
        current_account = config[f"{stage}Account"]['awsAccountId']
        pipeline_account = config['pipelineAccount']['awsAccountId']
        
        # 基础路径
        base_assets_path = f"s3://aws-glue-assets-{current_account}-{self.region}"
        
        # 按环境区分的路径
        env_scripts_path = f"{base_assets_path}/{stage}/scripts/"
        env_spark_logs_path = f"{base_assets_path}/{stage}/sparkHistoryLogs/"
        env_temp_path = f"{base_assets_path}/{stage}/temporary/"

        # Create cross-account role
        self.cross_account_role = self.create_cross_account_role(
            f"GlueCrossAccountRole-{stage}",
            str(pipeline_account)
        )

        # 创建 Glue 服务角色
        glue_service_role = iam.Role(self, f"GlueServiceRole-{stage}",
            role_name=f"AWSGlueServiceRole-{stage}",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess")
            ]
        )

        # 从配置文件读取作业定义
        env_config = config.get(stage, {})
        jobs_config = env_config.get('jobs', {})
        
        for job_name, job_config in jobs_config.items():
            full_job_name = f"{job_name_prefix}{job_name}"
            
            # 将本地脚本作为资产上传
            script_asset = s3_assets.Asset(self, f"{full_job_name}Script",
                path=f"aws_glue_cdk_baseline/scripts/{job_name}.py",
                asset_hash_type=AssetHashType.OUTPUT
            )
            
            # 使用资产的位置
            script_location = script_asset.s3_object_url

            # 准备默认参数
            default_arguments = {
                "--enable-metrics": "true",
                "--enable-spark-ui": "true",
                "--spark-event-logs-path": env_spark_logs_path,
                "--enable-job-insights": "true",
                "--enable-observability-metrics": "true",
                "--conf": "spark.eventLog.rolling.enabled=true",
                "--enable-glue-datacatalog": "true",
                "--job-bookmark-option": "job-bookmark-disable",
                "--job-language": "python",
                "--TempDir": env_temp_path
            }
            
            # 如果配置中有 inputLocation，添加到参数中
            if 'inputLocation' in job_config:
                default_arguments["--input_path"] = job_config['inputLocation']

            # 创建 Glue 作业
            glue.CfnJob(self, f"{full_job_name}Job",
                name=full_job_name,
                role=glue_service_role.role_arn,
                command={
                    "name": "glueetl",
                    "scriptLocation": script_location,
                    "pythonVersion": "3"
                },
                default_arguments=default_arguments,
                max_retries=0,
                timeout=480,
                worker_type="G.1X",
                number_of_workers=2,
                glue_version="5.0",
                execution_class="STANDARD"
            )

        # 创建测试角色（用于集成测试）
        self.iam_role = iam.Role(self, f"GlueTestRole-{stage}",
            role_name=f"glue-test-{stage}",
            assumed_by=iam.ArnPrincipal(f"arn:aws:iam::{pipeline_account}:root"),
            inline_policies={
                "GluePolicy": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            actions=[
                                "glue:GetJobs",
                                "glue:GetJobRun",
                                "glue:GetTags",
                                "glue:StartJobRun"
                            ],
                            resources=["*"]
                        )
                    ]
                )
            }
        )

    def create_cross_account_role(self, role_name: str, trusted_account_id: str):
        """创建跨账号角色，允许管道账号部署"""
        return iam.Role(self, f"{role_name}CrossAccountRole",
            role_name=role_name,
            assumed_by=iam.AccountPrincipal(trusted_account_id),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AdministratorAccess")
            ]
        )

    @property
    def iam_role_arn(self):
        return self.iam_role.role_arn

    @property
    def cross_account_role_arn(self):
        return self.cross_account_role.role_arn