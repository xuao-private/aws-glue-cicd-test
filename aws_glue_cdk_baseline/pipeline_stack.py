# Copyright 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
from typing import Dict
from aws_cdk import (
    Environment,
    Stack,
    aws_iam as iam,
    aws_codebuild as codebuild
)
from constructs import Construct
from aws_cdk.pipelines import CodePipeline, CodePipelineSource, CodeBuildStep, ManualApprovalStep, ShellStep
from helper import create_archive
from aws_glue_cdk_baseline.glue_app_stage import GlueAppStage


class PipelineStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, config: Dict, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        source = CodePipelineSource.connection(
            repo_string="xuao-private/aws-glue-cicd-test",
            branch="main",
            connection_arn="arn:aws:codeconnections:ap-northeast-1:288378107057:connection/92d6be0e-c606-4b63-a879-edf3f90e0d65"
        )
        
        pipeline = CodePipeline(self, "GluePipeline",
            pipeline_name="GluePipeline",
            cross_account_keys=True,
            docker_enabled_for_synth=True,
            synth=CodeBuildStep("CdkSynth_UnitTest",
                input=source,
                partial_build_spec=codebuild.BuildSpec.from_object({
                    "version": "0.2",
                    "phases": {
                        "install": {
                            "runtime-versions": {
                                "nodejs": "18",
                                "python": "3.10"
                            }
                        }
                    },
                    "cache": {
                        "paths": [
                            "/root/.cache/pip/**/*",
                            "/root/.npm/**/*"
                        ]
                    }
                }),
                install_commands=[
                    "pip install --upgrade pip",
                    "pip install -r requirements-dev.txt",
                    "pip install -r requirements.txt",
                    "npm install -g aws-cdk",
                ],
                commands=[
                    "cdk synth -c stage=dev",
                    # Unit test for CDK stack
                    "python -m pytest",
                    # Unit test for job scripts
                    "WORKSPACE_LOCATION=$(pwd)/aws_glue_cdk_baseline/job_scripts/",
                    "echo $WORKSPACE_LOCATION",
                    "docker pull amazon/aws-glue-libs:glue_libs_4.0.0_image_01",
                    "docker run -v ~/.aws:/home/glue_user/.aws -v $WORKSPACE_LOCATION:/home/glue_user/workspace/"
                    " -e DISABLE_SSL=true --rm -p 4040:4040 -p 18080:18080"
                    f" -e AWS_REGION={config['pipelineAccount']['awsAccountId']} -e AWS_CONTAINER_CREDENTIALS_RELATIVE_URI"
                    " --name glue_pytest amazon/aws-glue-libs:glue_libs_4.0.0_image_01 -c \"python3 -m pytest\"",
                ],
                role_policy_statements=[
                    # S3 read only
                    iam.PolicyStatement(
                        actions=[
                            "s3:ListBucket",
                            "s3:GetObject"
                        ],
                        resources=["*"]
                    ),
                    # Glue read only
                    iam.PolicyStatement(
                        actions=[
                            "glue:GetDatabase",
                            "glue:GetDatabases",
                            "glue:GetTable",
                            "glue:GetTables",
                            "glue:GetTableVersion",
                            "glue:GetTableVersions",
                            "glue:GetPartition",
                            "glue:GetPartitions",
                            "glue:BatchGetPartition",
                            "glue:GetPartitionIndexes",
                        ],
                        resources=[
                            "arn:aws:glue:*:*:catalog",
                            "arn:aws:glue:*:*:database/*",
                            "arn:aws:glue:*:*:table/*"
                        ]
                    )
                ]
            )
        )
        
        # Dev deployment
        dev_stage_name = "DeployDev"
        if config["pipelineAccount"]["awsAccountId"] == config["devAccount"]["awsAccountId"] and config["pipelineAccount"]["awsRegion"] == config["devAccount"]["awsRegion"]:
            dev_env = None
        else:
            dev_env = Environment(
                account=str(config["devAccount"]["awsAccountId"]), 
                region=config["devAccount"]["awsRegion"]
            )
        dev_stage_app = GlueAppStage(
            self, 
            dev_stage_name,
            config=config,
            stage="dev",
            env=dev_env
        )
        dev_stage = pipeline.add_stage(dev_stage_app)


        # Integ test
        dev_stage.add_post(CodeBuildStep("IntegrationTest",
                input=source,
                partial_build_spec=codebuild.BuildSpec.from_object({
                    "version": "0.2",
                    "phases": {
                        "install": {
                            "runtime-versions": {
                                "nodejs": "18",
                                "python": "3.10"
                            }
                        }
                    }
                }),
                install_commands=[
                    "pip install --upgrade pip",
                    "pip install -r requirements-dev.txt"
                ],
                commands=[
                    # Integ test for Glue App stack
                    f"python $(pwd)/tests/integ/integ_test_glue_app_stack.py --account {str(config['devAccount']['awsAccountId'])} --region {config['devAccount']['awsRegion']} --stage-name {dev_stage_name} --sts-role-arn {dev_stage_app.iam_role_arn}",
                ],
                role_policy_statements=[
                    # Glue only
                    iam.PolicyStatement(
                        actions=[
                            "sts:AssumeRole"
                        ],
                        resources=[
                            "*"
                        ]
                    )
                ]
            )
        )

        # Prod deployment
        prod_stage_name = "DeployProd"
        if config["pipelineAccount"]["awsAccountId"] == config["prodAccount"]["awsAccountId"] and config["pipelineAccount"]["awsRegion"] == config["prodAccount"]["awsRegion"]:
            prod_env = None
        else:
            prod_env = Environment(
                account=str(config["prodAccount"]["awsAccountId"]), 
                region=config["prodAccount"]["awsRegion"]
            )
        prod_stage_app = GlueAppStage(
            self, 
            prod_stage_name,
            config=config,
            stage="prod",
            env=prod_env
        )
        prod_stage = pipeline.add_stage(prod_stage_app)
        prod_stage.add_pre(ManualApprovalStep("Approval"))