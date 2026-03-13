#!/usr/bin/env python3

import os
import yaml
import aws_cdk as cdk
from aws_glue_cdk_baseline.pipeline_stack import PipelineStack

app = cdk.App()

# context から環境タイプを取得
env_type = app.node.try_get_context("envType")
if not env_type:
    print("環境タイプを指定してください: -c envType=dev または -c envType=stg または -c envType=prod")
    exit(1)

# 共通設定ファイルを読み込み
common_config_file = f"./config/{env_type}/common.yaml"
print(f"共通設定ファイル: {common_config_file}")

with open(common_config_file, 'r', encoding="utf-8") as f:
    config = yaml.load(f, Loader=yaml.SafeLoader)

# すべてのジョブ設定を読み込み（必要な場合）
# 必要に応じて、ここにジョブ設定の読み込みロジックを追加可能

print(f"環境タイプ: {env_type}")
print(f"設定ファイル: {common_config_file}")

# PipelineStack を作成
PipelineStack(
    app, 
    f"{env_type}-pipeline-stack-glue-test-bydl",
    config=config,
    env_type=env_type,
    env=cdk.Environment(
        account=str(config['pipelineAccount']['awsAccountId']), 
        region=config['pipelineAccount']['awsRegion']
    )
)

app.synth()