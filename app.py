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

# 対応する環境の設定ファイルを読み込み
config_file = f"./config/{env_type}.yaml"
print(f"環境タイプ: {env_type}")
print(f"設定ファイル: {config_file}")

with open(config_file, 'r', encoding="utf-8") as f:
    config = yaml.load(f, Loader=yaml.SafeLoader)

# PipelineStack を作成
PipelineStack(
    app, 
    f"PipelineStack-{env_type}",  # 環境ごとにスタック名を区別
    config=config,
    env_type=env_type,
    env=cdk.Environment(
        account=str(config['pipelineAccount']['awsAccountId']), 
        region=config['pipelineAccount']['awsRegion']
    )
)

app.synth()