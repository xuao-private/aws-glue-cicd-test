import yaml
import json
import os

def generate_mapping():
    with open('default-config.yaml', 'r') as config_file:
        config = yaml.safe_load(config_file)
    
    # 从环境变量获取当前阶段
    current_stage = os.environ.get('STAGE', 'dev')
    
    # 确定目标环境
    if current_stage == 'dev':
        target_stage = 'dev'
    elif current_stage == 'stg':
        target_stage = 'stg'
    elif current_stage == 'prod':
        target_stage = 'prod'
    else:
        target_stage = 'prod'
    
    # 获取源和目标账号信息
    src_account = config[f"{current_stage}Account"]['awsAccountId']
    src_region = config[f"{current_stage}Account"]['awsRegion']
    dst_account = config[f"{target_stage}Account"]['awsAccountId']
    dst_region = config[f"{target_stage}Account"]['awsRegion']
    
    # 构建基本的资源映射
    mapping = {
        # S3 Assets 映射（脚本位置）
        f"s3://aws-glue-assets-{src_account}-{src_region}": 
        f"s3://aws-glue-assets-{dst_account}-{dst_region}",
        
        # IAM 角色映射
        f"arn:aws:iam::{src_account}:role/": 
        f"arn:aws:iam::{dst_account}:role/",
    }
    
    with open('mapping.json', 'w') as mapping_file:
        json.dump(mapping, mapping_file, indent=2)
    print(f"Basic resource mapping generated for stage '{current_stage}'")

if __name__ == "__main__":
    generate_mapping()