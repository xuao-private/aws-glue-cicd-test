import yaml
import json
import os

def generate_mapping():
    with open('default-config.yaml', 'r') as config_file:
        config = yaml.safe_load(config_file)
    
    # 从环境变量获取目标环境
    target_env = os.environ.get('TARGET_ENV', 'prod')
    
    # 源账号（开发账号）
    src_account = config['devAccount']['awsAccountId']
    src_region = config['devAccount']['awsRegion']
    
    # 目标账号
    target_account = config[f"{target_env}Account"]['awsAccountId']
    target_region = config[f"{target_env}Account"]['awsRegion']
    
    # 基础映射
    mapping = {
        # S3 Assets 映射（用于脚本路径）
        f"s3://aws-glue-assets-{src_account}-{src_region}": 
        f"s3://aws-glue-assets-{target_account}-{target_region}",
        
        # IAM 角色映射（替换账号ID部分，保留完整路径）
        f"arn:aws:iam::{src_account}:role/": 
        f"arn:aws:iam::{target_account}:role/",
    }
    
    # 智能遍历所有 job 的配置
    if target_env in config and 'jobs' in config[target_env]:
        for job_name, job_config in config[target_env]['jobs'].items():
            for key, value in job_config.items():
                if isinstance(value, str):
                    placeholder = f"{{{job_name}.{key}}}"
                    mapping[placeholder] = value
                    print(f"Job mapping: {placeholder} → {value}")
    
    with open('mapping.json', 'w') as f:
        json.dump(mapping, f, indent=2)
    
    print(f"\n✅ Mapping generated for {target_env}")
    print(f"   Source: {src_account}/{src_region}")
    print(f"   Target: {target_account}/{target_region}")
    print(f"   Total mappings: {len(mapping)}")

if __name__ == "__main__":
    generate_mapping()