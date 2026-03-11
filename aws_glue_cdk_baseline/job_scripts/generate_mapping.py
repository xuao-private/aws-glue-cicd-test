import yaml
import json
import os

def generate_mapping():
    with open('default-config.yaml', 'r') as config_file:
        config = yaml.safe_load(config_file)
    
    # 从环境变量获取目标环境
    target_env = os.environ.get('TARGET_ENV', 'prod')
    
    # 根据目标环境选择目标账号
    if target_env == 'dev':
        target_account = config['devAccount']['awsAccountId']
        target_region = config['devAccount']['awsRegion']
    else:  # prod
        target_account = config['prodAccount']['awsAccountId']
        target_region = config['prodAccount']['awsRegion']
    
    mapping = {
        f"s3://aws-glue-assets-{config['devAccount']['awsAccountId']}-{config['devAccount']['awsRegion']}": 
        f"s3://aws-glue-assets-{target_account}-{target_region}",
        
        f"arn:aws:iam::{config['devAccount']['awsAccountId']}:role/service-role/AWSGlueServiceRole": 
        f"arn:aws:iam::{target_account}:role/service-role/AWSGlueServiceRole",
        
        f"s3://dev-glue-data-{config['devAccount']['awsAccountId']}-{config['devAccount']['awsRegion']}": 
        f"s3://{target_env}-glue-data-{target_account}-{target_region}"
    }
    
    with open('mapping.json', 'w') as mapping_file:
        json.dump(mapping, mapping_file, indent=2)
    print(f"Mapping generated for target environment: {target_env}")

if __name__ == "__main__":
    generate_mapping()