import yaml
import json
import os

def generate_mapping():
    with open('default-config.yaml', 'r') as config_file:
        config = yaml.safe_load(config_file)
    
    # 環境変数からターゲット環境を取得
    target_env = os.environ.get('TARGET_ENV', 'prod')
    
    # 現在の環境の設定を取得
    env_config = config[target_env]
    
    mapping = {}
    
    # すべての設定項目をループ
    for key, value in env_config.items():
        if isinstance(value, str):
            mapping[f"{{{key}}}"] = value
            print(f"Env mapping: {{{key}}} → {value}")
        elif isinstance(value, dict) and key == 'jobs':
            # jobs 内の設定を処理
            for job_name, job_config in value.items():
                for job_key, job_value in job_config.items():
                    if isinstance(job_value, str):
                        placeholder = f"{{{job_name}.{job_key}}}"
                        mapping[placeholder] = job_value
                        print(f"Job mapping: {placeholder} → {job_value}")
    
    with open('mapping.json', 'w') as f:
        json.dump(mapping, f, indent=2)
    
    print(f"\nMapping generated for {target_env}")
    print(f"   Total mappings: {len(mapping)}")

if __name__ == "__main__":
    generate_mapping()