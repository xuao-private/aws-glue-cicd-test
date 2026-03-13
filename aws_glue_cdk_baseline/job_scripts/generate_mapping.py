import yaml
import json
import os
import glob

def generate_mapping():
    # 環境変数からターゲット環境を取得
    target_env = os.environ.get('TARGET_ENV', 'prod')
    
    mapping = {}
    
    # 1. 共通設定を読み込む
    common_file = f"./config/{target_env}/common.yaml"
    if os.path.exists(common_file):
        with open(common_file, 'r') as f:
            common = yaml.safe_load(f)
            for key, value in common.items():
                if isinstance(value, str):
                    mapping[f"{{{key}}}"] = value
                    print(f"Common mapping: {{{key}}} → {value}")
    
    # 2. 各ジョブの設定ファイルを読み込む（common.yaml 以外）
    for job_file in glob.glob(f"./config/{target_env}/*.yaml"):
        if job_file.endswith('common.yaml'):
            continue
        
        with open(job_file, 'r') as f:
            job_config = yaml.safe_load(f)
            # job_config は { job_name: { ... } } の形式
            for job_name, config in job_config.items():
                for key, value in config.items():
                    if isinstance(value, str):
                        placeholder = f"{{{job_name}.{key}}}"
                        mapping[placeholder] = value
                        print(f"Job mapping: {placeholder} → {value}")
    
    # 3. mapping.json を保存
    with open('mapping.json', 'w') as f:
        json.dump(mapping, f, indent=2)
    
    print(f"\n Mapping generated for {target_env}")
    print(f"   Total mappings: {len(mapping)}")

if __name__ == "__main__":
    generate_mapping()