import boto3
import json
import os
import glob
import yaml
from pathlib import Path

def publish_transforms():
    """カスタムノード公開スクリプト - 環境プレフィックス自動追加"""
    
    print("カスタムノードの公開を開始します...")
    
    # 環境変数からターゲット環境を取得
    target_env = os.environ.get('TARGET_ENV', 'dev')
    
    # common.yaml から設定を読み込む
    common_file = f"config/{target_env}/common.yaml"
    try:
        with open(common_file, 'r', encoding='utf-8') as f:
            common_config = yaml.safe_load(f)
    except FileNotFoundError:
        print(f"設定ファイルが見つかりません: {common_file}")
        return
    except Exception as e:
        print(f"設定ファイルの読み込みに失敗しました: {e}")
        return
    
    # common.yaml からアカウント情報を取得
    pipeline_account = common_config.get('pipelineAccount', {})
    account_id = pipeline_account.get('awsAccountId')
    region = pipeline_account.get('awsRegion')
    
    if not account_id or not region:
        print("common.yaml に pipelineAccount 情報がありません")
        return
    
    # assets_path からバケット名を取得
    assets_path = common_config.get('assets_path', '')
    if assets_path.startswith('s3://'):
        bucket = assets_path[5:].split('/')[0]
    else:
        bucket = f"aws-glue-assets-{account_id}-{region}"
    
    print(f"ターゲット環境: {target_env}")
    print(f"アカウントID: {account_id}")
    print(f"リージョン: {region}")
    print(f"S3 バケット: {bucket}")
    
    s3 = boto3.client('s3')
    
    # 全てのノードディレクトリを探索
    transform_dirs = glob.glob('glue_common_transforms/*/')
    
    if not transform_dirs:
        print("カスタムノードが見つかりません")
        return
    
    total_uploaded = 0
    
    for transform_dir in transform_dirs:
        transform_dir = Path(transform_dir)
        transform_name = transform_dir.name
        
        print(f"\nノードを処理中: {transform_name}")
        
        json_files = list(transform_dir.glob('*.json'))
        py_files = list(transform_dir.glob('*.py'))
        
        node_uploaded = 0
        node_failed = 0
        
        # JSON ファイルアップロード
        for json_file in json_files:
            new_filename = f"{target_env}_{json_file.name}"
            s3_key = f"transforms/{new_filename}"
            try:
                s3.upload_file(str(json_file), bucket, s3_key)
                node_uploaded += 1
            except Exception as e:
                print(f"JSON ファイルアップロード失敗: {json_file.name} - {e}")
                node_failed += 1
                raise e  # 失敗時に終了
        
        # Python ファイルアップロード
        for py_file in py_files:
            new_filename = f"{target_env}_{py_file.name}"
            s3_key = f"transforms/{new_filename}"
            try:
                s3.upload_file(str(py_file), bucket, s3_key)
                node_uploaded += 1
            except Exception as e:
                print(f"Python ファイルアップロード失敗: {py_file.name} - {e}")
                node_failed += 1
                raise e  # 失敗時に終了
        
        print(f"ノード '{transform_name}' のアップロード結果: 成功 {node_uploaded} 件, 失敗 {node_failed} 件")
        total_uploaded += node_uploaded
    
    print(f"\nカスタムノードの公開が完了しました。合計 {total_uploaded} ファイルをアップロードしました")
    print(f"環境プレフィックス '{target_env}_' が自動的に追加されました")

if __name__ == "__main__":
    publish_transforms()