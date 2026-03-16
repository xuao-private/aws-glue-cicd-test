import boto3
import json
import os
import glob
import yaml
from pathlib import Path

def publish_transforms():
    """カスタムノード公開スクリプト"""
    
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
        # s3://aws-glue-assets-deploy-test-dev 形式からバケット名を抽出
        bucket = assets_path[5:].split('/')[0]
    else:
        # assets_path がない場合、デフォルトの命名規則を使用
        bucket = f"aws-glue-assets-{account_id}-{region}"
    
    print(f"ターゲット環境: {target_env}")
    print(f"アカウントID: {account_id}")
    print(f"リージョン: {region}")
    print(f"S3 バケット: {bucket}")
    
    s3 = boto3.client('s3')
    
    # バケットが存在するか確認
    try:
        s3.head_bucket(Bucket=bucket)
        print(f"バケットが存在します: {bucket}")
    except Exception as e:
        print(f"バケットが存在しないか、アクセス権限がありません: {bucket}")
        print(f"エラー: {e}")
        return
    
    # 全てのノードディレクトリを探索
    transform_dirs = glob.glob('glue_common_transforms/*/')
    
    if not transform_dirs:
        print("カスタムノードが見つかりません")
        return
    
    uploaded_count = 0
    
    for transform_dir in transform_dirs:
        transform_dir = Path(transform_dir)
        transform_name = transform_dir.name
        
        print(f"\nノードを処理中: {transform_name}")
        
        # JSON ファイルを探す
        json_files = list(transform_dir.glob('*.json'))
        if not json_files:
            print("  JSON 定義ファイルが見つかりません")
            continue
        
        # Python ファイルを探す
        py_files = list(transform_dir.glob('*.py'))
        if not py_files:
            print("  Python コードファイルが見つかりません")
            continue
        
        # JSON ファイルをアップロード - transforms/ 直下にアップロード
        for json_file in json_files:
            s3_key = f"transforms/{json_file.name}"
            try:
                s3.upload_file(str(json_file), bucket, s3_key)
                print(f"  アップロード成功: {json_file.name} → s3://{bucket}/{s3_key}")
                uploaded_count += 1
            except Exception as e:
                print(f"  アップロード失敗: {json_file.name} - {e}")
        
        # Python ファイルをアップロード - transforms/ 直下にアップロード
        for py_file in py_files:
            s3_key = f"transforms/{py_file.name}"
            try:
                s3.upload_file(str(py_file), bucket, s3_key)
                print(f"  アップロード成功: {py_file.name} → s3://{bucket}/{s3_key}")
                uploaded_count += 1
            except Exception as e:
                print(f"  アップロード失敗: {py_file.name} - {e}")
    
    print(f"\nカスタムノードの公開が完了しました。合計 {uploaded_count} ファイルをアップロードしました")

if __name__ == "__main__":
    publish_transforms()