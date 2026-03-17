import boto3
import os
import yaml
from pathlib import Path
import logging
from botocore.exceptions import ClientError, NoCredentialsError
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

# ロギング設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

class PublishTransformsError(Exception):
    """カスタム例外クラス"""
    pass

def flatten_dict(d, parent_key='', sep='.'):
    if not isinstance(d, dict):
        return {}
    items = {}
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.update(flatten_dict(v, new_key, sep))
        elif v is not None:
            items[new_key] = v
    return items

def replace_variables_in_json(content, target_env, flat_config):
    try:
        for key, value in flat_config.items():
            if isinstance(value, str):
                content = content.replace(f"{{{key}}}", value)
        content = content.replace("{env}", target_env)
        content = content.replace("{ENV}", target_env.upper())
        return content
    except Exception as e:
        logger.error(f"変数置換中にエラーが発生しました: {e}")
        raise PublishTransformsError(f"変数置換失敗: {e}")

def upload_file_safe(s3_client, content, bucket, s3_key, is_binary=False):
    try:
        if is_binary:
            s3_client.upload_file(str(content), bucket, s3_key)
        else:
            import io
            content_bytes = content.encode('utf-8')
            content_stream = io.BytesIO(content_bytes)
            s3_client.upload_fileobj(content_stream, bucket, s3_key)
        logger.debug(f"アップロード成功: s3://{bucket}/{s3_key}")
        return True
    except ClientError as e:
        logger.error(f"S3アップロード中にクライアントエラー: {e}")
        return False
    except Exception as e:
        logger.error(f"S3アップロード中に予期せぬエラー: {e}")
        return False

def publish_transforms():
    target_env = os.environ.get("TARGET_ENV", "").strip()
    if not target_env:
        logger.error("環境変数 TARGET_ENV が設定されていません")
        sys.exit(1)
    logger.info(f"対象環境: {target_env}")

    # 設定ファイル
    common_file = Path(f"config/{target_env}/common.yaml")
    if not common_file.exists():
        logger.error(f"設定ファイルが見つかりません: {common_file}")
        sys.exit(1)
    try:
        with open(common_file, "r", encoding="utf-8") as f:
            common_config = yaml.safe_load(f) or {}
        logger.info(f"設定ファイル読み込み完了: {common_file}")
    except Exception as e:
        logger.error(f"設定ファイル読み込みエラー: {e}")
        sys.exit(1)

    pipeline_account = common_config.get("pipelineAccount", {})
    account_id = pipeline_account.get("awsAccountId")
    region = pipeline_account.get("awsRegion")
    if not account_id or not region:
        logger.error("common.yaml に pipelineAccount の設定が不足しています")
        sys.exit(1)

    assets_path = common_config.get("assets_path", "")
    if assets_path.startswith("s3://"):
        bucket = assets_path[5:].split("/")[0]
    else:
        bucket = f"aws-glue-assets-{account_id}-{region}"

    try:
        s3_client = boto3.client("s3", region_name=region)
    except Exception as e:
        logger.error(f"S3クライアント初期化エラー: {e}")
        sys.exit(1)

    flat_config = flatten_dict(common_config)
    transform_root = Path("glue_common_transforms")
    if not transform_root.exists():
        logger.info("glue_common_transforms ディレクトリが存在しません。スキップします。")
        return
    transform_dirs = [p for p in transform_root.iterdir() if p.is_dir()]
    if not transform_dirs:
        logger.info("カスタムノードが見つかりません。処理をスキップします。")
        return

    total_uploaded = 0
    total_failed = 0
    success_nodes = []
    failed_nodes = []

    # 並列アップロード実行
    for transform_dir in transform_dirs:
        transform_name = transform_dir.name
        logger.info(f"\n--- ノード処理: {transform_name} ---")
        files_to_upload = []

        for json_file in transform_dir.glob("*.json"):
            with open(json_file, "r", encoding="utf-8") as f:
                content = replace_variables_in_json(f.read(), target_env, flat_config)
            new_filename = f"{target_env}_{json_file.name}"
            s3_key = f"transforms/{new_filename}"
            files_to_upload.append((content, s3_key, False, json_file.name))

        for py_file in transform_dir.glob("*.py"):
            new_filename = f"{target_env}_{py_file.name}"
            s3_key = f"transforms/{new_filename}"
            files_to_upload.append((py_file, s3_key, True, py_file.name))

        node_uploaded = 0
        node_failed = 0

        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_file = {
                executor.submit(upload_file_safe, s3_client, f[0], bucket, f[1], f[2]): f[3]
                for f in files_to_upload
            }
            for future in as_completed(future_to_file):
                filename = future_to_file[future]
                try:
                    result = future.result()
                    if result:
                        node_uploaded += 1
                        logger.info(f"[UPLOAD] {filename} 成功")
                    else:
                        node_failed += 1
                        logger.error(f"[UPLOAD] {filename} 失敗")
                except Exception as e:
                    node_failed += 1
                    logger.error(f"[UPLOAD] {filename} 処理中に例外: {e}")

        total_uploaded += node_uploaded
        total_failed += node_failed
        if node_failed == 0:
            success_nodes.append(transform_name)
        else:
            failed_nodes.append(transform_name)
        logger.info(f"ノード '{transform_name}' 結果: 成功 {node_uploaded}, 失敗 {node_failed}")

    logger.info("=" * 50)
    logger.info(f"全体結果: 成功 {total_uploaded}, 失敗 {total_failed}")
    if success_nodes:
        logger.info(f"成功ノード: {', '.join(success_nodes)}")
    if failed_nodes:
        logger.warning(f"失敗ノード: {', '.join(failed_nodes)}")
    logger.info(f"環境プレフィックス '{target_env}_' を付与しました")
    logger.info("=" * 50)

    sys.exit(1 if total_failed > 0 else 0)

if __name__ == "__main__":
    try:
        publish_transforms()
    except KeyboardInterrupt:
        logger.info("ユーザーによって中断されました")
        sys.exit(130)
    except Exception as e:
        logger.error(f"予期せぬエラーが発生しました: {e}")
        sys.exit(1)