from awsglue.dynamicframe import DynamicFrame
from awsglue.context import GlueContext
from pyspark.context import SparkContext
import boto3
from datetime import datetime

def email_notifier(self, 
                   source_email=None,
                   destination_emails=None):
    """
    メール送信を行う共通カスタムノード
    
    Parameters:
    - source_email: 送信元メールアドレス
    - destination_emails: 送信先メールアドレス（JSON形式のリスト）
                         例: '["user1@example.com", "user2@example.com"]'
    """
    
    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    logger = glueContext.get_logger()
    
    logger.info("=== メール送信処理開始-deploy ===")
    
    # プレビューモード検出
    import sys
    if '--JOB_RUN_ID' not in ' '.join(sys.argv):
        logger.info("プレビューモード: メール送信をスキップします")
        return self
    
    # パラメータチェック
    if not source_email:
        error_msg = "source_emailが指定されていません"
        logger.error(error_msg)
        raise Exception(error_msg)
    
    if not destination_emails:
        error_msg = "destination_emailsが指定されていません"
        logger.error(error_msg)
        raise Exception(error_msg)
    
    # 送信先メールアドレスをパース
    import json
    try:
        to_addresses = json.loads(destination_emails)
        if not isinstance(to_addresses, list):
            error_msg = "destination_emailsはJSON配列である必要があります"
            logger.error(error_msg)
            raise Exception(error_msg)
        logger.info(f"送信先: {to_addresses}")
    except json.JSONDecodeError as e:
        error_msg = f"destination_emailsのJSONパースに失敗: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)
    
    logger.info(f"送信元: {source_email}")
    
    # SESクライアント作成
    ses = boto3.client('ses', region_name='ap-northeast-1')
    
    # タイムスタンプ
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # ★★★ 固定の日本語メール内容 ★★★
    subject = "【Glue通知】ジョブ実行完了のお知らせ"
    
    body_text = f"""
Glueジョブが正常に完了しました。

━━━━━━━━━━━━━━━━━━━━━━
■ 実行日時: {timestamp}
■ ステータス: 正常完了
━━━━━━━━━━━━━━━━━━━━━━

詳細はGlueコンソールでご確認ください。

---
本メールは自動送信されています。
    """
    
    try:
        # メール送信
        response = ses.send_email(
            Source=source_email,
            Destination={
                'ToAddresses': to_addresses
            },
            Message={
                'Subject': {
                    'Data': subject,
                    'Charset': 'UTF-8'
                },
                'Body': {
                    'Text': {
                        'Data': body_text,
                        'Charset': 'UTF-8'
                    }
                }
            }
        )
        
        logger.info(f"✓ メール送信成功: {response['MessageId']}")
        logger.info(f"送信先: {to_addresses}")
        
    except Exception as e:
        logger.error(f"✗ メール送信失敗: {str(e)}")
        # メール送信失敗はジョブを止めない（警告のみ）
        # raise Exception(f"メール送信エラー: {str(e)}")
    
    logger.info("=== メール送信処理完了 ===")
    return self

# DynamicFrameにメソッドとして登録
DynamicFrame.email_notifier = email_notifier