from awsglue.dynamicframe import DynamicFrame
from awsglue.context import GlueContext
from pyspark.context import SparkContext

def check_file_exist(self, input_path):
    import boto3
    from botocore.exceptions import ClientError
    
    # get logger
    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    logger = glueContext.get_logger()
    
    logger.info("CSVファイル存在チェックを開始しました-deploy")
    logger.info(f"チェックするCSVファイル: {input_path}")
    
    # S3パスを解析
    try:
        path_parts = input_path.replace('s3://', '').split('/', 1)
        if len(path_parts) != 2:
            error_msg = f"無効なS3パス形式です: {input_path}"
            logger.error(error_msg)
            raise Exception(error_msg)
            
        bucket, key = path_parts[0], path_parts[1]
        logger.info(f"バケット: {bucket}, キー: {key}")
        
    except Exception as e:
        logger.error(f"S3パス解析エラー: {str(e)}")
        raise Exception(f"S3パスの解析に失敗しました: {input_path}")
    
    # ファイルの存在チェック
    s3_client = boto3.client('s3')
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        logger.info(f"✓ ファイルが存在します: {input_path}")
        return self
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        
        if error_code == '404' or error_code == 'NoSuchKey':
            error_msg = f"✗ ファイルが存在しません: {input_path}"
            logger.error(error_msg)
            raise Exception(error_msg)
            
        elif error_code == '403':
            error_msg = f"✗ S3アクセス権限がありません: {bucket}"
            logger.error(error_msg)
            raise Exception(f"S3バケットへのアクセスが拒否されました: {bucket}")
            
        else:
            error_msg = f"✗ ファイルチェック中にエラーが発生しました: {error_code} - {str(e)}"
            logger.error(error_msg)
            raise Exception(error_msg)

DynamicFrame.check_file_exist = check_file_exist