# AWS Glue CI/CD パイプラインテンプレート

AWS CDK を使用して構築された Glue ジョブの CI/CD テンプレートです。マルチ環境の独立したパイプラインをサポートします。

## アーキテクチャの特徴
- 独立したパイプライン：各環境（dev/stg/prod）に独自のパイプライン
- GitHub 直接連携：CodeCommit 不要、GitHub ブランチを直接監視
- 設定ファイル分離：環境ごとに設定ファイルを分離
- Visual ジョブ対応：`resources.json` で Visual モードのジョブを管理

## 前提条件
- Python 3.9 以上
- AWS アカウント（開発環境と本番環境は同一でも異なってもOK）
- AWS CLI 各環境のプロファイル設定済み
- AWS CDK Toolkit 2.87.0 以上
- GitHub アカウントと CodeStar Connection

## プロジェクト構造
.
├── aws_glue_cdk_baseline/
│ ├── job_scripts/
│ │ ├── generate_mapping.py # 環境マッピング生成
│ │ └── sync.py # Glue ジョブ同期ツール
│ ├── resources/
│ │ └── resources.json # Glue ジョブ定義（Visual モード）
│ └── pipeline_stack.py # パイプライン定義
├── config/
│ ├── dev.yaml # 開発環境設定
│ ├── stg.yaml # ステージング環境設定
│ └── prod.yaml # 本番環境設定
├── app.py # CDK アプリケーションエントリ
└── cdk.json # CDK 設定ファイル

## セットアップ手順
```bash
# リポジトリのクローン
git clone https://github.com/your-repo/aws-glue-cicd-test.git
cd aws-glue-cicd-test

# 仮想環境の作成
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# .venv\Scripts\activate     # Windows

# 依存関係のインストール
pip install -r requirements.txt
```
## 設定ファイルの作成
config/dev.yaml
pipelineAccount:
  awsAccountId: "xxxxxx"
  awsRegion: "ap-northeast-1"
assets_path: "s3://aws-glue-assets-xxxxxx-ap-northeast-1"
github:
  repo: "your-org/your-repo"
  branch: "dev"
  connection_arn: "arn:aws:codeconnections:ap-northeast-1:xxxxxx:connection/xxxx"
jobs:
  your-job:
    input_path: "s3://your-bucket/dev/path/"
    table_name: "your_table_dev"
    glue_role: "arn:aws:iam::xxxxxx:role/service-role/YourGlueRole"

config/stg.yaml
...
config/prod.yaml
...

## Glue ジョブの定義
aws_glue_cdk_baseline/resources/resources.json
{
  "jobs": [
    {
      "Name": "your-job",
      "JobMode": "VISUAL",
      "Role": "{your-job.glue_role}",
      "Command": {
        "ScriptLocation": "{assets_path}/scripts/your-job.py",
        "PythonVersion": "3"
      },
      "DefaultArguments": {
        "--enable-metrics": "true",
        "--enable-spark-ui": "true",
        "--spark-event-logs-path": "{assets_path}/sparkHistoryLogs/",
        "--TempDir": "{assets_path}/temporary/"
      },
      "GlueVersion": "5.0",
      "WorkerType": "G.1X",
      "NumberOfWorkers": 2,
      "CodeGenConfigurationNodes": {}
    }
  ]
}

## パイプラインのデプロイ
# 開発環境
cdk deploy --profile dev-account -c envType=dev
# ステージング環境
cdk deploy --profile dev-account -c envType=stg
# 本番環境
cdk deploy --profile prod-account -c envType=prod

## 環境変数
TARGET_ENV: ターゲット環境（dev/stg/prod）

## リソースの削除
cdk destroy --profile dev-account -c envType=dev
cdk destroy --profile dev-account -c envType=stg
cdk destroy --profile prod-account -c envType=prod

## ジョブのjsonを取得
python aws_glue_cdk_baseline\job_scripts\sync.py `
  --src-profile dev-account `
  --src-region ap-northeast-1 `
  --src-job-names "ifjk002-deploy-test2" `
  --serialize-to-file resources\resources.json `
  --targets job `
  --skip-prompt