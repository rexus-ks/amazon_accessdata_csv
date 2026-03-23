import os
import glob
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# --- 環境変数から認証情報を取得 ---
creds = Credentials(
    None,
    refresh_token=os.environ['GDRIVE_REFRESH_TOKEN'],
    client_id=os.environ['GDRIVE_CLIENT_ID'],
    client_secret=os.environ['GDRIVE_CLIENT_SECRET'],
    token_uri="https://oauth2.googleapis.com/token"
)

# --- Google Drive API に接続 ---
service = build('drive', 'v3', credentials=creds)

# --- 最新の CSV ファイルを自動取得 ---
list_of_files = glob.glob('ama_access_*.csv')  # ama_access_で始まるCSVを対象
if not list_of_files:
    raise FileNotFoundError("ama_access_*.csv ファイルが見つかりません")
file_name = max(list_of_files, key=os.path.getmtime)  # 更新日時で最新ファイルを選択

# --- Google Drive 上の保存先指定（任意のフォルダID） ---
folder_id = '1ZvEUgAFy2Rzq7GXcnoWst2-ZNcruiesL'  # 文字列として指定
file_metadata = {
    'name': file_name,
    'parents': [folder_id]  # リストに文字列を入れる
}

# --- アップロード ---
media = MediaFileUpload(file_name, mimetype='text/csv')
file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()

print(f'Uploaded file ID: {file.get("id")}')
print(f'File uploaded: {file_name} to folder ID: {folder_id}')