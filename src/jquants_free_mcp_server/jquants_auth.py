import json
import os
from datetime import datetime, timedelta
import requests

# IDトークンのファイルパスを定義
ID_TOKEN_FILE_PATH = "jquantsapi-id-token.txt"
ID_TOKEN_EXPIRY_FILE_PATH = "jquantsapi-id-token-expiry.txt"
REFRESH_TOKEN_FILE_PATH = "jquantsapi-key.txt"

def get_refresh_token_from_file(refresh_token_file_path: str = REFRESH_TOKEN_FILE_PATH):
    """リフレッシュトークン読み込み(ファイルから)"""
    with open(refresh_token_file_path, "r") as f:
        refresh_token = f.read()
    return refresh_token.rstrip().lstrip()

def get_refresh_token(mail_address: str, password: str) -> dict:
    """リフレッシュトークン取得(mail, passから)"""
    data = {"mailaddress": mail_address, "password": password}
    r_post = requests.post("https://api.jquants.com/v1/token/auth_user", data=json.dumps(data))
    return r_post.json()

def save_id_token(id_token: str) -> None:
    """IDトークンをファイルに保存"""
    with open(ID_TOKEN_FILE_PATH, mode="w") as f:
        f.write(id_token)
    
    # IDトークンの有効期限（1時間）を保存
    expiry_time = datetime.now() + timedelta(hours=1)
    with open(ID_TOKEN_EXPIRY_FILE_PATH, mode="w") as f:
        f.write(expiry_time.isoformat())

def get_id_token_from_file() -> str:
    """ファイルからIDトークンを読み込み、有効期限内であれば返す"""
    if not os.path.exists(ID_TOKEN_FILE_PATH) or not os.path.exists(ID_TOKEN_EXPIRY_FILE_PATH):
        return None
    
    with open(ID_TOKEN_EXPIRY_FILE_PATH, "r") as f:
        expiry_time = datetime.fromisoformat(f.read().strip())
    
    # 有効期限切れの場合はNoneを返す（10分の余裕を持たせる）
    if datetime.now() > expiry_time - timedelta(minutes=10):
        return None
    
    with open(ID_TOKEN_FILE_PATH, "r") as f:
        id_token = f.read().strip()
    
    return id_token

def get_id_token(mail_address: str, password: str) -> str:
    """メールアドレスとパスワードからIDトークンを取得"""
    # リフレッシュトークンを取得
    refresh_token_data = get_refresh_token(mail_address, password)
    refresh_token = refresh_token_data["refreshToken"]
    
    # リフレッシュトークンをファイルに保存
    with open(REFRESH_TOKEN_FILE_PATH, mode="w") as f:
        f.write(refresh_token)
    
    # IDトークンを取得
    cli = jquantsapi.Client(refresh_token=refresh_token)
    id_token = cli.get_id_token()
    
    # IDトークンを保存
    save_id_token(id_token)
    
    return id_token
