from mcp_handler import handle_mcp_request
import os
from dotenv import load_dotenv

# 環境変数の読み込み
load_dotenv()

# J-Quantsの認証情報
JQ_MAIL = os.getenv("JQ_MAIL_ADDRESS")
JQ_PASS = os.getenv("JQ_PASSWORD")

# テストクエリの例
TEST_QUERIES = [
    "トヨタの株価を教えて",
    "ソフトバンクの財務情報を取得",
    "信用取引のデータを見せて"
]

def run_test_queries():
    for query in TEST_QUERIES:
        print(f"\n=== クエリ実行: '{query}' ===")
        
        params = {
            "mailaddress": JQ_MAIL,
            "password": JQ_PASS, 
            "query": query
        }
        
        result = handle_mcp_request(params)
        
        # 結果の表示
        if "error" in result:
            print(f"エラー: {result['error']}")
        else:
            print("取得成功 - 結果のサンプル:")
            # 最初の3アイテムだけ表示
            for key in list(result['result'].keys())[:3]:
                print(f"{key}: {result['result'][key][:50]}...")

if __name__ == "__main__":
    print("MCPハンドラのテストを開始します")
    run_test_queries()
    print("\nテスト完了")
