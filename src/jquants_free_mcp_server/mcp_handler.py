from jquants_auth import get_id_token, get_id_token_from_file
import jquantsapi
import json
from typing import Dict, Any

class JQuantsMCPHandler:
    def __init__(self):
        self.client = None
        
    def initialize_client(self, mail_address: str = None, password: str = None) -> bool:
        """J-Quants APIクライアントを初期化"""
        # ファイルからIDトークンを取得
        id_token = get_id_token_from_file()
        
        if id_token:
            self.client = jquantsapi.Client(refresh_token=None, id_token=id_token)
            return True
        
        # ファイルにIDトークンがない場合はメールアドレスとパスワードから取得
        if mail_address and password:
            id_token = get_id_token(mail_address, password)
            if id_token:
                self.client = jquantsapi.Client(refresh_token=None, id_token=id_token)
                return True
        
        return False
    
    def process_query(self, query: str) -> Dict[str, Any]:
        """自然言語クエリを処理して結果を返す"""
        if not self.client:
            return {"error": "Client not initialized"}
        
        # クエリ解析と適切なAPI呼び出し
        if "株価" in query or "価格" in query:
            return self._handle_price_query(query)
        elif "財務" in query or "決算" in query:
            return self._handle_finance_query(query)
        elif "信用" in query or "空売り" in query:
            return self._handle_margin_query(query)
        else:
            return {"error": "Unsupported query type"}
    
    def _handle_price_query(self, query: str) -> Dict[str, Any]:
        """株価関連のクエリを処理"""
        # 簡易的な実装 - 実際にはクエリ解析を強化する必要あり
        try:
            prices = self.client.get_price_range()
            return {"result": prices.to_dict()}
        except Exception as e:
            return {"error": str(e)}
    
    def _handle_finance_query(self, query: str) -> Dict[str, Any]:
        """財務関連のクエリを処理"""
        try:
            statements = self.client.get_statements_range()
            return {"result": statements.to_dict()}
        except Exception as e:
            return {"error": str(e)}
    
    def _handle_margin_query(self, query: str) -> Dict[str, Any]:
        """信用取引関連のクエリを処理"""
        try:
            margin_data = self.client.get_weekly_margin_range()
            return {"result": margin_data.to_dict()}
        except Exception as e:
            return {"error": str(e)}

def handle_mcp_request(params: Dict[str, Any]) -> Dict[str, Any]:
    """MCPサーバーからのリクエストを処理するエントリポイント"""
    handler = JQuantsMCPHandler()
    
    # 認証情報の取得
    mail_address = params.get("mailaddress")
    password = params.get("password")
    query = params.get("query")
    
    # クライアント初期化
    if not handler.initialize_client(mail_address, password):
        return {"error": "Failed to initialize J-Quants client"}
    
    # クエリ処理
    if query:
        return handler.process_query(query)
    else:
        return {"error": "No query provided"}
