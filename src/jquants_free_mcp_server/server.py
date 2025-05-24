import asyncio
import os
import json
from datetime import datetime, timedelta
from typing import Any
import httpx
from mcp.server.fastmcp import FastMCP

# Dify APIクライアント設定
DIFY_API_KEY = os.environ.get("DIFY_API_KEY", "")
DIFY_API_URL = os.environ.get("DIFY_API_URL", "https://api.dify.ai/v1")

mcp_server = FastMCP("JQuants-MCP-server")

async def make_requests(url: str,timeout: int = 30) -> dict[str, Any]:
    """
    Function to process requests

    Args:
        url (str): URL for the request
        timeout (int, optional): Timeout in seconds. Default is 30 seconds.

    Returns:
        str: API response text
    """
    try:
        idToken = os.environ.get("JQUANTS_ID_TOKEN", "")
        if not idToken:
            return {"error": "JQUANTS_ID_TOKENが設定されていません。", "status": "id_token_error"}

        async with httpx.AsyncClient(timeout=timeout) as client:
            headers = {'Authorization': 'Bearer {}'.format(idToken)}
            response = await client.get(url, headers=headers)
            if response.status_code != 200:
                return {"error": f"APIリクエストに失敗しました。ステータスコード: {response.status_code}", "status": "request_error"}
            if response.headers.get("Content-Type") != "application/json":
                return {"error": "APIレスポンスがJSON形式ではありません。", "status": "response_format_error"}

            return json.loads(response.text)

    except Exception as e:
        if isinstance(e, httpx.TimeoutException):
            error_msg =  f"タイムアウトエラーが発生しました。現在のタイムアウト設定: {timeout}秒"
            return {"error": error_msg, "status": "timeout"}
        elif isinstance(e, httpx.ConnectError):
            error_msg = "E-Stat APIサーバーへの接続に失敗しました。ネットワーク接続を確認してください。"
            return {"error": error_msg, "status": "connection_error"}
        elif isinstance(e, httpx.HTTPStatusError):
            error_msg = f"HTTPエラー（ステータスコード: {e.response.status_code}）が発生しました。"
            return {"error": error_msg, "status": "http_error"}
        else:
            error_msg = f"予期せぬエラーが発生しました: {str(e)}"
            return {"error": error_msg, "status": "unexpected_error"}

async def make_dify_request(prompt: str, context: str = "", timeout: int = 60) -> dict[str, Any]:
    """
    Dify APIにリクエストを送信し、LLM推論結果を取得
    
    Args:
        prompt (str): LLMへのプロンプト
        context (str, optional): 追加コンテキスト. Defaults to "".
        timeout (int, optional): タイムアウト秒数. Defaults to 60.
        
    Returns:
        dict[str, Any]: APIレスポンス
    """
    if not DIFY_API_KEY:
        return {"error": "DIFY_API_KEYが設定されていません", "status": "api_key_error"}
    
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            headers = {
                'Authorization': f'Bearer {DIFY_API_KEY}',
                'Content-Type': 'application/json'
            }
            data = {
                "inputs": {"prompt": prompt, "context": context},
                "response_mode": "blocking"
            }
            response = await client.post(
                f"{DIFY_API_URL}/completion-messages",
                headers=headers,
                json=data
            )
            
            if response.status_code != 200:
                return {
                    "error": f"Dify APIリクエスト失敗. ステータスコード: {response.status_code}",
                    "status": "request_error"
                }
                
            return response.json()
            
    except Exception as e:
        return {
            "error": f"Dify APIリクエスト中にエラー: {str(e)}",
            "status": "api_error"
        }


@mcp_server.tool()
async def search_company(
        query : str,
        limit : int = 10,
        start_position : int = 0,
    ) -> str:
    """
    Search for listed stocks by company name.

    Args:
        query (str): Query parameter for searching company names. Specify a string contained in the company name.
            Example: Specifying "トヨタ" will search for stocks with "トヨタ" in the company name.
            Must be in Japanese.
        limit (int, optional): Maximum number of results to retrieve. Defaults to 10.
        start_position (int, optional): The starting position for the search. Defaults to 0.

    Returns:
        str: API response text
    """
    url = "https://api.jquants.com/v1/listed/info"
    response = await make_requests(url)
    if "error" in response:
        return json.dumps(response, ensure_ascii=False)

    response_json_list = response.get("info", [])
    response_json_list = [
        r for r in response_json_list
        if (
            query.lower() in r.get("CompanyName", "").lower()
            or
            query.lower() in r.get("CompanyNameEnglish", "").lower()
        )
    ][start_position:start_position + limit]

    response_json = {'info': response_json_list}
    return json.dumps(response_json, ensure_ascii=False)



@mcp_server.tool()
async def get_daily_quotes(
        code : str,
        from_date : str,
        to_date : str,
        limit : int = 10,
        start_position : int = 0,
    ) -> str:
    """
    Retrieve daily stock price data for a specified stock code.
    The available data spans from 2 years prior to today up until 12 weeks ago.

    Args:
        code (str): Specify the stock code. Example: "72030" (トヨタ自動車)
        from_date (str): Specify the start date. Example: "2023-01-01" must be in YYYY-MM-DD format
        to_date (str): Specify the end date. Example: "2023-01-31" must be in YYYY-MM-DD format
        limit (int, optional): Maximum number of results to retrieve. Defaults to 10.
        start_position (int, optional): The starting position for the search. Defaults to 0.

    Returns:
        str: API response text
    """

    url = "https://api.jquants.com/v1/prices/daily_quotes?code={}&from={}&to={}".format(
        code,
        from_date,
        to_date
    )
    response = await make_requests(url)
    if "error" in response:
        return json.dumps(response, ensure_ascii=False)
    response_json_list = response.get("daily_quotes", [])
    response_json_list = response_json_list[start_position:start_position + limit]
    response_json = {'daily_quotes': response_json_list}
    return json.dumps(response_json, ensure_ascii=False)


@mcp_server.tool()
async def get_financial_statements(
        code : str,
        limit : int = 10,
        start_position : int = 0,
    ) -> str:
    """
    Retrieve financial statements for a specified stock code.
    The available data spans from 2 years prior to today up until 12 weeks ago.
    You can obtain quarterly financial summary reports and disclosure information regarding
    revisions to performance and dividend information (mainly numerical data) for listed companies.

    Args:
        code (str): Specify the stock code. Example: "72030" (トヨタ自動車)
        limit (int, optional): Maximum number of results to retrieve. Defaults to 10.
        start_position (int, optional): The starting position for the search. Defaults to 0.
    """
    url = "https://api.jquants.com/v1/fins/statements?code={}".format(code)
    response = await make_requests(url)
    if "error" in response:
        return json.dumps(response, ensure_ascii=False)
    response_json_list = response.get("statements", [])
    response_json_list = [
        {k:v for k,v in r.items() if v != ""}
        for r in response_json_list
    ][start_position:start_position + limit]
    response_json = {'statements': response_json_list}
    return json.dumps(response_json, ensure_ascii=False)


@mcp_server.tool()
async def analyze_with_dify(
        data: str,
        prompt: str = "この金融データを分析してください",
    ) -> str:
    """
    Dify APIを使用してLLMでデータ分析を実行
    
    Args:
        data (str): 分析対象のデータ (JSON文字列)
        prompt (str, optional): LLMへのプロンプト. Defaults to "この金融データを分析してください".
        
    Returns:
        str: LLM分析結果 (JSON文字列)
    """
    try:
        # データをJSONとしてパースしてコンテキスト作成
        json_data = json.loads(data)
        context = f"分析対象データ: {json.dumps(json_data, ensure_ascii=False)}"
        
        # Dify API呼び出し
        result = await make_dify_request(prompt, context)
        if "error" in result:
            return json.dumps(result, ensure_ascii=False)
            
        return json.dumps({
            "analysis": result.get("answer", ""),
            "status": "success"
        }, ensure_ascii=False)
        
    except json.JSONDecodeError:
        return json.dumps({
            "error": "無効なJSONデータです",
            "status": "invalid_json"
        }, ensure_ascii=False)
    except Exception as e:
        return json.dumps({
            "error": f"分析中にエラーが発生しました: {str(e)}",
            "status": "analysis_error"
        }, ensure_ascii=False)

def main() -> None:
    print("Starting J-Quants MCP server!")
    mcp_server.run(transport="stdio")

if __name__ == "__main__":
    main()
