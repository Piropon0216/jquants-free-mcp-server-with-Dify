import asyncio
import os
from dotenv import load_dotenv
from server import search_company, get_financial_statements

import json

async def calculate_equity_ratio(company_name):
    print(f"\n{company_name}のデータ取得を開始します...")
    
    # 企業コード取得
    print(f"{company_name}の企業コードを検索中...")
    company_info_str = await search_company(company_name)
    print(f"企業コード検索結果: {company_info_str}")
    company_info = json.loads(company_info_str)
    
    if "error" in company_info:
        print(f"エラー: {company_info['error']}")
        return None
    if not company_info.get("info"):
        print(f"{company_name}の企業情報が見つかりませんでした")
        return None
    
    company_code = company_info["info"][0]["Code"]
    print(f"{company_name}の企業コード: {company_code}")
    
    # 財務諸表データ取得
    print(f"{company_name}の財務諸表データを取得中...")
    financials_str = await get_financial_statements(company_code)
    print(f"財務諸表データ取得結果: {financials_str[:200]}...")  # 最初の200文字のみ表示
    financials = json.loads(financials_str)
    
    if "error" in financials:
        print(f"エラー: {financials['error']}")
        return None
    if not financials.get("statements"):
        print(f"{company_name}の財務データが見つかりませんでした")
        return None
    
    # 最新期のデータを取得 (日付が新しい順にソート)
    statements = sorted(
        financials["statements"],
        key=lambda x: x["DisclosedDate"],
        reverse=True
    )
    latest = statements[0]
    print(f"最新財務データ: {latest}")
    
    # 適切なキー名でデータ取得
    equity = float(latest.get("Equity", latest.get("NetAssets", 0)))
    total_assets = float(latest.get("TotalAssets", 1))
    print(f"自己資本: {equity}, 総資産: {total_assets}")
    
    if total_assets == 0:
        print(f"{company_name}の総資産が0のため計算できません")
        return None
    
    equity_ratio = (equity / total_assets) * 100
    print(f"計算された自己資本比率: {equity_ratio:.2f}%")
    return equity_ratio

async def main():
    # 環境変数の読み込み
    load_dotenv('.env')
    
    # 結果をファイルに出力
    with open("equity_ratio_results.txt", "w", encoding="utf-8") as f:
        # コメダの自己資本比率計算
        komeda_ratio = await calculate_equity_ratio("コメダ")
        if komeda_ratio:
            result = f"コメダの自己資本比率: {komeda_ratio:.2f}%\n"
            print(result)
            f.write(result)
        
        # ルノアールの自己資本比率計算
        renoir_ratio = await calculate_equity_ratio("ルノアール")
        if renoir_ratio:
            result = f"ルノアールの自己資本比率: {renoir_ratio:.2f}%\n"
            print(result)
            f.write(result)

if __name__ == "__main__":
    asyncio.run(main())
