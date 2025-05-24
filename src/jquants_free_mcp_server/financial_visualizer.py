import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from server import search_company, get_financial_statements
import json
from dotenv import load_dotenv

# 環境変数の読み込み
load_dotenv('.env')

def visualize_equity_ratio(company_name, ratio):
    fig, ax = plt.subplots()
    ax.bar(company_name, ratio, color=['#1f77b4'])
    ax.set_ylabel('自己資本比率 (%)')
    ax.set_title(f'{company_name} 自己資本比率')
    ax.set_ylim(0, 100)
    st.pyplot(fig)

async def main():
    st.title('企業財務分析ダッシュボード')
    
    # 企業名入力
    company1 = st.text_input('比較企業1 (例: コメダ)', 'コメダ')
    company2 = st.text_input('比較企業2 (例: ルノアール)', 'ルノアール')
    
    if st.button('分析実行'):
        with st.spinner('データ取得中...'):
            # 企業1のデータ取得
            ratio1 = await calculate_equity_ratio(company1)
            # 企業2のデータ取得
            ratio2 = await calculate_equity_ratio(company2)
            
            if ratio1 and ratio2:
                # 結果表示
                st.success('分析完了')
                
                # 数値表示
                col1, col2 = st.columns(2)
                with col1:
                    st.metric(f"{company1} 自己資本比率", f"{ratio1:.2f}%")
                with col2:
                    st.metric(f"{company2} 自己資本比率", f"{ratio2:.2f}%")
                
                # 可視化
                visualize_equity_ratio([company1, company2], [ratio1, ratio2])
                
                # 詳細データ表示
                st.subheader('分析結果詳細')
                st.json({
                    company1: f"{ratio1:.2f}%",
                    company2: f"{ratio2:.2f}%"
                })
            else:
                st.error('データ取得に失敗しました')

async def calculate_equity_ratio(company_name):
    # 企業コード取得
    company_info_str = await search_company(company_name)
    company_info = json.loads(company_info_str)
    
    if not company_info.get("info"):
        return None
    
    # 財務諸表データ取得
    financials_str = await get_financial_statements(company_info["info"][0]["Code"])
    financials = json.loads(financials_str)
    
    if not financials.get("statements"):
        return None
    
    # 最新期のデータを取得
    statements = sorted(
        financials["statements"],
        key=lambda x: x["DisclosedDate"],
        reverse=True
    )
    latest = statements[0]
    
    equity = float(latest.get("Equity", latest.get("NetAssets", 0)))
    total_assets = float(latest.get("TotalAssets", 1))
    
    if total_assets == 0:
        return None
    
    return (equity / total_assets) * 100

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
