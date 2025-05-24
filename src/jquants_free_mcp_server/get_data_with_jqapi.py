from datetime import datetime, timedelta
import jquantsapi
import pandas as pd
from requests import HTTPError
import requests
import json
import numpy as np
from tqdm import tqdm
from pathlib import Path
from sqlalchemy import create_engine  # 英語列名版データもそのまま格納するようにする。
import polars as pl
import time_recorder
from dateutil import tz
import os

# リフレッシュトークンが記載されているファイルを指定します
DATA_PATH = Path("data")
REFRESH_TOKEN_FILE_PATH = "jquantsapi-key.txt"
STOCK_PRICE_FILENAME = DATA_PATH / Path("stock_price.csv")
STOCK_LIST_FILENAME = DATA_PATH / Path("stock_list.csv")
STOCK_FINANCE_FILENAME = DATA_PATH / Path("stock_fin.csv")
TRADE_SPEC_FILENAME = DATA_PATH / Path("markets_trades_spec.csv")
TOPIX_FILENAME = DATA_PATH / Path("option.csv")
OP_FILENAME = DATA_PATH / Path("option.csv")
MERGIN_FILENAME = DATA_PATH / Path("margin_interest.csv")
SHORT_FILENAME = DATA_PATH / Path("short_selling.csv")

BUFFER_DATES = 10  # 計算のための余分な日付バッファ(とりあえず10日前後とっておく）
STORAGE_DIR_PATH = "marketdata"
OPEN_COL = "AdjustmentOpen"
CLOSE_COL = "AdjustmentClose"
HIGH_COL = "AdjustmentHigh"
LOW_COL = "AdjustmentLow"
VOLUME_COL = "AdjustmentVolume"

RAW_STOCK_CODE = "Code"  # ベースデータの銘柄コード(ETF共存との関係で末尾に0が入って5桁になっているので後で加工必須)

STR_KAIDAN_DAYS = "HigherLowDays"
STR_HIGHER_VOL_DATE = "HighVolumeDates"

# IDトークンのファイルパスを定義
ID_TOKEN_FILE_PATH = "jquantsapi-id-token.txt"
ID_TOKEN_EXPIRY_FILE_PATH = "jquantsapi-id-token-expiry.txt"


# リフレッシュトークンをファイルから読み込むための関数を定義します
def get_refresh_token_from_file(refresh_token_file_path: str =
                                REFRESH_TOKEN_FILE_PATH):
    """リフレッシュトークン読み込み(ファイルから)"""
    with open(refresh_token_file_path, "r") as f:
        refresh_token = f.read()
    return refresh_token.rstrip().lstrip()


def get_refresh_token(mail_address, password):
    """リフレッシュトークン読み込み(mail, passから)"""
    data = {"mailaddress": mail_address, "password": password}
    r_post = requests.post("https://api.jquants.com/v1/token/auth_user", data=json.dumps(data))
    return r_post.json()


def save_id_token(id_token):
    """IDトークンをファイルに保存"""
    with open(ID_TOKEN_FILE_PATH, mode="w") as f:
        f.write(id_token)
    
    # IDトークンの有効期限（1時間）を保存
    expiry_time = datetime.now() + timedelta(hours=1)
    with open(ID_TOKEN_EXPIRY_FILE_PATH, mode="w") as f:
        f.write(expiry_time.isoformat())


def get_id_token_from_file():
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


if __name__ == '__main__':
	
    # J-Quants API から取得するデータの期間
    # 取得開始日
    
    start_dt: datetime = datetime(2024, 8, 1).replace(hour=9, minute=0, second=0, microsecond=0)
    # 現在の日時を取得
    end_dt: datetime = datetime.now().replace(hour=9, minute=0,
                                              second=0, microsecond=0)
    print(f'start:{start_dt},  end:{end_dt}')

    FILE_LIST = ["stock_list", "stock_price", "stock_fin", "topix", "option",
             "margin_interest", "short_selling"]
    KEY_COL = "変数名"
    VALUE_COL = "説明"
    # USER='nakahara'
    USER='postgres'  # 241104 postgresに変更
    PASSWORD='finllm'
    # HOST='163.44.98.83:3306'
    HOST='163.44.98.83:5432'  # 241104 postgresに変更
    # DATABASE='stock_en'
    DATABASE='postgres'  # 241104 postgresに変更
    FOLDER_PATH = Path("data")

    # engineを指定(241104:postgresに変更)
    # engine = create_engine(f'mysql+pymysql://{USER}:{PASSWORD}@{HOST}/{DATABASE}')
    engine = create_engine(f'postgresql://{USER}:{PASSWORD}@{HOST}/{DATABASE}')

    # リフレッシュトークン
    # refresh_token = ''
    with open("settings.json", "r") as f:  # 設定情報読み込み
        settings = json.load(f)

    refresh_token = get_refresh_token_from_file()  # ファイルに書き込んでたらこっちを読み込み
    cli = jquantsapi.Client(refresh_token=refresh_token)
    
    # IDトークンをファイルから読み込む
    id_token = get_id_token_from_file()
    
    if id_token:
        print("IDトークンをファイルから読み込みました。")
        # IDトークンをクライアントに設定
        cli.id_token = id_token
    else:
        try:
            # IDトークンを取得
            id_token = cli.get_id_token()
            if len(id_token) > 0:
                print("refresh_tokenは正常です。新しいIDトークンを取得しました。")
                # IDトークンをファイルに保存
                save_id_token(id_token)
        except HTTPError:
            print("refresh_tokenを使用できません。登録情報からreftokenを取得し、更新します。")
            ref = get_refresh_token(settings["mailaddress"], settings["password"])
            with open(REFRESH_TOKEN_FILE_PATH, mode="w") as f:  # ファイル書き込み
                f.write(ref["refreshToken"])
            
            # 新しいリフレッシュトークンでクライアントを再初期化
            refresh_token = get_refresh_token_from_file()
            cli = jquantsapi.Client(refresh_token=refresh_token)
            
            # IDトークンを取得して保存
            id_token = cli.get_id_token()
            save_id_token(id_token)
    
    # フリープラン
    print(f"start:{str(start_dt)[0:10]}, end:{str(end_dt)[0:10]}")
    @time_recorder.time_recorder
    # 銘柄一覧(listed_info)
    def get_stock_list():
        filename = "stock_list"
        stock_list_load: pd.DataFrame = cli.get_list()
        stock_list_load.to_csv(STOCK_LIST_FILENAME, index=False)
        stock_list_load.to_sql(filename, con=engine, if_exists="replace", index=False, method="multi")  # polarsのデータフレームをpandasのデータフレームに変換してto_sqlメソッドを使用
        # stock_list_load = pl.DataFrame(cli.get_list()) # データの取得
        # stock_list_load = pl.from_pandas(cli.get_list()) # データの取得

        # stock_list_load.write_csv(STOCK_LIST_FILENAME) # CSVファイルに保存
        # stock_list_load.to_pandas().to_sql(filename, con=engine, if_exists="replace", index=False, method="multi")
        # stock_list_load.write_database(table_name=filename, connection=engine, if_table_exists="replace")
        print(f'stock list end,  length:{len(stock_list_load)}')

    print("stock_list start")
    get_stock_list()

    @time_recorder.time_recorder
    # 株価情報(daily_quote)
    def get_daily_quote():
        filename = "stock_price"
        stock_price_load: pd.DataFrame = cli.get_price_range(start_dt, end_dt)
        stock_price_load.to_csv(STOCK_PRICE_FILENAME, index=False)
        stock_price_load.to_sql(filename, con=engine, if_exists="replace", index=False, method="multi")  # polarsのデータフレームをpandasのデータフレームに変換してto_sqlメソッドを使用
        # print(f'quote 1_1')
        # # stock_price_load = pl.DataFrame(cli.get_price_range(start_dt, end_dt)) # データの取得
        # df = cli.get_price_range(start_dt, end_dt)
        # print(f'quote 1_2')
        # df_cleaned = df.dropna(axis=1, how='all')
        # print(f'quote 1_3')
        # stock_price_load = pl.from_pandas(df_cleaned) # データの取得
        # print(f'quote 2')
        # stock_price_load.write_csv(STOCK_PRICE_FILENAME) # CSVファイルに保存
        # print(f'quote 3')
        # stock_price_load.to_pandas().to_sql(filename, con=engine, if_exists="replace", index=False, method="multi")
        # print(f'quote 4')
        # stock_price_load.write_database(table_name=filename, connection=engine, if_table_exists="replace")
        print(f'stock price end,  length:{len(stock_price_load)}')

    print("stock_quote start")
    get_daily_quote()

    @time_recorder.time_recorder
    # 財務情報(statements)
    def get_statements():
        stock_fin_load: pd.DataFrame = cli.get_statements_range(start_dt, end_dt)
        stock_fin_load.to_csv(STOCK_FINANCE_FILENAME, index=False)
        filename = "stock_fin"
        stock_fin_load.to_sql(filename, con=engine, if_exists="replace", index=False, method="multi")  # polarsのデータフレームをpandasのデータフレームに変換してto_sqlメソッドを使用
        # print(f'fin 1')
        # # stock_fin_load = pl.DataFrame(cli.get_statements_range(start_dt, end_dt))
        # df = cli.get_statements_range(start_dt, end_dt)
        # df_cleaned = df.dropna(axis=1, how='all')
        # stock_fin_load = pl.from_pandas(df_cleaned)
        # # 'NumberOfIssuedAndOutstandingSharesAtTheEndOfFiscalYearIncludingTreasuryStock'列を除外(64byte超)
        # print(f'fin 2')
        # stock_fin_load = stock_fin_load.drop('NumberOfIssuedAndOutstandingSharesAtTheEndOfFiscalYearIncludingTreasuryStock')
        # print(f'fin 3')
        # stock_fin_load.write_csv(STOCK_FINANCE_FILENAME) # CSVファイルに保存
        # # 今は64byte超過のものがあるからいったんDB保存はコメント
        # print(f'fin 4')
        # stock_fin_load.write_database(table_name=filename, connection=engine, if_table_exists="replace")
        print(f'stock fin end,  length:{len(stock_fin_load)}')

    print("stock_fin start")
    get_statements()

    # ライトプラン
    @time_recorder.time_recorder
    # 投資部門別情報(trades_spec)
    def get_trades_spec():
        section_str: str = "TSEPrime"  # sectionを指定しないとデータが取れない模様
        # markets_trades_spec_load: pd.DataFrame = cli.get_markets_trades_spec(section=section_str, from_yyyymmdd=str(start_dt)[0:10], to_yyyymmdd=str(end_dt)[0:10])
        # markets_trades_spec_load.to_csv(TRADE_SPEC_FILENAME, index=False)
        filename = "markets_trades_spec"
        # markets_trades_spec_load.to_sql(filename, con=engine, if_exists="replace", index=False, method="multi")  # polarsのデータフレームをpandasのデータフレームに変換してto_sqlメソッドを使用
        markets_trades_spec_load=pl.DataFrame(cli.get_markets_trades_spec(section=section_str, from_yyyymmdd=str(start_dt)[0:10], to_yyyymmdd=str(end_dt)[0:10]))
        markets_trades_spec_load.write_csv(TRADE_SPEC_FILENAME) # CSVファイルに保存
        markets_trades_spec_load.write_database(table_name=filename, connection=engine, if_table_exists="replace")
        print(f'markets trades spec end,  length:{len(markets_trades_spec_load)}')
        # print(end_dt)

    print("stock_trades_spec start")
    get_trades_spec()

    # Topix(indices)
    @time_recorder.time_recorder
    def get_indices():
        filename = "topix"
        # stock_topix_load: pd.DataFrame = cli.get_indices_topix(str(start_dt)[0:10], str(end_dt)[0:10])
        # stock_topix_load.to_csv(TOPIX_FILENAME, index=False)
        # stock_topix_load.to_sql(filename, con=engine, if_exists="replace", index=False, method="multi")  # polarsのデータフレームをpandasのデータフレームに変換してto_sqlメソッドを使用
        stock_topix_load = pl.DataFrame(cli.get_indices_topix(str(start_dt)[0:10], str(end_dt)[0:10]))
        stock_topix_load.write_csv(TOPIX_FILENAME) # CSVファイルに保存
        stock_topix_load.write_database(table_name=filename, connection=engine, if_table_exists="replace")
        print(f'topix end,  length:{len(stock_topix_load)}')

    print("stock_topix start")
    get_indices()

    # スタンダードプラン
    @time_recorder.time_recorder
    def get_option():
        # オプション四本値(option)
        filename = "option"
        option_load: pd.DataFrame = cli.get_index_option_range(str(start_dt)[0:10], str(end_dt)[0:10])
        option_load.to_csv(OP_FILENAME, index=False)
        option_load.to_sql(filename, con=engine, if_exists="replace", index=False, method="multi")  # polarsのデータフレームをpandasのデータフレームに変換してto_sqlメソッドを使用
        # print(f'op 1')
        # # option_load=pl.DataFrame(cli.get_index_option_range(str(start_dt)[0:10], str(end_dt)[0:10]))
        # option_load=pl.DataFrame(cli.get_index_option_range(start_dt, end_dt))
        # print(f'op 2')
        # option_load.write_csv(OP_FILENAME) # CSVファイルに保存
        # print(f'op 3')
        # option_load.write_database(table_name=filename, connection=engine, if_table_exists="replace")
        print(f'option end,  length:{len(option_load)}')

    print("stock_op start")
    get_option()

    @time_recorder.time_recorder
    # 信用取引週末残高(mergin_interest)
    def get_mergin_interest():
        filename = "margin_interest"
        # markets_weekly_margin_interest_load: pd.DataFrame = cli.get_weekly_margin_range(str(start_dt)[0:10], str(end_dt)[0:10])
        # markets_weekly_margin_interest_load.to_csv(MERGIN_FILENAME, index=False)
        # markets_weekly_margin_interest_load.to_sql(filename, con=engine, if_exists="replace", index=False, method="multi")  # polarsのデータフレームをpandasのデータフレームに変換してto_sqlメソッドを使用
        print(f'mi 1')
        markets_weekly_margin_interest_load=pl.DataFrame(cli.get_weekly_margin_range(start_dt, end_dt))
        print(f'mi 2')
        markets_weekly_margin_interest_load.write_csv(MERGIN_FILENAME) # CSVファイルに保存
        print(f'mi 3')
        markets_weekly_margin_interest_load.write_database(table_name=filename, connection=engine, if_table_exists="replace")
        print(f'margin interest end,  length:{len(markets_weekly_margin_interest_load)}')

    print("stock_margin_interest start")
    get_mergin_interest()

    @time_recorder.time_recorder
    # 業種別空売り比率(short_selling)
    def get_short_selling():
        filename = "short_selling"
        # markets_short_selling_load: pd.DataFrame = cli.get_short_selling_range(str(start_dt)[0:10], str(end_dt)[0:10])
        # markets_short_selling_load.to_csv(SHORT_FILENAME, index=False)
        # markets_short_selling_load.to_sql(filename, con=engine, if_exists="replace", index=False, method="multi")  # polarsのデータフレームをpandasのデータフレームに変換してto_sqlメソッドを使用
        print(f'ss 1')
        markets_short_selling_load=pl.DataFrame(cli.get_short_selling_range(start_dt, end_dt))
        print(f'ss 2')
        markets_short_selling_load.write_csv(SHORT_FILENAME) # CSVファイルに保存
        print(f'ss 3')
        markets_short_selling_load.write_database(table_name=filename, connection=engine, if_table_exists="replace")
        print(f'short selling end,  length:{len(markets_short_selling_load)}')

    print("stock_short_selling start")
    get_short_selling()
