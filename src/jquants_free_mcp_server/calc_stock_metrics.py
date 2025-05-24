from datetime import datetime
import jquantsapi
import pandas as pd
from requests import HTTPError
import requests
import json
import numpy as np
from tqdm import tqdm
from pathlib import Path

# 将来のダウンキャスト挙動を明示的に有効化
pd.set_option('future.no_silent_downcasting', True)

# リフレッシュトークンが記載されているファイルを指定します
DATA_PATH = Path("data")
REFRESH_TOKEN_FILE_PATH = "jquantsapi-key.txt"
STOCK_PRICE_FILENAME = DATA_PATH / Path("stock_price.csv")
STOCK_LIST_FILENAME = DATA_PATH / Path("stock_list.csv")
STOCK_FINANCE_FILENAME = DATA_PATH / Path("stock_fin.csv")
TRADE_SPEC_FILENAME = DATA_PATH / Path("markets_trades_spec.csv")
TOPIX_FILENAME = DATA_PATH / Path("topix.csv")
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


def add_ma_dev_rate(df, short=25, middle=75, long=200):
    """ 移動平均および乖離率、パーフェクトオーダーかどうかをメトリクスとして追加する。 """
    df[f"SMA{short}"] = df[CLOSE_COL].rolling(window=short).mean()
    df[f"SMA{middle}"] = df[CLOSE_COL].rolling(window=middle).mean()
    df[f"SMA{long}"] = df[CLOSE_COL].rolling(window=long).mean()

    # 移動平均線乖離率
    df[f"SMA{short}_乖離率"] = (df[CLOSE_COL] - df[f"SMA{short}"]) / df[f"SMA{short}"] * 100
    df[f"SMA{middle}_乖離率"] = (df[CLOSE_COL] - df[f"SMA{middle}"]) / df[f"SMA{middle}"] * 100
    df[f"SMA{long}_乖離率"] = (df[CLOSE_COL] - df[f"SMA{long}"]) / df[f"SMA{long}"] * 100

    df["PerfectOrder"] = np.where((df[f"SMA{short}"] > df[f"SMA{middle}"]) &
                                  (df[f"SMA{middle}"] > df[f"SMA{long}"]), 1, 0)


def add_metrics_kaidan(stock_df, days):  # 230510追加
    # 前日の安値より当日の安値のほうが高い日数を計算する(前日の安値が数値として存在しない場合は前日安値でfillする)
    stock_df[LOW_COL] = stock_df[LOW_COL].ffill()  # 修正: fillna(method="ffill") を ffill() に変更
    stock_df["HigherLowDays"] = (stock_df[LOW_COL] >= stock_df[LOW_COL].shift(1)).cumsum()
    # 指定した日数の出来高の平均値のX倍が発生した時の日付を格納する
    volume_mean = stock_df[VOLUME_COL].rolling(days).mean()
    X = 5  # ここでXの値を設定する
    stock_df["HighVolumeDates"] = stock_df["Date"].where(stock_df["AdjustmentVolume"] >= volume_mean * X)
    stock_df["HighVolumeDates"] = stock_df["HighVolumeDates"].ffill()  # 修正: infer_objectsを削除
    # メトリクスを追加した新しいDataFrameを作る
    new_df = stock_df.copy()
    return new_df


def add_stock_metrics(df):
    """ 全銘柄分に、メトリクスを追加して結合していく。"""
    sc_list = df[RAW_STOCK_CODE].unique()
    concat_df = pd.DataFrame([], columns=df.columns)
    SHORT_DAYS = 5
    for sc in tqdm(sc_list):

        # if int(sc) / 10 > 1500:  # テスト的に一部銘柄コードのデータだけ動かしたいときの処理
        #     break
        df_filter = df.query(f"{RAW_STOCK_CODE} == @sc").copy()
        add_ma_dev_rate(df_filter, short=SHORT_DAYS, middle=25, long=75)  # 5,25,75のパーフェクトオーダを算出する
        DAYS_OF_TARGET = 30
        if len(df_filter):
            pass
        else:
            print(f"データ:{df_filter}は空です, {type(df_filter)}")
            # continue()
        tmp_df = add_metrics_kaidan(df_filter, DAYS_OF_TARGET)  # 階段チャート状態のものを確認ver.1
        tmp_df = SMA_over(tmp_df, SHORT_DAYS)  # 5日移動平均を上回るかどうか

        # 空でないデータフレームのみ結合
        if not tmp_df.empty:
            concat_df = pd.concat([concat_df, tmp_df])

    return concat_df


def SMA_over(df_stock, n):

    # 経過日数を計算
    def get_days(x):
        # xよりも前の日付でis_tanki_SMA_overが1となるものが存在するかどうかをチェック
        if dates_SMA_over[dates_SMA_over <= x].size > 0:
            # 存在する場合は、その最後の日付との差分を返す
            # return (x - dates_SMA_over[dates_SMA_over <= x][-1]).days
            return (x - dates_SMA_over[dates_SMA_over <= x][-1])
        else:
            # 存在しない場合は、Noneを返す
            return None

    # n日の移動平均を計算
    df_stock['tanki_SMA'] = df_stock[CLOSE_COL].rolling(n).mean()

    # closeが移動平均より上回っているかどうかを判定
    df_stock['is_tanki_SMA_over'] = (df_stock[CLOSE_COL] > df_stock['tanki_SMA']).astype(int)

    # is_tanki_SMA_overが1となる日付(index)を取得
    dates_SMA_over = df_stock.index[df_stock['is_tanki_SMA_over'] == 1]

    # dates_SMA_overをint型に変換
    dates_SMA_over = dates_SMA_over.astype('int')
    # # dates_SMA_overをdatetime型に変換
    # dates_SMA_over = pd.to_datetime(dates_SMA_over)

    # 経過日数を計算
    # df_stock['terms_SMA_over'] = df_stock.index.to_series().apply(lambda x: (x - dates_SMA_over[dates_SMA_over <= x][-1]).days)
    df_stock['terms_SMA_over'] = df_stock.index.to_series().apply(get_days)
    return df_stock

if __name__ == '__main__':
    # 指標追加(株価データを読み込んで列追加する)
    df_p = pd.read_csv(STOCK_PRICE_FILENAME, dtype={'user_id': int})

    df_p = add_stock_metrics(df_p)
    df_p.to_csv(DATA_PATH / Path("stock_metrics_result.csv"))

