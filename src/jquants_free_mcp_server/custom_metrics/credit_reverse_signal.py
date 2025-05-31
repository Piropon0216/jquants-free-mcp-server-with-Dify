from jquants_free_mcp_server.custom_metrics.base import CustomMetricBase
import pandas as pd

class CreditReverseSignal(CustomMetricBase):
    """
    信用残高逆張りシグナル
    - 信用買い残高が前週比+20%以上増加: -1（売りシグナル）
    - 信用買い残高が前週比-20%以上減少: +1（買いシグナル）
    - それ以外: 0（ノーシグナル）
    """

    def calculate(self, df: pd.DataFrame) -> pd.Series:
        # 必要なカラム: 'LongMarginTradeVolume'（買い残高）, 'Date'
        # DataFrameは日付昇順で渡される想定
        if 'LongMarginTradeVolume' not in df.columns:
            raise ValueError("LongMarginTradeVolume列が必要です")
        if 'Date' not in df.columns:
            raise ValueError("Date列が必要です")

        # 週次データであることを前提
        # 前週比変化率を計算
        buy_margin = df['LongMarginTradeVolume']
        pct_change = buy_margin.pct_change()  # 前週比

        # シグナル判定
        signal = pd.Series(0, index=df.index)
        signal[pct_change >= 0.2] = -1  # 買い残急増→売り
        signal[pct_change <= -0.2] = 1  # 買い残急減→買い

        # シグナルを返す
        return signal

# 必須: クラス名と同じ変数名でインスタンスをエクスポート
credit_reverse_signal = CreditReverseSignal()
