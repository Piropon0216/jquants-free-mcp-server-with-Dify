from mcp_server.custom_metrics.base import CustomMetricBase
import pandas as pd

class ForeignersFlowSignal(CustomMetricBase):
    """
    投資主体別売買動向フォロー戦略（海外投資家）
    - 直近週の海外投資家差引（ForeignersBalance）が過去4週平均の2倍以上: +1（買いシグナル）
    - 直近週の海外投資家差引が過去4週平均の-2倍以下: -1（売りシグナル）
    - それ以外: 0（ノーシグナル）
    """

    def calculate(self, df: pd.DataFrame) -> pd.Series:
        # 必要なカラム: 'ForeignersBalance'（海外投資家差引）, 'Date'
        if 'ForeignersBalance' not in df.columns:
            raise ValueError("ForeignersBalance列が必要です")
        if 'Date' not in df.columns:
            raise ValueError("Date列が必要です")

        # ForeignersBalanceが週次であることを前提
        foreigners = df['ForeignersBalance']

        # 直近4週平均（直近週を除く）
        rolling_mean = foreigners.shift(1).rolling(window=4, min_periods=1).mean()

        # シグナル判定
        signal = pd.Series(0, index=df.index)
        # 買いシグナル
        signal[foreigners >= 2 * rolling_mean] = 1
        # 売りシグナル
        signal[foreigners <= 2 * rolling_mean * -1] = -1

        return signal

foreigners_flow_signal = ForeignersFlowSignal()
