from mcp_server.custom_metrics.base import CustomMetricBase
import pandas as pd

class ShortSqueezeSignal(CustomMetricBase):
    """
    空売り残高急増アノマリー戦略シグナル
    - 空売り残高割合（ShortPositionsToSharesOutstandingRatio）が前週比+50%以上増加: +1（買いシグナル）
    - それ以外: 0（ノーシグナル）
    """

    def calculate(self, df: pd.DataFrame) -> pd.Series:
        # 必要なカラム: 'ShortPositionsToSharesOutstandingRatio'
        if 'ShortPositionsToSharesOutstandingRatio' not in df.columns:
            raise ValueError("ShortPositionsToSharesOutstandingRatio列が必要です")

        ratio = pd.to_numeric(df['ShortPositionsToSharesOutstandingRatio'], errors='coerce')
        pct_change = ratio.pct_change()

        # シグナル判定
        signal = pd.Series(0, index=df.index)
        signal[pct_change >= 0.5] = 1  # 50%以上増加で買いシグナル

        return signal

short_squeeze_signal = ShortSqueezeSignal()
