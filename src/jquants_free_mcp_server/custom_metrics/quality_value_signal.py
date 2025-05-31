from mcp_server.custom_metrics.base import CustomMetricBase
import pandas as pd

class QualityValueSignal(CustomMetricBase):
    """
    財務健全性＋バリュー株投資シグナル
    - 自己資本比率30%以上
    - 営業利益率10%以上
    - PER15倍以下
    - PBR1.2倍以下
    をすべて満たす場合: +1（買いシグナル）、それ以外: 0
    """

    def calculate(self, df: pd.DataFrame) -> pd.Series:
        # 必要なカラム: 'EquityToAssetRatio', 'OperatingProfit', 'NetSales', 'EarningsPerShare', 'BookValuePerShare', 'Close'
        required_cols = [
            'EquityToAssetRatio', 'OperatingProfit', 'NetSales',
            'EarningsPerShare', 'BookValuePerShare', 'Close'
        ]
        for col in required_cols:
            if col not in df.columns:
                raise ValueError(f"{col}列が必要です")

        # 営業利益率
        op_margin = pd.to_numeric(df['OperatingProfit'], errors='coerce') / pd.to_numeric(df['NetSales'], errors='coerce')
        # PER
        per = pd.to_numeric(df['Close'], errors='coerce') / pd.to_numeric(df['EarningsPerShare'], errors='coerce')
        # PBR
        pbr = pd.to_numeric(df['Close'], errors='coerce') / pd.to_numeric(df['BookValuePerShare'], errors='coerce')
        # 自己資本比率
        equity_ratio = pd.to_numeric(df['EquityToAssetRatio'], errors='coerce')

        # シグナル判定
        signal = (
            (equity_ratio >= 30) &
            (op_margin >= 0.10) &
            (per <= 15) &
            (pbr <= 1.2)
        ).astype(int)

        return signal

quality_value_signal = QualityValueSignal()
