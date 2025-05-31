from mcp_server.custom_metrics.base import CustomMetricBase
import pandas as pd

class SectorMomentumSignal(CustomMetricBase):
    """
    業種別モメンタム＋指数連動戦略シグナル
    - 各業種（Sector33Code）ごとに直近1ヶ月（20営業日）リターンを計算
    - 上位3業種に属する銘柄に+1（買いシグナル）、それ以外は0
    """

    def calculate(self, df: pd.DataFrame) -> pd.Series:
        # 必要なカラム: 'Sector33Code', 'Close', 'Date'
        for col in ['Sector33Code', 'Close', 'Date']:
            if col not in df.columns:
                raise ValueError(f"{col}列が必要です")

        # 日付昇順で並んでいることを前提
        df = df.copy()
        df['Date'] = pd.to_datetime(df['Date'])

        # 直近1ヶ月（20営業日）リターンを業種ごとに計算
        # 最新日付を取得
        latest_date = df['Date'].max()
        # 1ヶ月前の日付
        one_month_ago = df['Date'].sort_values().unique()[-21]  # 0-indexed

        # 最新日と1ヶ月前のデータを抽出
        latest_df = df[df['Date'] == latest_date]
        past_df = df[df['Date'] == one_month_ago]

        # 業種ごとに平均リターンを計算
        merged = pd.merge(
            latest_df[['Sector33Code', 'Close']],
            past_df[['Sector33Code', 'Close']],
            on='Sector33Code',
            suffixes=('_latest', '_past')
        )
        merged['sector_return'] = (merged['Close_latest'] - merged['Close_past']) / merged['Close_past']

        # 上位3業種を抽出
        top_sectors = merged.sort_values('sector_return', ascending=False)['Sector33Code'].head(3).tolist()

        # 各銘柄が属する業種が上位3業種なら+1
        signal = df[df['Date'] == latest_date]['Sector33Code'].apply(lambda x: 1 if x in top_sectors else 0)
        # インデックスを元に戻す
        signal.index = df[df['Date'] == latest_date].index

        # 全期間で出力する場合は、最新日以外は0
        full_signal = pd.Series(0, index=df.index)
        full_signal.loc[signal.index] = signal

        return full_signal

sector_momentum_signal = SectorMomentumSignal()
