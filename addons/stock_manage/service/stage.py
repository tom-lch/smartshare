import pandas as pd
class Long():
    def __init__(self, stock_name: str, ts_code: str, df :pd.DataFrame):
        self.stock_name = stock_name
        self.ts_code = ts_code
        self.df = df

    def addonTags(self):
        # 龙头强势股
        # 1. 价格上涨 一天 >= 5% ||  2天>=10% || 3天>=15% || 5天>= 20%
        # 2. 成交量上涨 
        # 3. 成交额上涨
        # 4. 换手率上涨 
        # 5. 超级资金大幅度流入
        df = self.df
        df['pct_chg2'] =  (df["close"] - df["close"].shift(2)) / df["close"].shift(2) * 100
        df['pct_chg3'] =  (df["close"] - df["close"].shift(3)) / df["close"].shift(3) * 100
        df['pct_chg5'] =  (df["close"] - df["close"].shift(5)) / df["close"].shift(5) * 100
        df['ellg_amount_rate'] = (df['buy_elg_amount'] +  df['buy_lg_amount']) / (df['sell_lg_amount'] + df['sell_elg_amount'])
        df['elg_amount_rate'] = df['buy_elg_amount'] / df['sell_elg_amount']
        df['lg_amount_rate'] = df['buy_lg_amount'] / df['sell_lg_amount']
        df['md_amount_rate'] = df['buy_md_amount'] / df['sell_md_amount']
        df['sm_amount_rate'] = df['buy_sm_amount'] / df['sell_sm_amount']


    def get_long(self):
        self.addonTags()
        data = self.df[['pct_chg', 'pct_chg2', 'pct_chg3', 'pct_chg5','ellg_amount_rate', 'elg_amount_rate', 'buy_elg_amount', 'lg_amount_rate', 'buy_lg_amount', 'md_amount_rate', 'buy_md_amount', 'sm_amount_rate', 'buy_sm_amount']]
        lastLine = data.iloc[-1,:]
        if lastLine['pct_chg'] <= 5 and lastLine['pct_chg'] > -1:
            if lastLine['elg_amount_rate'] >= 1.8 and lastLine["buy_elg_amount"] > 10000:
                # 潜龙
                return (2, self.stock_name, self.ts_code)
            if lastLine['elg_amount_rate'] < 1.8 and lastLine['elg_amount_rate'] >= 1.2 or (lastLine['elg_amount_rate'] >= 1.8 and lastLine["buy_elg_amount"] < 10000):
                # 草莽
                return (3, self.stock_name, self.ts_code)
        if (lastLine['pct_chg'] > 5 or lastLine['pct_chg2'] > 10 or lastLine['pct_chg3'] > 15 or lastLine['pct_chg5'] > 20) :
            if lastLine["buy_elg_amount"] > 10000:
                if lastLine['elg_amount_rate'] >= 1.55:
                    # 强强龙
                    return (0, self.stock_name, self.ts_code)
                elif lastLine['elg_amount_rate'] < 1.55 and lastLine['elg_amount_rate'] >= 1.2:
                    return (1, self.stock_name, self.ts_code)
            else :
                return (4, self.stock_name, self.ts_code)
