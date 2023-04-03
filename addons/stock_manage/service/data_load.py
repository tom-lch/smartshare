from typing import List
import datetime
from addons.stock_manage.models.stock_manage import StocksBase, StockDaily, StockDailyBasic, StockDailyMoneyFlow
import pandas as pd
import numpy as np
import talib

max_min_scaler = lambda x: (x-np.min(x)) / (np.max(x) - np.min(x))

class Stocks():
    def __init__(self, markets: List[str] = ["科创板", "创业板", "主板", "中小板"]):
        self.markets = markets
        self.raws = self.query_stocks()
    
    def query_stocks(self):
        res = (StocksBase.select(StocksBase.ts_code, StocksBase.name).
               where(StocksBase.market << self.markets))
        return res
    
    def get_stocks(self):
        return [raw.ts_code for raw in self.raws]


# 加载数据模块
class StockDataLoader:
    def __init__(self, stock_code: str, start_date: datetime.date, end_date: datetime.date):
        self.df = None
        self.stock_code = stock_code
        self.start_date = start_date or '2000-01-01'
        self.end_date = end_date or datetime.date.today()
    
    def _load_stock_daily(self):
        res = (StockDaily.select(StockDaily.ts_code, 
                        StockDaily.trade_date, 
                        StockDaily.open,
                        StockDaily.high, 
                        StockDaily.low, 
                        StockDaily.close, 
                        StockDaily.pre_close, 
                        StockDaily.pct_chg,
                        StockDaily.change,
                        StockDaily.vol / 1000000, 
                        StockDaily.amount / 10000000).
               where((StockDaily.ts_code == self.stock_code) & 
                     (StockDaily.trade_date.between(self.start_date, self.end_date))).
                     order_by(StockDaily.trade_date))
        return res
        
    def _load_stock_daily_basic(self):
        res = (StockDailyBasic.select(
                        StockDailyBasic.ts_code,
                        StockDailyBasic.trade_date,
                        StockDailyBasic.turnover_rate,
                        StockDailyBasic.turnover_rate_f,
                        StockDailyBasic.volume_ratio,
                        StockDailyBasic.pe,
                        StockDailyBasic.pe_ttm,
                        StockDailyBasic.pb,
                        StockDailyBasic.ps,
                        # StockDailyBasic.total_share,
                        # StockDailyBasic.float_share,
                        # StockDailyBasic.free_share,
                        # StockDailyBasic.total_mv,
                        # StockDailyBasic.circ_mv
        ).
               where((StockDailyBasic.ts_code == self.stock_code) & 
                     (StockDailyBasic.trade_date.between(self.start_date, self.end_date))).order_by(StockDailyBasic.trade_date))
        return res
    def _load_stock_daily_money_flow(self):
        res = (StockDailyMoneyFlow.select(
                        StockDailyMoneyFlow.ts_code,
                        StockDailyMoneyFlow.trade_date,
                        StockDailyMoneyFlow.buy_sm_vol  / 10000000,
                        StockDailyMoneyFlow.buy_sm_amount  / 10000000,
                        StockDailyMoneyFlow.sell_sm_vol / 10000000,
                        StockDailyMoneyFlow.sell_sm_amount / 10000000,
                        StockDailyMoneyFlow.buy_md_vol / 10000000,
                        StockDailyMoneyFlow.buy_md_amount / 10000000,
                        StockDailyMoneyFlow.sell_md_vol / 10000000,
                        StockDailyMoneyFlow.sell_md_amount / 10000000,
                        StockDailyMoneyFlow.buy_lg_vol / 10000000,
                        StockDailyMoneyFlow.buy_lg_amount / 10000000,
                        StockDailyMoneyFlow.sell_lg_vol / 10000000,
                        StockDailyMoneyFlow.sell_lg_amount / 10000000,
                        StockDailyMoneyFlow.buy_elg_vol / 10000000,
                        StockDailyMoneyFlow.buy_elg_amount / 10000000,
                        StockDailyMoneyFlow.sell_elg_vol / 10000000,
                        StockDailyMoneyFlow.sell_elg_amount / 10000000,
                        StockDailyMoneyFlow.net_mf_vol / 10000000,
                        StockDailyMoneyFlow.net_mf_amount / 10000000,
        ).
               where((StockDailyMoneyFlow.ts_code == self.stock_code) & 
                     (StockDailyMoneyFlow.trade_date.between(self.start_date, self.end_date))).order_by(StockDailyMoneyFlow.trade_date))
        return res
    
    def _merge_data(self, stock_daily, stock_daily_basic, stock_daily_money_flow):
        res_daily = pd.DataFrame(list(stock_daily.dicts()))
        res_basic = pd.DataFrame(list(stock_daily_basic.dicts()))
        res_1 = pd.merge(res_daily, res_basic, on=['ts_code', 'trade_date'])
        res_flow = pd.DataFrame(list(stock_daily_money_flow.dicts()))
        return pd.merge(res_1, res_flow, on=['ts_code', 'trade_date'])
    
    def stock_all_data(self):
        stock_daily = self._load_stock_daily()
        stock_daily_basic = self._load_stock_daily_basic()
        stock_daily_money_flow = self._load_stock_daily_money_flow()
        df = self._merge_data(stock_daily, stock_daily_basic, stock_daily_money_flow)
        return df
    
    def get_stack_daily(self):
        return pd.DataFrame(list(self._load_stock_daily().dicts()))
    
    def get_stock_basic(self):
        return pd.DataFrame(list(self._load_stock_daily_basic().dicts()))

    def get_stock_money_flow(self):
        return pd.DataFrame(list(self._load_stock_daily_money_flow().dicts()))
    
    def get_all_stock_data(self):
        df = self.stock_all_data()
        df = df.set_index('trade_date',drop=False, append=False, inplace=False, verify_integrity=False)
        return df
    
    def add_target(self, df: pd.DataFrame):
        df["Open-Close"] = df["open"] - df["close"]
        df["High-Low"] = df["high"] - df["low"]
        # df["Val_Norm"] = df[["vol"]].apply(max_min_scaler)
        # df["Amount_Norm"] = df[["amount"]].apply(max_min_scaler)
        df["target_cls"] = np.where(df["close"].shift(-1) > df["close"], 1, -1)
        df["target_reg"] = df["close"].shift(-1) - df["close"]
        df.fillna(0, inplace=True)
        return df
    
    def add_ema(self, df):
        # 计算2日EMA
        close_prices = df["close"]
        df["ema_2"] = talib.EMA(close_prices, timeperiod=2)
        df["ema_5"] = talib.EMA(close_prices, timeperiod=5)
        df["ema_30"] = talib.EMA(close_prices, timeperiod=30)
        # 计算21日斜率
        slope21 = talib.LINEARREG_SLOPE(close_prices, timeperiod=21) * 20 + close_prices
        # 计算42日EMA
        df["ema42"] = talib.EMA(slope21, timeperiod=42)
        df["buy_signal"] = np.where(df["ema_2"] >= df["ema42"], 1, 0)
        df["sell_signal"] = np.where(df["ema_2"] < df["ema42"], 1, 0)
        df.fillna(0, inplace=True)
    