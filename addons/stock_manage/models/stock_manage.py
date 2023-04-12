import datetime

from peewee import *
from base.base_module import BaseModel


class StocksBase(BaseModel):
    class Meta:
        table_name = "stocks_base"
    id = AutoField()
    gen_time = DateTimeField(default=datetime.datetime.now())
    status = IntegerField(default=0)
    delete_status = BooleanField()
    version = IntegerField(default=1)
    name = CharField()
    ts_code = CharField()
    list_status = CharField()
    market = CharField()
    symbol = CharField()
    area = CharField()
    industry = CharField()
    fullname = CharField()
    list_date = CharField()
    delist_date = CharField()


class StockDaily(BaseModel):
    class Meta:
        table_name = "stock_daily"
    id = AutoField()
    gen_time = DateTimeField(default=datetime.datetime.now())
    status = IntegerField(default=0)
    delete_status = BooleanField()
    version = IntegerField(default=1)
    stocks_base_id = IntegerField()
    ts_code = CharField()
    trade_date = DateField()
    open = FloatField()
    high = FloatField()
    low = FloatField()
    close = FloatField()
    pre_close = FloatField()
    change = FloatField()
    pct_chg = FloatField()
    vol = FloatField()
    amount = FloatField()

class StockDailyBasic(BaseModel):
    class Meta:
        table_name = "stock_daily_basic"
    id = AutoField()
    gen_time = DateTimeField(default=datetime.datetime.now())
    status = IntegerField(default=0)
    delete_status = BooleanField()
    version = IntegerField(default=1)
    ts_code = CharField()
    trade_date = DateField()
    turnover_rate = FloatField()
    turnover_rate_f = FloatField()
    volume_ratio = FloatField()
    pe = FloatField()
    pe_ttm = FloatField()
    pb = FloatField()
    ps = FloatField()
    ps_ttm = FloatField()
    dv_ratio = FloatField()
    dv_ttm = FloatField()
    total_share = FloatField()
    float_share = FloatField()
    free_share = FloatField()
    total_mv = FloatField()
    circ_mv = FloatField()


class StockDailyMoneyFlow(BaseModel):
    class Meta:
        table_name = "stock_daily_money_flow"
    id = AutoField()
    gen_time = DateTimeField(default=datetime.datetime.now())
    status = IntegerField(default=0)
    delete_status = BooleanField()
    version = IntegerField(default=1)
    ts_code = CharField()
    trade_date = DateField()
    buy_sm_vol = IntegerField(default=0)
    buy_sm_amount = FloatField(default=0.000000)
    sell_sm_vol = IntegerField(default=0)
    sell_sm_amount = FloatField(default=0.000000)
    buy_md_vol = IntegerField(default=0)
    buy_md_amount = FloatField(default=0.000000)
    sell_md_vol = IntegerField(default=0)
    sell_md_amount = FloatField(default=0.000000)
    buy_lg_vol = IntegerField(default=0)
    buy_lg_amount = FloatField(default=0.000000)
    sell_lg_vol = IntegerField(default=0)
    sell_lg_amount = FloatField(default=0.000000)
    buy_elg_vol = IntegerField(default=0)
    buy_elg_amount = FloatField(default=0.000000)
    sell_elg_vol = IntegerField(default=0)
    sell_elg_amount = FloatField(default=0.000000)
    net_mf_vol = IntegerField(default=0)
    net_mf_amount = FloatField(default=0.000000)


class StockDailyBS(BaseModel):
    class Meta:
        table_name = "stock_daily_bs"
    id = AutoField()
    ts_code = CharField()
    trade_date = DateField()
    buy_signal = IntegerField(default=0)
    sell_signal = IntegerField(default=0)


class StockDailyLeaveBS(BaseModel):
    class Meta:
        table_name = "stock_daily_leave_bs"
    id = AutoField()
    ts_code = CharField()
    trade_date = DateField()
    buy_signal = IntegerField(default=0)
    sell_signal = IntegerField(default=0)
    dynamic_line = FloatField(default=0.000000, help_text="0.2底部 0.5买入 3.2阶段卖出 3.5清仓卖出")

class StockDailyBuyPoint(BaseModel):
    class Meta:
        table_name = "stock_daily_buy_point"
    id = AutoField()
    ts_code = CharField()
    trade_date = DateField()
    buy_signal = IntegerField(default=0)
    build_area_signal = IntegerField(default=0)