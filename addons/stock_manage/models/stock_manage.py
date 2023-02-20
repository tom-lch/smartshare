import datetime

from peewee import *
from base.base_module import BaseModel


class Stocks_Base(BaseModel):
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


class Stock_Daily(BaseModel):

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


