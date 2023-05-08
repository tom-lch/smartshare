from addons.stock_manage.service import StockDataLoader, Stocks, Long
from addons.stock_manage.models.stock_manage import StockLong
import datetime

if __name__ == '__main__':
    stocks = Stocks().get_stock_name_codes()
    today = datetime.date.today()
    day = today + datetime.timedelta(days=1)
    c = StockLong()
    for stock in stocks:
        print("开始处理", stock[0], stock[1])
        d = StockDataLoader(stock[1], '2018-01-01', f'{day}')
        res_data = d.get_all_stock_data()
        i = 1
        if len(res_data) < i:
            break
        info = res_data.iloc[-i, :]
        if i == 1:
            data = res_data
        else:
            data = res_data.iloc[:-i+1, :]
        lastDay = f"{info['trade_date']}"
        if f"{lastDay}" != f"2023-05-08":
            continue
        longl = Long(stock[0], stock[1], data)
        res = longl.get_long()
        if not res:
            print(stock[0], stock[1], "不是龙头股")
            continue
        if res[0] == 0:
            res_sql = c.insert({
                "ts_code": res[2],
                "name": res[1],
                "trade_date": lastDay,
                "long_type": 100,
            }).execute()
            print("强强龙", res_sql)
        if res[0] == 1:
            res_sql = c.insert({
                "ts_code": res[2],
                "name": res[1],
                "trade_date": lastDay,
                "long_type": 10,
            }).execute()
            print("强龙", res_sql)
        if res[0] == 2:
            res_sql = c.insert({
                "ts_code": res[2],
                "name": res[1],
                "trade_date": lastDay,
                "long_type": 1,
            }).execute()
            print("潜龙", res_sql)