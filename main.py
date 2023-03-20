from addons.stock_manage.models.stock_manage import StockDaily
def main():
    print("hello world!")
    res = StockDaily.select().limit(1).first()
    print(res.close)

if __name__ == "__main__":
    main()
