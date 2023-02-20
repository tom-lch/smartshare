from addons.stock_manage.models.stock_manage import Stock_Daily
def main():
    print("hello world!")
    res = Stock_Daily.select().limit(1).first()
    print(res.close)

if __name__ == "__main__":
    main()
