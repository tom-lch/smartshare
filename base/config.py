# -* - coding: UTF-8 -* -
import os
from configparser import ConfigParser
import datetime


def datetime_trans(daily_time):
    return datetime.datetime.strptime(daily_time, "%H:%M")


def get_or_error(conf, section, option):
    value = conf.get(section, option, fallback=None)
    if not value:
        print(f"config value not present in file server.conf with section:{section} and option:{option}, "
              f"please refer to server.conf.example for configuration guide")
        exit(1)
    return value


class UsmsConfig:
    ###########################################################################
    # 局部变量定义
    # 以下局部变量的值来自配置文件server.conf的配置
    app_name = ''
    # 数据库默认配置信息
    db_addr, db_database, db_username, db_password, db_port = "localhost", "server_dev", "erik", "asd123asd", 5432
    # 服务器默认配置信息
    server_addr, server_port = "", 0

    # ocr
    ocr_url = ''

    # 读取配置文件
    conf_exit = False
    conf_name = "config.conf"

    # search the config file
    max_path: int = 100
    current_path: int = 0
    while not conf_exit and current_path < max_path:
        if os.path.exists(conf_name):
            conf_name = conf_name
            conf_exit = True
            break
        conf_name = "".join(["../", conf_name])
        current_path += 1

    if conf_exit:
        # print('-----目录指向准确')
        try:
            conf = ConfigParser()
            conf.read(conf_name, encoding="utf-8")
            # 读取数据库配置
            db_addr = conf.get("server_database", "db_addr")
            db_port = conf.getint("server_database", "db_port")
            db_database = conf.get("server_database", "db_database")
            db_username = conf.get("server_database", "db_username")
            db_password = conf.get("server_database", "db_password")

        except Exception as e:
            print(e)

    @classmethod
    def update(cls, section, key, value):
        conf = ConfigParser()
        conf.read(cls.conf_name)
        print("update cfg file:", cls.conf_name)
        conf.set(section, key, value)
        conf.write(open(cls.conf_name, 'w'))


cfg = UsmsConfig()
