import cx_Oracle
import oracledb
import os
import datetime as dt
import pandas as pd
import numpy as np


class Para:
    DB_wind = {
        "userName": "cjsxk",
        "password": "cjsxk",
        "hostIP": "172.16.125.222:1521",
        "dbName": "pra",
        "tablePrefix": "wind",
    }  # wind数据

    DB_cjfinres = {
        "userName": "cjfinres",
        "password": "cjfinres",
        "hostIP": "172.16.125.222:1521",
        "dbName": "pra",
        "tablePrefix": "CJFINRES",
    }  # pra数据

    DB_dfcf = {
        "userName": "cjcsyjl",
        "password": "cjcsyjl",
        "hostIP": "172.16.121.198:1521",  # '172.16.50.232:1521',
        "dbName": "dfcfCS",
    }  # 东财数据库

    DB_jajx = {
        "userName": "cjfinres",
        "password": "cjfinres",
        "hostIP": "172.16.125.222:1521",
        "dbName": "pra",
        "tablePrefix": "jajxfund",
    }  # pra数据

    DB_zntz = {
        "userName": "cjsxk",
        "password": "cjsxk",
        "hostIP": "172.16.125.249:1521",
        "dbName": "zntz",
        "tablePrefix": "cjhj",
    }  # pra数据


def DatasetConnect(DB_message):
    os.environ["NLS_LANG"] = (
        "SIMPLIFIED CHINESE_CHINA.AL32UTF8"  # 为避免连接Oracle乱码问题
    )
    # fund_db = cx_Oracle.connect(user=DB_message['userName'], \
    # password=DB_message['password'], \
    # dsn=DB_message['hostIP'] + '/' + DB_message['dbName'])
    fund_db = oracledb.connect(
        user=DB_message["userName"],
        password=DB_message["password"],
        dsn=DB_message["hostIP"] + "/" + DB_message["dbName"],
    )
    # cu = fund_db.cursor()
    return fund_db


######## step1: 建表
def CreateTable(param, table, table_name):
    fund_db = DatasetConnect(param.DB_write)
    cu = fund_db.cursor()
    variables = ""
    for key in table:
        variables = variables + key + " " + table[key] + ",\r"
    variables = variables[:-2] + "\r"
    sql = (
        "create table "
        + param.DB_write["tablePrefix"]
        + "."
        + table_name
        + "(\r"
        + variables
        + ")"
    )
    cu.execute(sql)
    cu.close()
    return


######## step2: 保存数据
def OutputData(param, table, table_name, resultDB):
    fund_db = DatasetConnect(param.DB_write)
    cu = fund_db.cursor()
    variables = ""
    placeholders = ""
    order = []
    i = 0
    for key in table:
        i += 1
        variables = variables + key + ","
        placeholders = placeholders + ":" + str(i) + ","
        order.append(key.strip('"'))
        if table[key][:6] == "number":
            start_index = table[key].find(",")
            num_decimal = int(table[key][start_index + 1 : -1])
            resultDB[key] = resultDB[key.strip('"')].apply(
                lambda x, num=num_decimal: (
                    round(x, num) if (x != "nan" and x != np.nan) else np.nan
                )
            )

    tmp = (
        "insert into "
        + param.DB_write["tablePrefix"]
        + "."
        + table_name
        + "("
        + variables[:-1]
        + ") values("
        + placeholders[:-1]
        + ")"
    )

    resultDB = resultDB.copy(deep=True)
    resultDB["CalDate"] = dt.datetime.today().strftime("%Y-%m-%d")
    resultDB = resultDB[order]
    if "EndDate" in order:
        resultDB["EndDate"] = resultDB["EndDate"].apply(lambda x: str(x)[:10])
    if "InfoPublDate" in order:
        resultDB["InfoPublDate"] = resultDB["InfoPublDate"].apply(lambda x: str(x)[:10])

    ## 无缺失数据的提交
    resultDB_keep = resultDB.copy()
    resultDB_keep = resultDB_keep.dropna(axis=0)
    resultDB1 = np.array(resultDB_keep).tolist()
    cu.executemany(tmp, resultDB1)
    fund_db.commit()

    index_drop = list(
        set(resultDB.index.tolist()).difference(set(resultDB_keep.index.tolist()))
    )
    resultDB_drop = resultDB.loc[index_drop]
    if resultDB_drop.shape[0] != 0:
        OutputNanData(param, cu, table_name, resultDB_drop)

    # print('finished insert')
    fund_db.commit()
    cu.close()
    return


def OutputNanData(param, table_name, data_df0):
    fund_db = DatasetConnect(param.DB_write)
    cur = fund_db.cursor()

    cols = data_df0.columns
    cols_str = ",".join(cols)
    i = 0
    while i < len(data_df0):
        data_df = data_df0.copy()
        data_df = data_df.iloc[i : i + 100, :]
        sql = "INSERT ALL "
        for index, row in data_df.iterrows():
            sql += "\nINTO " + table_name + "(" + cols_str + ") VALUES ("
            sql += "'"
            sql += "','".join(row.values.astype(str))
            sql += "'"
            sql += ")"
            if index == data_df.index[-1]:
                sql += " SELECT * FROM dual"
        sql = sql.replace("'nan'", "NULL")
        cur.execute(sql)
        i = i + 100
    return


######## step: 获取数据库的最后一个日期
def GetLastDate(cu, table_name):
    # fund_db = DatasetConnect(param.DB_write)
    # cu = fund_db.cursor()
    sql = (
        """
    SELECT MAX(TRADING_DATE) FROM CJFINRES.
    """
        + table_name
    )
    lastdate = pd.DataFrame(cu.execute(sql).fetchall()).iloc[0, 0]
    cu.close()
    return lastdate


def get_filelastdate(start_date, trade_dates, file_path, data_type, cu):
    from utils import read_csv

    if data_type == "dataset":
        last_date = GetLastDate(cu)  ### 获取数据库的最后一个日期
    else:
        holding_num = read_csv.get_holding_num(file_path)  # 读持仓文件
        if len(holding_num) != 0:
            last_date = holding_num.columns[-1]
        else:
            last_date = None

    if last_date is None:
        pass  ### 获取开始日期
    else:
        last_date = last_date
        start_date = trade_dates.iloc[
            trade_dates[trade_dates["date"] == last_date].index[0] - 1, 0
        ]

    start_index = trade_dates[trade_dates["date"] >= start_date].index[-1]
    return start_date, start_index
