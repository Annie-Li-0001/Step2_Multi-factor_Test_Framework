"""
全部已核对
"""

import pandas as pd
import datetime as dt


# 读日期
def get_tradeday(
    file_trade_days, frequency, start_date, end_date, date_type=-1, last_date=0
):
    trade_day = pd.read_csv(file_trade_days, dtype={"date": str})
    trade_day = get_trade_days_by_frequency(trade_day, frequency, date_type, last_date)
    trade_day["date"] = pd.to_datetime(trade_day["date"], format="%Y%m%d")
    beginDate = dt.datetime.strptime(start_date, "%Y-%m-%d")
    endDate = dt.datetime.strptime(end_date, "%Y-%m-%d")
    trade_day = trade_day[trade_day.date >= beginDate]
    trade_day = trade_day[trade_day.date <= endDate]
    if date_type == 0 and trade_day.iloc[-1, 0] != dt.datetime.strptime(
        end_date, "%Y-%m-%d"
    ):
        trade_day = trade_day.append(
            [{"date": dt.datetime.strptime(end_date, "%Y-%m-%d")}]
        )
    trade_day = trade_day.reset_index(drop=True)
    return trade_day


# 根据固定频率获取交易日数据
def get_trade_days_by_frequency(df, frequency, date_type=-1, last_day=0):
    """
    :param df: Dataframe, 内容为int
    :param param: param.frequency: str 'd','w', 'm', 'q'
    :param date_type: 是否要加上非频率周期的最后一天
    :param last_day: 该周期的倒数第几天
    :return:
    """
    import pandas as pd
    import datetime as dt
    import numpy as np

    df = df.sort_values(by="date", ascending=True)
    df["date"] = [int(i) for i in df["date"]]
    df = df.sort_values(by=["date"], ascending=True)
    df = df.reset_index(drop=True)
    df["Month"] = df["date"] // 100
    df["Year"] = df["date"] // 10000
    df_final = pd.DataFrame()

    if frequency == "d":
        df_final["Date"] = df["date"]

    if frequency == "m":
        df_final["Date"] = df.groupby("Month").last()["date"]

    if frequency == "q":
        df["Season"] = df["Month"] % 100
        df_temp = df[(df["Season"] % 3 == 0)]
        df_final["Date"] = df_temp.groupby(["Year", "Season"]).last()["date"]

    if frequency == "w":
        df["Date(datetime)"] = pd.to_datetime(df["date"].astype(str))
        df["delta_days"] = df["Date(datetime)"].shift(-1) - df["Date(datetime)"]
        df_temp = df[(df["delta_days"] > dt.timedelta(days=2))]
        df_temp = df_temp.reset_index(drop=True).sort_values(
            by="Date(datetime)", ascending=True
        )
        df_temp["Week"] = df_temp.index
        df = df.merge(
            df_temp[["Date(datetime)", "Week"]], on=["Date(datetime)"], how="left"
        )
        df["Week"] = df["Week"].fillna(method="ffill")
        df_final["Date"] = df.groupby(["Week"]).apply(
            lambda x: x["date"].iloc[last_day] if x.shape[0] > -last_day else np.nan
        )
        df_final = df_final.dropna()
        # df_final['Date'] = df_temp['date']

    trade_days = [str(i) for i in df_final["Date"].tolist()]
    trade_days = pd.DataFrame(trade_days)
    trade_days.columns = ["date"]
    return trade_days


# 根据B数据的日期，提取B数据日期中能获取到的最新A数据
def match_latest_data(df_A, df_B, if_equal=True):
    """

    :param df_A: 提供数据，列名有date
    :param df_B: 提供日期，列名有date
    :return:
    """
    import pandas as pd
    import numpy as np

    # 寻找当前交易日(trade_days:df_B)日期对应的数据(factordata:df_A)最新日期
    df_B = df_B[df_B["date"] >= df_A["date"].min()]
    data = df_B[["date"]].copy(deep=True)
    data["true_date"] = data["date"]  # 交易日的真实日期
    del data["date"]
    factor_date = pd.DataFrame(
        df_A["date"].unique(), columns=["date"]
    )  # 因子数据的不重复日期
    true_date = pd.DataFrame(
        data["true_date"].unique(), columns=["true_date"]
    )  # 交易日的不重复真实日期
    pool_date = factor_date.sort_values(by="date", ascending=True).reset_index(
        drop=True
    )
    if if_equal:
        true_date["date"] = [
            pool_date.iloc[np.where(pool_date["date"] <= i)[0][-1], 0]
            for i in true_date["true_date"]
        ]  # 交易日和当前最新数据日期匹配
    else:
        true_date["date"] = [
            pool_date.iloc[np.where(pool_date["date"] < i)[0][-1], 0]
            for i in true_date["true_date"]
        ]  # 交易日和当前最新数据日期匹配
    # 匹配在当前交易日下，最新的因子数据日期，仅保留有因子数据的日期
    data = true_date.copy(deep=True)
    data = data.merge(
        df_A, on=["date"], how="inner"
    )  # 交易日和因子数据merge，仅留下有因子数据的交易日
    data = data.reset_index(drop=True)
    data["date"] = data["true_date"]  # 数据赋值回原真实日期
    del data["true_date"]
    return data


# 获取财报截止日
def get_enddate(date):
    if date < dt.datetime(date.year, 4, 30):
        return dt.datetime(date.year - 1, 9, 30)
    elif date < dt.datetime(date.year, 8, 31):
        return dt.datetime(date.year, 3, 31)
    elif date < dt.datetime(date.year, 10, 31):
        return dt.datetime(date.year, 6, 30)
    else:
        return dt.datetime(date.year, 9, 30)


# 获取财报发布日
def get_reportdate(date):
    if date.month == 3:
        return dt.datetime(date.year + 1, 5, 1)
    elif date.month == 6:
        return dt.datetime(date.year, 9, 1)
    elif date.month == 9:
        return dt.datetime(date.year, 11, 1)
    elif date.month == 12:
        return dt.datetime(date.year + 1, 5, 1)


# 数据转换
def format_to_datetime(dt64):
    import numpy as np
    import datetime as dt

    return dt.datetime.fromtimestamp(
        (dt64 - np.datetime64("1970-01-01T00:00:00Z")) / np.timedelta64(1, "s")
    )


# 对数据做日期错位处理
def change_use_date(report_data):
    # 日期错位处理
    # 处理原因：一般交易所晚上就会将第二天的公告发完，因此可做日期错位，方便在第二天开盘前选好操作个股
    from Update_Data.windAPI import get_TradeDay

    start_date = (report_data["date"].min() - dt.timedelta(days=10)).strftime("%Y%m%d")
    end_date = report_data["date"].max().strftime("%Y%m%d")
    trade_days = get_TradeDay(start_date, end_date)
    trade_days["use_date"] = trade_days["date"]  # 数据使用日期
    trade_days = match_latest_data(trade_days, report_data, False)  # 合并最近的交易日
    report_data = report_data.sort_values(by="date").drop_duplicates(
        subset=["date", "code"], keep="first"
    )
    report_data = report_data.merge(trade_days, on="date", how="left")
    report_data["date"] = report_data["use_date"]
    del report_data["use_date"]
    return report_data
