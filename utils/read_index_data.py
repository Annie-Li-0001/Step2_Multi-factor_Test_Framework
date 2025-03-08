import os
import pandas as pd
import numpy as np
import datetime as dt
from utils.days_tool import get_tradeday
from dateutil.relativedelta import relativedelta


# 获取指数成分股
def get_index_component_weight(
    start_date,
    end_date0,
    index_code,
    file_index_component,
    file_trade_days,
    trade_day=None,
    if_update=1,
):
    # start_date的格式与trade_day中元素格式需保持一致
    # import Factor_Update.windAPI as api
    # import Factor_Lib.update_factor_data as wind
    if trade_day is None:
        trade_day_temp = get_tradeday(
            file_trade_days,
            "m",
            (start_date - relativedelta(months=1)).strftime("%Y-%m-%d"),
            dt.datetime.strftime(end_date0, "%Y-%m-%d"),
            date_type=-1,
        )
    else:  # 在Barra_pure factor游泳到
        trade_day_temp = trade_day.copy(deep=True)
    print("trade_day_temp:", trade_day_temp)
    end_date = trade_day_temp["date"].iloc[-1]
    print("end_date:", end_date)
    # 判断是否有文件
    if os.path.exists(file_index_component):  # 文件存在
        # 读取已有数据
        data_pivot = pd.read_csv(
            file_index_component,
            index_col=0,
            dtype={"date": str, "component_code": str, "code": str},
        )

        data_pivot["code"] = [str(x).zfill(6) for x in data_pivot["code"]]
        data_pivot["component_code"] = [
            str(x).zfill(6) for x in data_pivot["component_code"]
        ]

        # 判断是否需要更新数据
        data_pivot = data_pivot.replace(float("nan"), "0")
        data_endDate = dt.datetime.strptime(data_pivot["date"].max(), "%Y%m%d")
        print("data_pivot:", data_pivot)
    else:  # 文件不存在
        data_endDate = dt.datetime.strptime(
            "19900101", "%Y%m%d"
        )  # 数据结束日期既为数据开始日期
        data_pivot = pd.DataFrame()
    print("data_endDate:", data_endDate)
    # 判断是否需要更新数据
    # if (data_endDate < end_date) & (if_update == 1):  # TODO：& (trade_day is None)之前有这么个条件，不知道有什么用
    #     # 如果测试截止日没有数据，则更新数据
    #     data_add = api.update_index_component_weight(dt.datetime.strftime(data_endDate, '%Y%m%d'),
    #                                                  dt.datetime.strftime(end_date, '%Y%m%d'), index_code)  # 获取增量数据
    #
    #     # 增量数据与原数据拼接
    #     data_pivot = pd.concat([data_pivot, data_add], axis=0)
    #     data_pivot = data_pivot.drop_duplicates(subset=['component_code', 'date'], keep='last')
    #
    #     # 保存最新数据
    #     data_pivot = data_pivot.sort_values(by='date', ascending=True)
    #     data_pivot.to_csv(file_index_component)

    data_pivot["date"] = [
        dt.datetime.strptime(str(i), "%Y%m%d") for i in data_pivot["date"]
    ]

    data_all = data_pivot.copy(deep=True).pivot(
        index="date", columns="component_code", values="component_weight"
    )
    data_all = data_all.replace(np.nan, 0).unstack().reset_index(drop=False)
    data_all.columns = ["code", "date", "label"]

    # 处理数据
    # 读所需交易日数据
    if trade_day is None:
        trade_day = get_tradeday(
            file_trade_days,
            "d",
            dt.datetime.strftime(start_date, "%Y-%m-%d"),
            dt.datetime.strftime(end_date0, "%Y-%m-%d"),
            date_type=-1,
        )
    trade_day = trade_day[trade_day["date"] >= start_date].sort_values(by="date")

    # 获取指数成分股数据
    date_list = trade_day.date.tolist() + [i for i in data_all.date.unique()]
    date_list = (
        pd.DataFrame(date_list, columns=["date"])
        .sort_values(by="date", ascending=True)
        .drop_duplicates(subset="date", keep="last")
    )
    data_allday = (
        pd.DataFrame(index=date_list.date, columns=data_all.code.unique())
        .unstack()
        .reset_index()
    )
    data_allday.columns = ["code", "date", "0"]

    data_allday = data_allday[["code", "date"]].merge(
        data_all, on=["code", "date"], how="left"
    )
    data_allday = data_allday.pivot(index="date", columns="code", values="label")
    data_allday = data_allday.fillna(method="ffill")
    data_allday = (
        data_allday[
            (data_allday.index >= start_date) & (data_allday.index <= end_date0)
        ]
        .unstack()
        .reset_index()
    )
    data_allday = data_allday.rename(columns={0: "weight"})
    data_allday["weight"] = data_allday["weight"].replace(0, np.nan)
    data_allday = data_allday.dropna()
    # print('index_component_weight finished')
    return data_allday
