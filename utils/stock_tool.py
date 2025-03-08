import datetime as dt
import pandas as pd
import numpy as np
from Config.job_config import FactorPara
from factor_analysis.data_io import (
    get_stock_industry,
    get_stock_return,
    get_stock_price,
)

# from Factor_Update.factor_update import get_old_data, get_stock_industry
from utils.read_index_data import get_index_component_weight
import os


# 合并收益数据
def merge_returndata_open(
    factordata,
    trade_days,
    end_date,
    frequency,
    price_market,
    price_data,
    interval_day=1,
):
    """
    factordata_ = merge_returndata(factordata_, trade_days,
                                   (dt.datetime.strptime(
                                       param.start_date, '%Y-%m-%d')-relativedelta(months=4)).strftime('%Y-%m-%d'),
                                   param.end_date, param.opt_freq, [], [], interval_day=1,
                                   param=param)
    @return:
    @param factordata: 因子数据
    @param trade_days: 日期数据
    @param start_date: 开始日期（可规定），若未规定（''）则为param中的开始日期
    @param end_date: 截止日期
    @param frequency: 间隔频率
    @param price_market: 外部输入的指数价格
    @param price_data: 外部输入的个股价格
    @param interval_day: 两个调仓日之间距离多少个trade_days
    @param param: 外部输入参数
    @return:

    """
    # 计算收益率
    trade_days["next_date"] = trade_days["date"].shift(-interval_day)

    _trade_days = trade_days[trade_days.date <= end_date]
    if len(_trade_days) > 0:
        end_date = dt.datetime.strftime(_trade_days.date.max(), "%Y-%m-%d")

    trade_days["next_date"].iloc[-1] = dt.datetime.strptime(end_date, "%Y-%m-%d")

    return_data = get_open_returns_by_date(trade_days, price_market, price_data)

    # 寻找当前交易日(trade_days)日期对应的数据(factordata)最新日期
    data = trade_days.copy(deep=True)
    data["true_date"] = data["date"]  # 交易日的真实日期
    del data["date"]
    try:
        factor_date = pd.DataFrame(
            factordata["date"].unique(), columns=["date"]
        )  # 因子数据的不重复日期
    except:
        print(
            "股票池选择超过因子文件股票池范围！选择修改读取的因子文件或param.stock_pool_list"
        )
    true_date = pd.DataFrame(
        data["true_date"].unique(), columns=["true_date"]
    )  # 交易日的不重复真实日期
    pool_date = factor_date.reset_index(drop=True)
    # 如果这一步报错则初始日期设置过晚或过早，较大几率是初始时间早于factordata的最初时间
    true_date["date"] = [
        pool_date.iloc[np.where(pool_date["date"] <= i)[0][-1], 0]
        for i in true_date["true_date"]
    ]  # 交易日和当前最新数据日期匹配
    # 匹配在当前交易日下，最新的因子数据日期，仅保留有因子数据的日期
    data = data.merge(
        true_date, on=["true_date"], how="left"
    )  # 交易日期merge因子数据日期
    data = data.merge(
        factordata, on=["date"], how="inner"
    )  # 交易日和因子数据merge，仅留下有因子数据的交易日
    data = data.reset_index(drop=True)
    data["date"] = data["true_date"]  # 数据赋值回原真实日期
    del data["true_date"]

    merge_data = pd.merge(
        data, return_data, how="left", on=["code", "date", "next_date"]
    )
    factordata = merge_data.rename(
        columns={"return_stock": "next_" + frequency + "_rtn"}
    )
    factordata = factordata.drop_duplicates(subset=["code", "date"])

    return factordata


def merge_returndata(
    factordata,
    trade_days,
    end_date,
    frequency,
    price_market,
    price_data,
    interval_day=1,
):
    """
    factordata_ = merge_returndata(factordata_, trade_days,
                                   (dt.datetime.strptime(
                                       param.start_date, '%Y-%m-%d')-relativedelta(months=4)).strftime('%Y-%m-%d'),
                                   param.end_date, param.opt_freq, [], [], interval_day=1,
                                   param=param)
    @return:
    @param factordata: 因子数据
    @param trade_days: 日期数据
    @param start_date: 开始日期（可规定），若未规定（''）则为param中的开始日期
    @param end_date: 截止日期
    @param frequency: 间隔频率
    @param price_market: 外部输入的指数价格
    @param price_data: 外部输入的个股价格
    @param interval_day: 两个调仓日之间距离多少个trade_days
    @param param: 外部输入参数
    @return:

    """
    # 计算收益率
    trade_days["next_date"] = trade_days["date"].shift(-interval_day)

    _trade_days = trade_days[trade_days.date <= end_date]
    if len(_trade_days) > 0:
        end_date = dt.datetime.strftime(_trade_days.date.max(), "%Y-%m-%d")

    trade_days["next_date"].iloc[-1] = dt.datetime.strptime(end_date, "%Y-%m-%d")

    return_data = get_returns_by_date(trade_days, price_market, price_data)

    # 寻找当前交易日(trade_days)日期对应的数据(factordata)最新日期
    data = trade_days.copy(deep=True)
    data["true_date"] = data["date"]  # 交易日的真实日期
    del data["date"]
    try:
        factor_date = pd.DataFrame(
            factordata["date"].unique(), columns=["date"]
        )  # 因子数据的不重复日期
    except:
        print(
            "股票池选择超过因子文件股票池范围！选择修改读取的因子文件或param.stock_pool_list"
        )
    true_date = pd.DataFrame(
        data["true_date"].unique(), columns=["true_date"]
    )  # 交易日的不重复真实日期
    pool_date = factor_date.reset_index(drop=True)
    # 如果这一步报错则初始日期设置过晚或过早，较大几率是初始时间早于factordata的最初时间
    true_date["date"] = [
        pool_date.iloc[np.where(pool_date["date"] <= i)[0][-1], 0]
        for i in true_date["true_date"]
    ]  # 交易日和当前最新数据日期匹配
    # 匹配在当前交易日下，最新的因子数据日期，仅保留有因子数据的日期
    data = data.merge(
        true_date, on=["true_date"], how="left"
    )  # 交易日期merge因子数据日期
    data = data.merge(
        factordata, on=["date"], how="inner"
    )  # 交易日和因子数据merge，仅留下有因子数据的交易日
    data = data.reset_index(drop=True)
    data["date"] = data["true_date"]  # 数据赋值回原真实日期
    del data["true_date"]

    merge_data = pd.merge(
        data, return_data, how="left", on=["code", "date", "next_date"]
    )
    factordata = merge_data.rename(
        columns={"return_stock": "next_" + frequency + "_rtn"}
    )
    factordata = factordata.drop_duplicates(subset=["code", "date"])

    return factordata


# 计算个股收益，根据日期做
def get_open_returns_by_date(trade_days, price_market, price_data):
    """
    :param param:
    :param trade_days:
    :return:
    """
    price_data = price_data.sort_values(["code", "date"]).reset_index(drop=True)
    price_df = []
    for code_, df_ in price_data.groupby("code"):
        df_["next_open"] = df_["stock_open"].shift(-1)
        price_df.append(df_)
    price_data = pd.concat(price_df, axis=0)
    price_data["next_open"].fillna(
        price_data["stock_open"], inplace=True
    )  # 未知下一个开盘，就用当日开盘填充
    price_data = price_data[
        (price_data.date.isin(trade_days.date.unique()))
        | (price_data.date.isin(trade_days.next_date.unique()))
    ]
    price_data = price_data.merge(trade_days, on=["date"], how="left")
    price_stock = price_data[["code", "date", "next_open"]].copy(deep=True)
    price_stock = price_stock.rename(
        columns={"date": "next_date", "next_open": "next_stock_open"}
    )
    price_data = price_data.merge(price_stock, on=["code", "next_date"], how="inner")
    price_data["return_stock"] = (
        price_data["next_stock_open"] / price_data["next_open"] - 1
    )

    price_market["next_open"] = price_market["market_open"].shift(-1)
    price_market["next_open"].fillna(
        price_market["market_open"], inplace=True
    )  # 未知下一个开盘，就用当日开盘填充

    price_market = price_market[
        (price_market.date.isin(trade_days.date.unique()))
        | (price_market.date.isin(trade_days.next_date.unique()))
    ]
    price_market = price_market.merge(trade_days, on="date", how="left")
    price_market_next = price_market[["date", "next_open"]].copy(deep=True)
    price_market_next = price_market_next.rename(
        columns={"date": "next_date", "next_open": "next_market_open"}
    )
    price_market = price_market.merge(price_market_next, on=["next_date"], how="inner")
    price_market["return_market"] = (
        price_market["next_market_open"] / price_market["next_open"] - 1
    )

    price_data = price_data.merge(
        price_market[["date", "return_market"]], on="date", how="outer"
    )
    price_data["return_relative"] = (1 + price_data["return_stock"]) / (
        1 + price_data["return_market"]
    ) - 1
    price_data = price_data.dropna()
    return price_data


def get_returns_by_date(trade_days, price_market, price_data):
    """
    :param param:
    :param trade_days:
    :return:
    """
    price_data = price_data.sort_values(["code", "date"]).reset_index(drop=True)
    price_data = price_data[
        (price_data.date.isin(trade_days.date.unique()))
        | (price_data.date.isin(trade_days.next_date.unique()))
    ]
    price_data = price_data.merge(trade_days, on=["date"], how="left")
    price_stock = price_data[["code", "date", "stock_close"]].copy(deep=True)
    price_stock = price_stock.rename(
        columns={"date": "next_date", "stock_close": "next_stock_close"}
    )
    price_data = price_data.merge(price_stock, on=["code", "next_date"], how="inner")
    price_data["return_stock"] = (
        price_data["next_stock_close"] / price_data["stock_close"] - 1
    )

    price_market = price_market[
        (price_market.date.isin(trade_days.date.unique()))
        | (price_market.date.isin(trade_days.next_date.unique()))
    ]
    price_market = price_market.merge(trade_days, on="date", how="left")
    price_market_next = price_market[["date", "market_close"]].copy(deep=True)
    price_market_next = price_market_next.rename(
        columns={"date": "next_date", "market_close": "next_market_close"}
    )
    price_market = price_market.merge(price_market_next, on=["next_date"], how="inner")
    price_market["return_market"] = (
        price_market["next_market_close"] / price_market["market_close"] - 1
    )

    price_data = price_data.merge(
        price_market[["date", "return_market"]], on="date", how="outer"
    )
    price_data["return_relative"] = (1 + price_data["return_stock"]) / (
        1 + price_data["return_market"]
    ) - 1
    price_data = price_data.dropna()
    return price_data


# 剔除不想要的行业
def drop_industry_stock(
    factordata, param, industry: list = None, industry_code_df: pd.DataFrame = None
):
    """
    :param industry: 要去除的行业代码列表，如['801780', '801790']
    :param industry_code_df: 股票行业代码矩阵，可选
    """
    if industry is None:
        print("未进行行业筛选")
        factordata_dropped = factordata.copy()
    else:
        if industry_code_df is None:
            industry_code_df = get_stock_industry(param.start_date, param.end_date)

        # industry_code_df = industry_code_df.unstack().reset_index().rename(columns={0: 'industry_code'})
        print("factordata:", factordata)
        print("industry_code_df:", industry_code_df)
        industry_code_df["industry_code"] = industry_code_df["sw_industry"].apply(
            lambda x: FactorPara.industry_mapping.get(x)
        )
        industry_code_df.rename(
            columns={"security_code": "code", "trade_date": "date"}, inplace=True
        )
        factordata_dropped = factordata.merge(
            industry_code_df, on=["code", "date"], how="left"
        )
        factordata_dropped = factordata_dropped[
            ~factordata_dropped["industry_code"].isin(industry)
        ]
    return factordata_dropped


# 对data进行筛选，筛选出所需要的股票池的股票,读取股票池成分股并匹配，考虑用get_index_component替代
def get_pool_stock(
    data,
    stock_pool,
    start_date,
    end_date,
    pool=None,
    file_pool=FactorPara.file_index,
    if_coverage=False,
    factor_name="",
):
    if (stock_pool == "all") | (stock_pool == "881001.WI"):
        pass
    else:
        if pool is None:
            # file_index_constituent = file_pool + stock_pool[:6] + '_component_weight.csv'
            file_index_constituent = os.path.join(
                file_pool, stock_pool[:6] + "_component_weight.csv"
            )
            # file_index_constituent = file_pool + '/000500_component_weight.csv'
            print("file_index_constituent:", file_index_constituent)
            pool = get_index_component_weight(
                dt.datetime.strptime(start_date, "%Y-%m-%d"),
                dt.datetime.strptime(end_date, "%Y-%m-%d"),
                stock_pool,
                file_index_constituent,
                FactorPara.file_trade_day,
            )[["code", "date"]]
        pool["code"] = pool["code"].str.zfill(6)
        pool = pool.sort_values(by="date", ascending=True)
        print("=========== pool:")
        print(pool)
        # 寻找当前数据日期对应的股票池最新日期
        data["true_date"] = data["date"]  # 数据真实日期
        # del data['date']
        pool_date = pd.DataFrame(
            pool["date"].unique(), columns=["date"]
        )  # 股票池不重复日期
        true_date = pd.DataFrame(
            data["true_date"].unique(), columns=["true_date"]
        )  # 数据的不重复真实日期
        pool_date = pool_date.reset_index(drop=True)
        if (
            true_date["true_date"][0] < pool_date["date"][0]
        ):  # 如果股票池的最早日期比因子数据的最早日期晚
            true_date = true_date[
                true_date["true_date"] >= pool_date["date"][0]
            ]  # 则按股票池的日期规范因子数据
            true_date = true_date.reset_index(drop=True)
        true_date["date"] = [
            pool_date.iloc[np.where(pool_date["date"] <= i)[0][-1], 0]
            for i in true_date["true_date"]
        ]  # 数据和股票池日期匹配
        # 匹配数据在当前日期下，最新的股票池信息，仅保留股票池内的数据
        data = data.merge(
            true_date, on=["true_date"], how="left"
        )  # 数据日期merge股票池日期
        del data["date_y"]
        data.rename(columns={"date_x": "date"}, inplace=True)
        print("=============================== data")
        print(data)
        data = data.merge(
            pool, on=["code", "date"], how="inner"
        )  # 数据和股票池merge，仅留下在股票池内的数据
        data = data.reset_index(drop=True)
        data["date"] = data["true_date"]  # 数据赋值回原真实日期
        del data["true_date"]
    print("data:")
    print(data)
    # 求覆盖度
    if if_coverage:
        if (stock_pool != "all") & (stock_pool != "881001.WI"):
            index_count = pool.groupby("date").count()["code"]
        else:
            # stock_all, data_endDate = get_old_data(config.file_stock, 'close_post.csv', data.date.min(),
            #                                        data.date.max())
            stock_all = get_stock_price()
            stock_all = (
                stock_all.stack()
                .reset_index()
                .rename(columns={"level_1": "code", 0: "close"})
            )
            stock_all = stock_all[stock_all.date.isin(data.date)]
            stock_all = stock_all.dropna()
            index_count = stock_all.groupby("date").count()["code"]
        coverage = pd.DataFrame()
        data_nonan = data.copy(deep=True).dropna()

        if factor_name + "_noneut" in data.columns:
            coverage["number_noneut"] = data_nonan.groupby("date").count()[
                factor_name + "_noneut"
            ]
            coverage["index_component"] = index_count
            coverage["rate_noneut"] = (
                coverage["number_noneut"] / coverage["index_component"]
            )
        if factor_name + "_neut" in data.columns:
            coverage["number_neut"] = data_nonan.groupby("date").count()[
                factor_name + "_neut"
            ]
            coverage["index_component"] = index_count
            coverage["rate_neut"] = (
                coverage["number_neut"] / coverage["index_component"]
            )
        if (factor_name + "_neut" not in data.columns) & (
            factor_name + "_noneut" not in data.columns
        ):
            coverage["number"] = data_nonan.groupby("date").count()[
                factor_name
            ]  # 覆盖股数
            coverage["index_component"] = index_count
            coverage["rate"] = (
                coverage["number"] / coverage["index_component"]
            )  # 覆盖率
        data = data.dropna()
        return data, coverage
    else:
        data = data.dropna()
        return data
