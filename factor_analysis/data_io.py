import pandas as pd
import datetime as dt
import os
from functools import lru_cache

here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def get_industry_mapping():
    file = os.path.join(here, r"Database/files/stock_belongs_industry_SW2014.csv")
    industry_code_df: pd.DataFrame = pd.read_csv(file, encoding="utf8")
    name_list = industry_code_df["industry_name"].tolist()
    code_list = industry_code_df["industry_code"].tolist()
    res = {name: code for name, code in zip(name_list, code_list)}
    return res


# lru_cache用来做数据缓存，第一次加载较慢，之后直接用缓存数据，速度加快
# 获取股票所在行业
@lru_cache(maxsize=128)
def get_stock_industry(start_date: str = None, end_date: str = None) -> pd.DataFrame:
    """
    读取行业数据
    :param start_date:
    :param end_date:
    :return:
    """
    file = os.path.join(here, r"Database/Origin_Database/industry_cate.pkl")
    stock_industry = pd.read_pickle(file)
    stock_industry.trade_date = pd.to_datetime(stock_industry.trade_date)
    stock_industry.sort_values(by="trade_date", inplace=True)
    if start_date:
        stock_industry = stock_industry[stock_industry.trade_date >= start_date]
    if end_date:
        stock_industry = stock_industry[stock_industry.trade_date <= end_date]

    return stock_industry


# stock_return:['code', 'date', 'next_date', 'open_close_return', 'stock_return']
# factor_df: [date, code,'stock_return', 'open_close_return', stock_open, stock_close, f1, f2,f3, .....,fn]


@lru_cache(maxsize=128)
def get_factor_data(start_date: str = None, end_date: str = None) -> pd.DataFrame:
    # 后面需要根据不同的数据源进行调整，函数非常耦合
    file_arr = [
        (os.path.join(here, r"Database/Neut_Database/BBI_neut.pkl"), "bbi" + "_neut"),
        (
            os.path.join(here, r"Database/Neut_Database/corr_model_neut.pkl"),
            "fin" + "_neut",
        ),
    ]
    factor_data = None
    for file, factor_name in file_arr:

        single_factor_df = pd.read_pickle(file)
        single_factor_df.rename(
            columns={"trade_date": "date", "security_code": "code"}, inplace=True
        )
        factor_col = single_factor_df.set_index(["date", "code"]).columns[0]
        single_factor_df.rename(columns={factor_col: factor_name}, inplace=True)
        single_factor_df = single_factor_df[["date", "code", factor_name]]
        if factor_data is None:
            factor_data = single_factor_df
        else:
            factor_data = pd.merge(factor_data, single_factor_df, how="inner")

    factor_data.sort_values(by="date", inplace=True)
    if start_date:
        factor_data = factor_data[factor_data.date >= start_date]
    if end_date:
        factor_data = factor_data[factor_data.date <= start_date]

    return factor_data


@lru_cache(maxsize=128)
def get_calendar_data() -> pd.DataFrame:
    file_calendar = os.path.join(here, r"Database/files/DailyTradeDays.csv")
    calendar_ = pd.read_csv(file_calendar)
    calendar_.date = calendar_.date.astype(str)
    calendar_.date = pd.to_datetime(calendar_.date)
    # calendar_.date = calendar_.date.apply(lambda x: x.strftime('%Y-%m-%d'))
    return calendar_


@lru_cache(maxsize=128)
def get_stock_price(start_date: str = None, end_date: str = None) -> pd.DataFrame:
    file = os.path.join(here, r"Database/Origin_Database/stock_price.pkl")
    stock_price = pd.read_pickle(file)
    stock_price = stock_price[
        ["security_code", "trade_date", "s_dq_open", "s_dq_close"]
    ]
    stock_price.rename(
        columns={
            "security_code": "code",
            "trade_date": "date",
            "s_dq_open": "stock_open",
            "s_dq_close": "stock_close",
        },
        inplace=True,
    )
    return stock_price


@lru_cache(maxsize=128)
def get_stock_return(start_date: str = None, end_date: str = None) -> pd.DataFrame:
    stock_trade_days: pd.DataFrame = get_calendar_data()
    stock_trade_days["next_date"] = stock_trade_days["date"].shift(-1)
    stock_trade_days["next_date"].iloc[-1] = stock_trade_days.date.max()
    stock_return = get_stock_price(start_date, end_date)
    stock_return = stock_return.sort_values(["code", "date"])
    stock_return["next_open"] = stock_return.groupby("code")["stock_open"].shift(
        -1
    )  # 获得下一日的开盘价
    stock_return["next_open"].fillna(
        stock_return["stock_open"], inplace=True
    )  # 未知下一个开盘，就用当日开盘填充
    stock_return = stock_return[
        (stock_return.date.isin(stock_trade_days.date.unique()))
        | (stock_return.date.isin(stock_trade_days.next_date.unique()))
    ]
    # print('============= 02', stock_return)
    stock_return = stock_return.merge(stock_trade_days, on=["date"], how="left")
    # stock_return: [date, code, stock_open, stock_close, next_date, next_open]
    # print('============= 03', stock_return)
    price_stock = stock_return[["code", "date", "stock_close", "next_open"]].copy(
        deep=True
    )
    price_stock = price_stock.rename(
        columns={
            "date": "next_date",
            "stock_close": "today_close",
            "next_open": "next_stock_open",
        }
    )
    # price_stock: ['code', 'next_date', 'today_close', 'next_stock_open']

    stock_return = stock_return.merge(
        price_stock, on=["code", "next_date"], how="inner"
    )
    stock_return["stock_return"] = (
        stock_return["next_stock_open"] / stock_return["next_open"] - 1
    )
    stock_return["open_close_return"] = (
        stock_return["today_close"] / stock_return["next_open"] - 1
    )
    # stock_return:['code', 'next_date',stock_open, stock_close, next_open, 'today_close', 'next_stock_open',       stock_return, open_close_return]
    # 去除次新和ST时间的涨跌幅
    stock_return_pivot = stock_return.pivot(
        columns="code", index="date", values="stock_return"
    )
    # if param.remove_st:  # 去除ST
    #     stock_return_pivot = remove_st_weekly(stock_return_pivot)
    # if param.remove_subnew:  # 去除次新
    #     stock_return_pivot = remove_subnew_weekly(stock_return_pivot
    stock_return_pivot = stock_return_pivot.stack().reset_index()
    stock_return_pivot.columns = ["date", "code", "stock_return"]
    stock_return = pd.merge(
        left=stock_return[["code", "date", "next_date", "open_close_return"]],
        right=stock_return_pivot,
        how="inner",
    )

    return stock_return


if __name__ == "__main__":
    # df = get_stock_industry()
    # print(df)
    # factor = get_factor_data()
    # print(factor)
    # calendar = get_calendar_data()
    # print(calendar)
    # stock_return_ = get_stock_return()
    # print(stock_return_)
    # print(stock_return_.columns)
    print()
    industry_code_df = get_stock_industry()
    print(industry_code_df)
    # industry_code_df = industry_code_df.unstack().reset_index().rename(columns={0: 'industry_code'})
    # print(industry_code_df)
    print(get_industry_mapping())
