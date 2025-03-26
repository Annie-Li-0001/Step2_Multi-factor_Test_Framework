import os
import time
import datetime as dt
import multiprocessing as mp
from functools import partial
from scipy import stats
import pandas as pd
import numpy as np
import seaborn as sns
from utils import stock_tool
import matplotlib
from os import mkdir
from .plot import show_return_result_ls
from .perf import stats_return_time_series_result
from Config.job_config import FactorPara

import warnings

warnings.filterwarnings("ignore")


def get_trade_days_by_frequency(df, frequency, date_type=-1, last_day=0):
    """
    :param df: Dataframe, 内容为int
    :param param: param.frequency: str 'd','w', 'm', 'q'
    :param date_type:
    :param last_day:
    :return:
    """
    import datetime as dt

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


# read index price data
def get_index_open(file_index_open, begin_date, end_date):
    """
    :param param:
    :return:
    """

    begin_date = dt.datetime.strftime(begin_date, "%Y%m%d")
    end_date = dt.datetime.strftime(end_date, "%Y%m%d")

    if os.path.exists(file_index_open):
        price_A = pd.read_csv(file_index_open, dtype={"date": str})
        data_endDate = price_A.date.max()
        price_A = price_A.set_index("date")

    else:
        price_A = pd.DataFrame()
        data_endDate = dt.datetime.strftime(
            dt.datetime.strptime("1970-01-01", "%Y-%m-%d"), "%Y%m%d"
        )

    # If the end_date is not a trading day, an error will occur: Move the end_date to the nearest previous trading day to prevent the error
    trade_days = get_tradeday(
        FactorPara.file_trade_day,
        "d",
        dt.datetime.strftime(dt.datetime.strptime(begin_date, "%Y%m%d"), "%Y-%m-%d"),
        dt.datetime.strftime(dt.datetime.strptime(end_date, "%Y%m%d"), "%Y-%m-%d"),
        date_type=-1,
        last_date=0,
    )
    trade_days = trade_days[trade_days.date <= end_date]
    if len(trade_days) > 0:
        end_date = dt.datetime.strftime(trade_days.date.max(), "%Y%m%d")

    price_A = price_A[price_A.index >= begin_date]
    price_A = price_A[price_A.index <= end_date]
    price_A.index = pd.to_datetime(price_A.index, format="%Y%m%d")
    price_A = price_A.reset_index(drop=False)
    print("market price finished")

    return price_A


def cal_rankIC(df, factors_name):
    rank_IC = df.groupby("date").corr(method="spearman")
    rank_IC = pd.pivot_table(
        rank_IC.reset_index(), values="stock_return", index="date", columns="level_1"
    )[factors_name]

    s_dt = pd.DataFrame(index=list(set(df.date).union(df.next_date))).sort_index()
    s_dt["date"] = s_dt.index
    s_dt["date"] = s_dt["date"].shift(-1)
    s_dt = s_dt["date"]

    rank_IC["date"] = rank_IC.index
    rank_IC["date"] = rank_IC["date"].map(s_dt)
    rank_IC = rank_IC.dropna(subset="date").set_index("date")

    return rank_IC


def get_group(indicator, param, data, isAscend=True):
    import pandas as pd

    _data = data.drop_duplicates(subset=[indicator])
    _data = _data[_data[indicator].abs() > 1e-10]
    if len(_data) < 2:
        data[indicator + "_group"] = np.nan
        return data

    num_group = param.group_num
    label = [i for i in range(1, num_group + 1)]  # 创建组号
    try:
        category = pd.qcut(data[indicator], num_group, labels=label)
    except:
        ranks = data[indicator].rank(ascending=isAscend)
        category = pd.cut(ranks, bins=num_group, labels=label)
    category.name = indicator + "_group"
    new_data = data.join(category)
    return new_data


def long_short_test(
    factordata_pool_neut,
    factor,
    param,
    trade_days,
    index_code="000300.SH",
    file_index_open=None,
):
    """
    :param if_positive: 因子与收益关系是正向时为True，反向时为False
    :param note: 是否中性化的文件名备注
    single_factor_group_return:记录各组每期收益
    result_single_f_group:记录每期个股、因子值和分组
    """

    df = factordata_pool_neut.copy()
    df[factor + "_group"] = df.groupby("date").apply(partial(get_group, factor, param))[
        factor + "_group"
    ]

    return_df = df.pivot_table(
        index="date", columns=factor + "_group", values="stock_return", aggfunc="mean"
    )
    return_df.columns = [str(i) for i in range(1, param.group_num + 1)]
    # 多空组合计算
    return_df["多空"] = (1 + return_df[str(param.group_num)]) / (1 + return_df["1"]) - 1
    # 第10组相对市场指数超额收益
    mkt = get_index_open(file_index_open, df.date.min(), df.date.max())
    mkt["next_open"] = mkt["market_open"].shift(-1)
    mkt["next_open"].fillna(
        mkt["market_open"], inplace=True
    )  # 未知下一个开盘，就用当日开盘填充
    mkt_ret = mkt.set_index("date")
    mkt_ret = mkt_ret[mkt_ret.index.isin(trade_days.date)]
    mkt_ret["bench_ret"] = mkt_ret["next_open"].shift(-1) / mkt_ret["next_open"] - 1
    mkt_ret.reset_index(inplace=True)
    mkt_ret = mkt_ret[["date", "bench_ret"]]
    mkt_ret.dropna(inplace=True)  # 没有最后一期的收益
    mkt_ret.date = pd.to_datetime(mkt_ret.date)
    return_df = return_df.join(
        mkt_ret[["date", "bench_ret"]].drop_duplicates().set_index("date")
    ).dropna()
    return_df["相对收益"] = return_df[str(param.group_num)] - return_df["bench_ret"]
    # 统计相关指标
    stats_result = pd.DataFrame()
    for group_name in ["多空", "相对收益"]:
        result = pd.DataFrame(return_df[[group_name, "bench_ret"]])
        stats_result_ = stats_return_time_series_result(
            result, param, group_name, trade_days["date"].iloc[-1]
        )
        pnl_t_value = stats.ttest_1samp(
            result[group_name], 0, nan_policy="omit"
        ).statistic
        stats_result_["收益率t值"] = pnl_t_value
        stats_result = pd.concat([stats_result, stats_result_])
    stats_result["factor"] = factor
    return return_df, stats_result


def factor_pool_test_simple(
    factor_neut,
    trade_days,
    stock_return,
    industry_code_df,
    drop_industry: list,
    index_code: str,
    param,
):
    # 这个函数是一个壳子，用来启动 因子检验的 进程
    pool = mp.Pool(processes=10)
    grp_args = [
        (
            factor_name,
            factor_neut,
            trade_days,
            stock_return,
            industry_code_df,
            drop_industry,
            index_code,
            param,
        )
        for factor_name in param.factor_list
    ]
    results = pool.map(factor_pool_test, grp_args)
    pool.close()
    pool.join()
    return results


# 针对不同index的参数设定
def param_index(param, index_code, factor):
    param.stock_pool_list = index_code
    param.file_index_close = os.path.join(
        param.file_index, index_code[:6] + "_close.csv"
    )
    param.file_index_open = os.path.join(param.file_index, index_code[:6] + "_open.csv")
    param.file_index_constituent = os.path.join(
        param.file_index, index_code[:6] + "_component_weight.csv"
    )
    if index_code in ["000300.SH"]:
        num_group = 5
    elif factor == "EARNYILD" and index_code == "000905.SH":
        num_group = 5
    else:
        num_group = 10
    param.group_num = num_group  # 调整分组组数
    return param


def factor_pool_test(args):

    (
        factor,
        factor_neut,
        trade_days,
        stock_return,
        industry_code_df,
        drop_industry,
        index_code,
        param,
    ) = args
    drop_industry: list = drop_industry
    index_code: str = index_code
    param = param_index(param, index_code, factor)
    df_neut = factor_neut[
        ["date", "next_date", "code", "stock_return", "%s_neut" % factor]
    ]
    # print("df_neut:", df_neut)
    # print(
    #     "df_neut.date.min().strftime('%Y-%m-%d'):",
    #     df_neut.date.min().strftime("%Y-%m-%d"),
    # )
    # print(
    #     "df_neut.date.max().strftime('%Y-%m-%d'):",
    #     df_neut.date.max().strftime("%Y-%m-%d"),
    # )
    factordata_pool_neut, coverage = stock_tool.get_pool_stock(
        df_neut.copy(),
        index_code,
        df_neut.date.min().strftime("%Y-%m-%d"),
        df_neut.date.max().strftime("%Y-%m-%d"),
        pool=None,
        file_pool=param.file_index,
        if_coverage=True,
        factor_name="%s" % factor,
    )

    coverage_thresh = min(
        coverage["rate_neut"].mean() - 2 * coverage["rate_neut"].std(),
        param.coverage_thresh,
    )
    start_date = coverage[coverage["rate_neut"] >= coverage_thresh].index.min()
    factordata_pool_neut = factordata_pool_neut[
        factordata_pool_neut["date"] >= start_date
    ]  # 只取覆盖率较高的数据
    # start_date = coverage[coverage['rate'] >= param.coverage_thresh].index.min()
    # factordata_pool_neut = factordata_pool_neut[factordata_pool_neut['date'] >= start_date]  # 只取覆盖率较高的数据

    # 2 去除金融行业
    factordata_pool_neut = stock_tool.drop_industry_stock(
        factordata_pool_neut,
        param,
        industry=drop_industry,
        industry_code_df=industry_code_df.copy(),
    )
    factordata_pool_neut = factordata_pool_neut.rename(
        columns={factor + "_neut": factor}
    )

    # 3 计算IC
    rank_IC_neut = cal_rankIC(factordata_pool_neut, [factor])

    # 4 分组收益，统计中性化分组收益
    return_df_neut, stats_info_neut = long_short_test(
        factordata_pool_neut,
        factor,
        param,
        trade_days,
        index_code=index_code,
        file_index_open=param.file_index_open,
    )

    # 5 汇总
    multiindex_single_idx = pd.MultiIndex.from_tuples(
        [
            ("因子", "因子名"),
            ("因子", "分类"),
            ("因子", "方向"),
            ("覆盖度", "均值"),
            ("RankIC", "均值"),
            ("RankIC", "IC_IR"),
            ("RankIC", "t值"),
            ("RankIC", "p值"),
            ("RankIC", "正显著比例 (2%)"),
            ("RankIC", "不显著比例"),
            ("RankIC", "负显著比例 (-2%)"),
            ("RankIC", "方向延续概率"),
            ("多空组合", "年化收益"),
            ("多空组合", "收益率t值"),
            ("多空组合", "夏普比"),
            ("多空组合", "胜率"),
            ("多空组合", "最大回撤"),
            ("多头超额", "年化超额"),
            ("多头超额", "超额t值"),
            ("多头超额", "夏普比"),
            ("多头超额", "胜率"),
            ("多头超额", "最大回撤"),
        ]
    )

    result_df = pd.DataFrame(index=multiindex_single_idx, columns=[factor])
    if factor in param.barra_factor:
        factor_category = "Barra"
    elif factor in param.tech_facotr:
        factor_category = "技术面"
    elif factor in param.fin_factor:
        factor_category = "基本面"
    else:
        factor_category = np.nan

    if_positive = param.factor_direction_dict[factor] == 1  # 确定因子方向
    if if_positive:
        factor_direction = "正向"
    else:
        factor_direction = "负向"
    result_df[factor] = [
        factor,
        factor_category,
        factor_direction,
        str(round(coverage[coverage.index > start_date]["rate_neut"].mean() * 100, 2))
        + "%",
        str(round(rank_IC_neut[factor].mean() * 100, 2)) + "%",
        round(rank_IC_neut[factor].mean() / rank_IC_neut[factor].std(), 2),
        round(
            stats.ttest_1samp(rank_IC_neut[factor], 0, nan_policy="omit").statistic, 2
        ),
        round(stats.ttest_1samp(rank_IC_neut[factor], 0, nan_policy="omit").pvalue, 4),
        str(round((rank_IC_neut[factor] > 0.02).mean() * 100, 2)) + "%",
        str(
            round(
                ((rank_IC_neut[factor] < 0.02) & (rank_IC_neut[factor] > -0.02)).mean()
                * 100,
                2,
            )
        )
        + "%",
        str(round((rank_IC_neut[factor] < -0.02).mean() * 100, 2)) + "%",
        str(
            round(
                (rank_IC_neut[factor] * rank_IC_neut[factor].shift(-1) > 0).mean()
                * 100,
                2,
            )
        )
        + "%",
        str(round(stats_info_neut.loc["多空", "组合年化收益率(%)"], 2)) + "%",
        str(round(stats_info_neut.loc["多空", "收益率t值"], 2)),
        str(round(stats_info_neut.loc["多空", "组合夏普比率"], 2)),
        str(round(stats_info_neut.loc["多空", "组合胜率(%)"], 2)) + "%",
        str(round(stats_info_neut.loc["多空", "组合第一大回撤率(%)"], 2)) + "%",
        str(round(stats_info_neut.loc["相对收益", "组合年化收益率(%)"], 2)) + "%",
        str(round(stats_info_neut.loc["相对收益", "收益率t值"], 2)),
        str(round(stats_info_neut.loc["相对收益", "组合夏普比率"], 2)),
        str(round(stats_info_neut.loc["相对收益", "组合胜率(%)"], 2)) + "%",
        str(round(stats_info_neut.loc["相对收益", "组合第一大回撤率(%)"], 2)) + "%",
    ]
    result_df = result_df.T
    # 6.1 保存中性化结果
    show_return_result_ls(return_df_neut["多空"], factor, param, num="多空收益")
    show_return_result_ls(return_df_neut["相对收益"], factor, param, num="相对收益")
    return result_df
