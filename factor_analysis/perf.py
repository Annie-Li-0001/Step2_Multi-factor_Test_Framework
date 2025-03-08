from scipy import stats
import numpy as np
import pandas as pd


# 分析组合周期收益指标
def analyze_empyrical(return_period, detail, type_period="monthly"):
    import empyrical

    # 计算组合收益指标
    port_cumreturn = empyrical.cum_returns(return_period).tolist()  # 累计收益
    port_mean = empyrical.annual_return(return_period, period=type_period)  # 年化收益
    port_std = empyrical.annual_volatility(
        return_period, period=type_period
    )  # 年化波动
    port_dd = empyrical.downside_risk(return_period)
    if (return_period == 0).sum() != len(return_period):
        port_sharpe = empyrical.sharpe_ratio(
            return_period, period=type_period
        )  # 夏普比率
        port_sortino = empyrical.sortino_ratio(
            return_period, period=type_period
        )  # 索提诺比率
    else:
        port_sharpe = np.nan
        port_sortino = np.nan
    port_sdr = port_sortino / (np.sqrt(2))

    port_mmd = empyrical.max_drawdown(return_period)  # 最大回撤

    port_ret_greater = np.sum(return_period > 0) / len(return_period)  # 胜率
    if np.sum(return_period[return_period < 0]) != 0:
        port_ret_pl = np.abs(
            np.sum(return_period[return_period > 0])
            / np.sum(return_period[return_period < 0])
        )  # 盈亏比
    else:
        port_ret_pl = np.nan

    row_content = [
        detail,
        port_cumreturn[-1],
        port_mean,
        port_sharpe,
        port_sortino,
        port_mmd,
        port_ret_greater,
        port_ret_pl,
    ]
    return row_content


def get_accreturn_series_drawdown(acc_result):
    acc_result["max2here"] = acc_result.iloc[:, 0].expanding().max()
    acc_result["dd2here"] = acc_result.iloc[:, 0] / acc_result.max2here
    remains = acc_result.sort_values(by=["dd2here"]).iloc[0].dd2here
    drawdown = round((1 - remains) * 100, 2)
    end_time = acc_result.sort_values(by=["dd2here"]).iloc[0].name
    start_time = (
        acc_result[acc_result.index <= end_time]
        .sort_values(by=acc_result.columns[0], ascending=False)
        .iloc[0]
        .name
    )

    return drawdown, start_time, end_time


# 对一个收益率时间序列进行收益率统计，包括年化收益波动率夏普比率最大回撤多空胜率等
def stats_return_time_series_result(result, param, group_name, last_day):
    """
    @param result: 索引为时间,bench_ret列为指数收益率列，均为各期收益率不是累计收益率
    @param param:
    @param group_name: 列为分组列(命名为group_name)
    @return:
    """

    if len(result.columns) > 1:
        result["相对收益率"] = result[group_name] - result["bench_ret"]
    else:
        result["相对收益率"] = result[group_name].copy()

    # 收益统计
    type_period = "weekly" if param.frequency == "w" else "monthly"
    pfm = analyze_empyrical(result[group_name], "", type_period=type_period)
    pfm_rlt = analyze_empyrical(result["相对收益率"], "", type_period=type_period)
    pfm_all = pd.DataFrame(
        {group_name: pfm, "相对收益率": pfm_rlt},
        index=[
            "detail",
            "cum_return",
            "annual_return",
            "sharpe_ratio",
            "sortino_ratio",
            "max_drawdown",
            "win_rate",
            "pnl_ratio",
        ],
    )

    # 最大回撤，开始时间，结束时间
    acc_result = result.add(1).cumprod()
    acc_result.loc[last_day, :] = np.nan
    acc_result = acc_result.shift(1).fillna(1)
    drawdown1, start_time1, end_time1 = get_accreturn_series_drawdown(acc_result)
    drawdown2_1, start_time2_1, end_time2_1 = get_accreturn_series_drawdown(
        pd.DataFrame(acc_result[:start_time1])
    )
    drawdown2_2, start_time2_2, end_time2_2 = get_accreturn_series_drawdown(
        pd.DataFrame(acc_result[end_time1:])
    )
    drawdown3_1, start_time3_1, end_time3_1 = get_accreturn_series_drawdown(
        pd.DataFrame(acc_result[:start_time2_1])
    )
    drawdown3_2, start_time3_2, end_time3_2 = get_accreturn_series_drawdown(
        pd.DataFrame(acc_result[end_time2_1:start_time1])
    )
    drawdown3_3, start_time3_3, end_time3_3 = get_accreturn_series_drawdown(
        pd.DataFrame(acc_result[end_time1:start_time2_2])
    )
    drawdown3_4, start_time3_4, end_time3_4 = get_accreturn_series_drawdown(
        pd.DataFrame(acc_result[end_time2_2:])
    )
    drawdown_list = pd.DataFrame(
        [
            [drawdown1, start_time1, end_time1],
            [drawdown2_1, start_time2_1, end_time2_1],
            [drawdown2_2, start_time2_2, end_time2_2],
            [drawdown3_1, start_time3_1, end_time3_1],
            [drawdown3_2, start_time3_2, end_time3_2],
            [drawdown3_3, start_time3_3, end_time3_3],
            [drawdown3_4, start_time3_4, end_time3_4],
        ]
    )
    index_nodrawdown = np.where(drawdown_list[0] == 0)[0]
    drawdown_list.iloc[index_nodrawdown, 1] = np.nan
    drawdown_list.iloc[index_nodrawdown, 2] = np.nan
    drawdown_list = drawdown_list.sort_values(by=0, ascending=False)
    if not all(result["相对收益率"].dropna() == 0):
        # 相对收益率t值
        rela_rtn_t = stats.ttest_1samp(
            result["相对收益率"], 0, nan_policy="omit"
        ).statistic
        # 相对收益率最大回撤
        drawdown_rela, start_time_rela, end_time_rela = get_accreturn_series_drawdown(
            acc_result[["相对收益率"]].copy()
        )
    else:
        rela_rtn_t = np.nan
        drawdown_rela, start_time_rela, end_time_rela = (
            0,
            acc_result.index[0],
            acc_result.index[0],
        )
    # 胜率
    if len(result.columns) > 1:
        # TODO: 胜率是相对全市场收益率还是指数收益率
        if group_name in ("多空", "相对收益"):
            win_rate = len(result[result.iloc[:, 0] > 0]) / len(result)
            win_rate = round(win_rate * 100, 2)
        else:
            win_rate = len(result[result.iloc[:, 0] > result["bench_ret"]]) / len(
                result
            )
            win_rate = round(win_rate * 100, 2)
        stats_result = pd.DataFrame(
            data={
                "组合收益率(%)": pfm_all.loc["cum_return", group_name] * 100,
                "组合年化收益率(%)": pfm_all.loc["annual_return", group_name] * 100,
                "组合年化波动率(%)": pfm_all.loc["annual_return", group_name]
                / pfm_all.loc["sharpe_ratio", group_name]
                * 100,
                "组合夏普比率": pfm_all.loc["sharpe_ratio", group_name],
                "组合胜率(%)": win_rate,
                "组合年化相对收益率(%)": pfm_all.loc["annual_return", "相对收益率"]
                * 100,
                "信息比率": pfm_all.loc["sharpe_ratio", "相对收益率"],
                "相对收益t值": rela_rtn_t,
                "组合第一大回撤率(%)": drawdown_list.iloc[0, 0],
                "组合第一大回撤开始时间": drawdown_list.iloc[0, 1],
                "组合第一大回撤结束时间": drawdown_list.iloc[0, 2],
                "组合第二大回撤率(%)": drawdown_list.iloc[1, 0],
                "组合第二大回撤开始时间": drawdown_list.iloc[1, 1],
                "组合第二大回撤结束时间": drawdown_list.iloc[1, 2],
                "组合第三大回撤率(%)": drawdown_list.iloc[2, 0],
                "组合第三大回撤开始时间": drawdown_list.iloc[2, 1],
                "组合第三大回撤结束时间": drawdown_list.iloc[2, 2],
                "最大对冲回撤率(%)": drawdown_rela,
                "最大对冲回撤开始时间": start_time_rela,
                "最大对冲回撤结束时间": end_time_rela,
            },
            index=[group_name],
        )
    else:
        stats_result = pd.DataFrame(
            data={
                "组合收益率(%)": pfm_all.loc["cum_return", group_name] * 100,
                "组合年化收益率(%)": pfm_all.loc["annual_return", group_name] * 100,
                "组合年化波动率(%)": pfm_all.loc["annual_return", group_name]
                / pfm_all.loc["sharpe_ratio", group_name]
                * 100,
                "组合夏普比率": pfm_all.loc["sharpe_ratio", group_name],
                "组合第一大回撤率(%)": drawdown_list.iloc[0, 0],
                "组合第一大回撤开始时间": drawdown_list.iloc[0, 1],
                "组合第一大回撤结束时间": drawdown_list.iloc[0, 2],
                "组合第二大回撤率(%)": drawdown_list.iloc[1, 0],
                "组合第二大回撤开始时间": drawdown_list.iloc[1, 1],
                "组合第二大回撤结束时间": drawdown_list.iloc[1, 2],
                "组合第三大回撤率(%)": drawdown_list.iloc[2, 0],
                "组合第三大回撤开始时间": drawdown_list.iloc[2, 1],
                "组合第三大回撤结束时间": drawdown_list.iloc[2, 2],
            },
            index=[group_name],
        )
    return stats_result
