import datetime as dt
import pandas as pd

from Config.config import DATA_PATH
from data_utils.data_files import TRADE_DATES


def get_trade_dates(resample=None):
    from dateutil.relativedelta import relativedelta
    import os
    import pickle

    end = dt.datetime.today()
    # 因为17:00前部分数据没有更新可能会导致后续计算错误，所以取前一天
    if end.hour < 16:
        end = end - relativedelta(days=1)

    date_file_path = os.path.join(DATA_PATH, TRADE_DATES)
    with open(date_file_path, "rb") as f:
        trade_dates = pickle.load(f)
        start = pd.Series(trade_dates).max()

    # 如果没更新trade_dates更新一下
    if start < end:
        from WindPy import w

        w.start()
        start = start.strftime("%Y-%m-%d")
        end = end.strftime("%Y-%m-%d")
        data = w.tdays(start, end, "").Data[0]
        data = [pd.to_datetime(d) for d in data]
        with open(date_file_path, "rb") as f:
            trade_dates = pickle.load(f)
        with open(date_file_path, "wb") as f:
            s = pd.Series(trade_dates + data).drop_duplicates().sort_values()
            pickle.dump(list(s), f)
        w.close()

    trade_dates = [d for d in trade_dates]
    ds_dates = pd.Series(trade_dates, index=trade_dates)
    if resample == "month_end":
        trade_dates = (
            ds_dates.groupby([ds_dates.index.year, ds_dates.index.month])
            .last()
            .tolist()
        )
    elif resample == "week_end":
        trade_dates = (
            ds_dates.groupby([ds_dates.index.year, ds_dates.index.week]).last().tolist()
        )
    elif resample == "week_start":
        trade_dates = (
            ds_dates.groupby([ds_dates.index.year, ds_dates.index.week])
            .first()
            .tolist()
        )
    return trade_dates


def format_bgn_date_for_wind(bgn_date):
    if type(bgn_date) == str:
        return bgn_date.replace("-", "")
    elif type(bgn_date) == pd.Timestamp or type(bgn_date) == dt.datetime:
        return bgn_date.strftime("%Y%m%d")
    return bgn_date


def format_bgn_date_for_dfcf(bgn_date):
    if type(bgn_date) == pd.Timestamp or type(bgn_date) == dt.datetime:
        return bgn_date.strftime("%Y-%m-%d")
    return bgn_date


def get_last_reportdate(report_dt):
    if report_dt.month == 3:
        return report_dt.replace(year=report_dt.year - 1, month=12, day=31)
    elif report_dt.month == 6:
        return report_dt.replace(month=3, day=31)
    elif report_dt.month == 9:
        return report_dt.replace(month=6, day=30)
    elif report_dt.month == 12:
        return report_dt.replace(month=9, day=30)
    else:
        return


def remove_exchange(s):
    s = s.str.replace(".SZ", "")
    s = s.str.replace(".SH", "")
    return s


def resample_to_monthend(s, keep_report_dt_col=False):

    trade_dates = get_trade_dates(resample="month_end")
    col_name = s.name
    info_cols = s.index.names
    code_col = info_cols[0]
    dt_col = info_cols[1]

    df = s.reset_index()

    pivot_data = df.pivot(columns=code_col, index=dt_col).sort_index()
    idx = pivot_data.index
    idx = idx.union(trade_dates)
    pivot_data = pivot_data.reindex(idx)

    pivot_data = pivot_data.resample("1D").last().fillna(method="ffill")
    pivot_data = pivot_data.loc[trade_dates]
    if keep_report_dt_col:
        return pd.Series(
            pivot_data.stack().reset_index().set_index(info_cols)[col_name],
            name=col_name,
        )
    else:
        return pd.Series(pivot_data.stack().swaplevel(0, 1)[col_name], name=col_name)


def resample_to_weekend(s, keep_report_dt_col=False):

    trade_dates = get_trade_dates(resample="week_end")
    col_name = s.name
    info_cols = s.index.names
    code_col = info_cols[0]
    dt_col = info_cols[1]

    df = s.reset_index()

    pivot_data = df.pivot(columns=code_col, index=dt_col).sort_index()
    idx = pivot_data.index
    idx = idx.union(trade_dates)
    pivot_data = pivot_data.reindex(idx)

    pivot_data = pivot_data.resample("1D").last().fillna(method="ffill")
    pivot_data = pivot_data.loc[trade_dates]
    if keep_report_dt_col:
        return pd.Series(
            pivot_data.stack().reset_index().set_index(info_cols)[col_name],
            name=col_name,
        )
    else:
        return pd.Series(pivot_data.stack().swaplevel(0, 1)[col_name], name=col_name)


# 将dataframe中多列columns中noticedate列变为月底，并和其他列拼接起来
def get_weekend_multicolumns(df, *columns):
    df_all = pd.DataFrame()
    for col in columns:
        df = df.sort_values(["securitycode", "noticedate", "reportdate"])
        s_col = df.set_index(["securitycode", "noticedate"])[col]
        s_col = s_col[~s_col.index.duplicated(keep="last")]
        df_all[s_col.name] = resample_to_weekend(s_col)
    return df_all.reset_index()


# 将dataframe中多列columns中noticedate列变为月底，并和其他列拼接起来
def get_monthend_multicolumns(df, *columns):
    df_all = pd.DataFrame()
    for col in columns:
        df = df.sort_values(["securitycode", "noticedate", "reportdate"])
        s_col = df.set_index(["securitycode", "noticedate"])[col]
        s_col = s_col[~s_col.index.duplicated(keep="last")]
        df_all[s_col.name] = resample_to_monthend(s_col)
    return df_all.reset_index()


def get_reportdate(trade_dt):
    if trade_dt.month == 12:
        return trade_dt.replace(year=trade_dt.year + 1, month=3, day=31)
    elif trade_dt.month == 3:
        return trade_dt.replace(month=6, day=30)
    elif trade_dt.month == 6:
        return trade_dt.replace(month=9, day=30)
    elif trade_dt.month == 9:
        return trade_dt.replace(month=12, day=31)
    else:
        return


def zscore(X):
    return (X - X.mean()) / X.std()
