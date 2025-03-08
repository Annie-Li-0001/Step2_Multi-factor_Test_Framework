import pandas as pd


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
