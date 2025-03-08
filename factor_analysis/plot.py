import os.path

import pandas as pd
import datetime as dt
from os import mkdir
from docx.enum.text import WD_PARAGRAPH_ALIGNMENT
from docx.shared import Inches
import seaborn as sns
import matplotlib

matplotlib.use("Agg")  # 不显示图片
# use命令：用来配置matplotlib的backend（后端）
# Agg - 用户接口典型的渲染器，使用Anti-Grain Geometry C++库产生光栅像素图
#    - 非交互式后端，没有GUI界面，用来生成图像文件 （vs GTKAgg - 交互式后端，拥有在屏幕上展示的能力）
# 因此，line28 可用来设置不显示图片
import matplotlib.pyplot as plt


def show_return_result_ls(return_info, factor, param, num=""):
    """多空收益、多空净值、回撤、分组年化收益率作图

    :param return_info: 多空组合收益率
    :param stats_info: 分组收益率统计表
    :param factor: 因子名称
    :param param: Para()
    :param num: 输出文件名需要添加的附注
    :return:
    """

    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    from matplotlib.ticker import FuncFormatter
    import pylab

    pylab.mpl.rcParams["font.sans-serif"] = ["Microsoft Yahei"]

    def to_percent(temp, position):
        return "%10.0f" % temp + "%"

    start_date = return_info.index[0]
    end_date = return_info.index[-1]
    fig_size = (12, 4)

    # # 多空月收益条形图
    # fig1, ax1 = plt.subplots(figsize=fig_size)
    # bar1 = ax1.bar(return_info.index, return_info * 100, width=22, color='red', label='收益率')
    # ax1.set_xlim(left=dt.datetime(start_date.year, 3 * start_date.quarter - 2, 1),
    #              right=dt.datetime(end_date.year + (end_date.month // 10), (3 * end_date.quarter + 1) % 12, 1))
    # ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
    # plt.setp(ax1.get_xticklabels(), rotation=90, ha="center")
    # # ax1.legend(loc='upper center', bbox_to_anchor=(0.5, 0.95), frameon=False)
    # ax1.set_title(factor + '因子的收益曲线（%s）'%num)
    #
    # plt.gca().yaxis.set_major_formatter(FuncFormatter(to_percent))
    # plt.axhline(y=0, color='black', linewidth=0.5)

    # plt.savefig(file_name + factor + num + '_多空月收益' + ' (股票池_' +
    #             param.index_code_dict[param.stock_pool_list][0]
    #             + '_调仓频率_' + param.frequency + ').png', dpi=150, bbox_inches="tight", pad_inches=0.2)
    # plt.close(fig1)

    # 多空净值及回撤图
    cum_return = return_info.fillna(0)
    cum_return = cum_return.add(1).cumprod().shift(1).fillna(1)  # 多空净值
    drawdown = pd.DataFrame()
    drawdown["cum_return"] = cum_return
    drawdown["max2here"] = drawdown["cum_return"].expanding().max()
    drawdown["dd2here"] = (
        drawdown["cum_return"] / drawdown["max2here"] - 1
    ) * 100  # 回撤

    fig2, ax2 = plt.subplots(figsize=fig_size)
    ax2_1 = ax2.twinx()
    ax2_1.yaxis.tick_right()
    ax2_1.yaxis.set_label_position("right")

    ax2_1.bar(
        drawdown.index,
        drawdown["dd2here"],
        width=10,
        alpha=0.5,
        edgecolor="None",
        label="月度回撤（右）",
    )
    plt.gca().yaxis.set_major_formatter(FuncFormatter(to_percent))
    ax2.plot(cum_return, color="red", label="多空净值（左）")
    ax2.set_xlim(
        left=dt.datetime(start_date.year, 3 * start_date.quarter - 2, 1),
        right=dt.datetime(
            end_date.year + (end_date.month // 10), (3 * end_date.quarter + 1) % 12, 1
        ),
    )
    ax2.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
    plt.setp(ax2.get_xticklabels(), rotation=90, ha="center")
    # ax2.legend(loc='upper left', ncol=2, frameon=False)
    plt.title(factor + "因子净值曲线及回撤（%s）" % num)
    file_name = param.pool_test
    if not os.path.exists(file_name):
        mkdir(file_name)
    plt.savefig(
        file_name + factor + "_净值曲线及回撤（%s）" % num + ".png",
        dpi=150,
        bbox_inches="tight",
        pad_inches=0.2,
    )
    plt.close(fig2)


def draw_thermodynamic_diagram(data, doc, file_name, inch):
    plt.figure(figsize=(35, 20))
    sns.heatmap(
        data * 10,
        fmt=".0f",
        annot_kws={"size": 15},
        annot=True,
        vmin=0,
        vmax=10,
        cmap="Reds",
    )
    plt.tick_params(axis="x", which="both", labelsize=12)
    plt.tick_params(axis="y", which="both", labelsize=12)
    plt.savefig("pool_file/%s.png" % file_name, dpi=100)
    doc.add_picture("pool_file/%s.png" % file_name, width=Inches(inches=inch))
    doc.paragraphs[-1].alignment = WD_PARAGRAPH_ALIGNMENT.CENTER
