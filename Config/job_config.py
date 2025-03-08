import os
import logging
from dataclasses import dataclass
from typing import List
from factor_analysis.data_io import get_industry_mapping
import pandas as pd

here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Project params
n_jobs = 3  # Number of process cores
end_date = "2022-12-30"  # must by trade date
factor_list = ["corr_model"]

# Default parameters, which should not be changed after the project is running, as modifying them may lead to missing previous values in the automatic update concatenation logic.
data_init_date = "2004-12-31"  # The start date for underlying data extraction is the last day of the previous year, as the data is pulled using a greater-than condition
factor_init_date = "2009-12-31"  # The start date for factor calculation is the last day of the previous year, as the data is pulled using a greater-than condition.
test_init_date = "2014-12-31"  # backtest start date

database_path = "Database/Origin_DataBase"  # raw data
neut_data = "Database/Neut_DataBase"  # Neutralized data

# Core files
stock_price_data = "stock_price.pkl"
barra_factor_data = "barra_factor.pkl"
industry_cate_data = "industry_cate.pkl"


ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT = "findata_service"
ROOT_DIR = ROOT_DIR[: (ROOT_DIR.find(PROJECT) + len(PROJECT))]
FailureRetryTimes = 5
LOG_FILE_NAME = os.path.join(ROOT_DIR, "log", "findata_service.log")
LOG_FORMAT = "%(asctime)s %(levelname)s %(funcName)s(%(lineno)d): %(message)s"
loggingConfig = {
    "level": logging.INFO,
    "format": LOG_FORMAT,
    "datefmt": "%Y-%m-%d %H:%M:%S",
    "filename": LOG_FILE_NAME,
    "filemode": "a",
}


@dataclass
class FactorPara:
    start_date: str = ""
    end_date: str = ""
    barra_factor = [
        "SIZE",
        "MOMENTUM",
        "RESVOL",
        "SIZENL",
        "BTOP",
        "LIQUIDTY",
        "EARNYILD",
    ]
    fin_factor = ["xsmll_indicator"]
    tech_factor = ["hf_skew"]
    pool_thresh = {
        "Barra": {
            "IC": 0.01,
            "t_value": 1.96,
            "direction_rate": 0.5,
            "win_rate": 0.5,
            "ARR": 0.03,
            "sharp": 0.6,
        },
        "技术面": {
            "IC": 0.03,
            "t_value": 2.58,
            "direction_rate": 0.5,
            "win_rate": 0.5,
            "ARR": 0.15,
            "sharp": 1.2,
        },
        "基本面": {
            "IC": 0.01,
            "t_value": 1.64,
            "direction_rate": 0.5,
            "win_rate": 0.4,
            "ARR": 0.08,
            "sharp": 0.3,
        },
    }
    stock_pool_list = []
    file_index = os.path.join(here, r"Database/files")
    file_index_close = ""
    file_index_open = ""
    file_index_constituent = ""
    group_num = 0
    file_trade_day = os.path.join(here, r"Database/files/DailyTradeDays.csv")
    factor_list = ["bbi", "fin"]
    coverage_thresh = 0.7
    industry_mapping = get_industry_mapping()
    frequency = "w"
    fin_factor = ["fin_neut"]
    tech_facotr = ["bbi_neut"]
    barra_factor = [
        "SIZE",
    ]
    pool_test = "pool_file/"
    factor_direction_dict = {
        "bbi": -1,
        "fin": 1,
        "corr_model": -1,
        "PEG": -1,
        "CFR": 1,
        "SIZE": -1,
        "BETA": -1,
        "MOMENTUM": -1,
        "RESVOL": -1,
        "SIZENL": -1,
        "BTOP": 1,
        "LIQUIDTY": -1,
        "EARNYILD": 1,
        "GROWTH": -1,
        "LEVERAGE": 1,
        "HoldPER": 1,
        "DHoldPER": 1,
        "MHoldPER": 1,
        "QES": 1,
        "xsmll_indicator": 1,
        "sumasset_mll_indicator": 1,
        "SUE": 1,
        "roet_indicator": 1,
        "roe_indicator": 1,
        "roe_resid_V1": 1,
        "roe_resid_V2": 1,
        "jor": 1,
        "hf_skew": -1,
        "hf_adj_reversal": -1,
        "hf_vol_ratio": -1,
        "BBI": -1,
        "ROC_3": -1,
        "ROC_20": -1,
        "BIAS_5": -1,
        "BIAS_22": -1,
        "BIAS_3_6": 1,
        "BIAS_11_22": 1,
        "RSI": -1,
        "CMO": -1,
        "BOLL": -1,
        "TTM_FEP": 1,
        "FROE_CHANGE_1M": 1,
        "FROE_CHANGE_3M": 1,
        "YOY_Quart_NP": 1,
        "YOY_Quart_OR": 1,
        "YOY_Quart_OP": 1,
        "Quart_ROE": 1,
        "Quart_ROA": 1,
        "TTM_ROE": 1,
        "TTM_ROA": 1,
        "ORGAN_NUM_1M": 1,
        "ORGAN_NUM_3M": 1,
        "Quart_EP": 1,
        "Quart_SP": 1,
        "TTM_EP": 1,
        "TTM_SP": 1,
        "TTM_EP_PCT_1Y": -1,
        "Delta_ROE": 1,
        "Delta_ROA": 1,
        "UD_PCT": 1,
        "SUR": 1,
        "factors(equal)": 1,
        "factors(ic)": 1,
        "factors(icir)": 1,
        "factors(ortho)": 1,
        "alpha_001": -1,
        "alpha_002": -1,
        "alpha_003": -1,
        "alpha_004": -1,
        "alpha_005": -1,
        "alpha_006": -1,
        "alpha_007": 1,
        "alpha_008": 1,
        "alpha_009": -1,
        "alpha_010": -1,
        "alpha_011": -1,
        "alpha_012": 1,
        "alpha_013": 1,
        "alpha_014": -1,
        "alpha_015": 1,
        "alpha_016": 1,
        "alpha_017": 1,
        "alpha_018": -1,
        "alpha_019": -1,
        "alpha_020": -1,
        "alpha_021": -1,
        "alpha_022": -1,
        "alpha_023": -1,
        "alpha_024": -1,
        "alpha_025": -1,
        "alpha_026": 1,
        "alpha_027": -1,
        "alpha_028": -1,
        "alpha_029": -1,
        "alpha_031": -1,
        "alpha_032": 1,
        "alpha_033": 1,
        "alpha_034": 1,
        "alpha_035": 1,
        "alpha_036": -1,
        "alpha_037": 1,
        "alpha_038": 1,
        "alpha_039": -1,
        "alpha_040": -1,
        "alpha_041": 1,
        "alpha_042": 1,
        "alpha_043": -1,
        "alpha_044": -1,
        "alpha_045": -1,
        "alpha_046": 1,
        "alpha_047": 1,
        "alpha_048": 1,
        "alpha_049": 1,
        "alpha_052": -1,
        "alpha_053": -1,
        "alpha_054": -1,
        "alpha_056": 1,
        "alpha_057": -1,
        "alpha_058": -1,
        "alpha_059": -1,
        "alpha_060": -1,
        "alpha_061": -1,
        "alpha_062": 1,
        "alpha_063": -1,
        "alpha_064": -1,
        "alpha_065": 1,
        "alpha_066": -1,
        "alpha_067": -1,
        "alpha_068": 1,
        "alpha_069": -1,
        "alpha_070": -1,
        "alpha_071": -1,
        "alpha_072": 1,
        "alpha_074": -1,
        "alpha_076": -1,
        "alpha_077": 1,
        "alpha_078": -1,
        "alpha_079": -1,
        "alpha_080": -1,
        "alpha_081": -1,
        "alpha_082": 1,
        "alpha_083": 1,
        "alpha_084": -1,
        "alpha_085": 1,
        "alpha_086": 1,
        "alpha_087": -1,
        "alpha_088": -1,
        "alpha_089": -1,
        "alpha_090": 1,
        "alpha_091": 1,
        "alpha_092": -1,
        "alpha_093": -1,
        "alpha_094": -1,
        "alpha_095": -1,
        "alpha_096": -1,
        "alpha_097": -1,
        "alpha_098": 1,
        "alpha_099": 1,
        "alpha_100": -1,
        "alpha_101": 1,
        "alpha_102": -1,
        "alpha_103": 1,
        "alpha_104": 1,
        "alpha_105": 1,
        "alpha_106": -1,
        "alpha_107": -1,
        "alpha_108": -1,
        "alpha_109": -1,
        "alpha_110": 1,
        "alpha_111": -1,
        "alpha_112": -1,
        "alpha_113": 1,
        "alpha_114": -1,
        "alpha_115": -1,
        "alpha_116": -1,
        "alpha_117": 1,
        "alpha_118": -1,
        "alpha_119": -1,
        "alpha_120": 1,
        "alpha_121": 1,
        "alpha_122": -1,
        "alpha_123": 1,
        "alpha_124": -1,
        "alpha_125": 1,
        "alpha_126": -1,
        "alpha_127": -1,
        "alpha_128": 1,
        "alpha_129": -1,
        "alpha_130": 1,
        "alpha_131": -1,
        "alpha_132": -1,
        "alpha_133": -1,
        "alpha_134": -1,
        "alpha_135": -1,
        "alpha_136": 1,
        "alpha_138": -1,
        "alpha_139": 1,
        "alpha_140": -1,
        "alpha_141": -1,
        "alpha_142": 1,
        "alpha_144": 1,
        "alpha_145": -1,
        "alpha_148": 1,
        "alpha_150": -1,
        "alpha_152": -1,
        "alpha_153": -1,
        "alpha_154": 1,
        "alpha_155": -1,
        "alpha_156": 1,
        "alpha_157": 1,
        "alpha_158": -1,
        "alpha_159": -1,
        "alpha_160": -1,
        "alpha_161": -1,
        "alpha_162": -1,
        "alpha_163": -1,
        "alpha_164": 1,
        "alpha_167": -1,
        "alpha_168": 1,
        "alpha_169": -1,
        "alpha_170": 1,
        "alpha_171": -1,
        "alpha_172": -1,
        "alpha_173": -1,
        "alpha_174": -1,
        "alpha_175": -1,
        "alpha_176": -1,
        "alpha_177": -1,
        "alpha_178": -1,
        "alpha_179": -1,
        "alpha_180": 1,
        "alpha_184": 1,
        "alpha_185": 1,
        "alpha_186": -1,
        "alpha_187": -1,
        "alpha_188": 1,
        "alpha_189": -1,
        "alpha_191": 1,
    }  # Factor direction, where 1 indicates a positive direction
