def mkdir(path):
    """
    :param path:
    :return:
    """
    # 引入模块
    import os

    isExists = os.path.exists(path)

    if not isExists:
        os.makedirs(path)

        print(path + " 创建成功")
        return True
    else:
        return False


# 申万行业代码与名称对应
def sw_ind_code_to_name(level_num=2):
    """

    :param level_num: 申万一级为2，申万二级为3，申万三级为4
    :return:
    str = ''
    for i in range(industry_sw2.shape[0]):
        str = str + "'" + industry_sw2.index[i] + "': '" + industry_sw2.iloc[i,0] + "', "
    """
    if level_num == 2:
        industry_dict = {
            "801010": "农林牧渔",
            "801020": "采掘",
            "801030": "基础化工",
            "801040": "钢铁",
            "801050": "有色金属",
            "801080": "电子",
            "801110": "家用电器",
            "801120": "食品饮料",
            "801130": "纺织服饰",
            "801140": "轻工制造",
            "801150": "医药生物",
            "801160": "公用事业",
            "801170": "交通运输",
            "801180": "房地产",
            "801200": "贸易零售",
            "801210": "社会服务",
            "801230": "综合",
            "801710": "建筑材料",
            "801720": "建筑装饰",
            "801730": "电力设备",
            "801740": "国防军工",
            "801750": "计算机",
            "801760": "传媒",
            "801770": "通信",
            "801780": "银行",
            "801790": "非银金融",
            "801880": "汽车",
            "801890": "机械设备",
            "801950": "煤炭",
            "801960": "石油石化",
            "801970": "环保",
            "801980": "美容护理",
        }
    elif level_num == 3:
        industry_dict = {
            "801011": "林业Ⅱ",
            "801012": "农产品加工",
            "801014": "饲料",
            "801015": "渔业",
            "801016": "种植业",
            "801017": "养殖业",
            "801018": "动物保健Ⅱ",
            "801032": "化学纤维",
            "801033": "化学原料",
            "801034": "化学制品",
            "801036": "塑料",
            "801037": "橡胶",
            "801038": "农化制品",
            "801039": "非金属材料Ⅱ",
            "801043": "冶钢原料",
            "801044": "普钢",
            "801045": "特钢Ⅱ",
            "801051": "金属新材料",
            "801053": "贵金属",
            "801054": "小金属",
            "801055": "工业金属",
            "801056": "能源金属",
            "801072": "通用设备",
            "801074": "专用设备",
            "801076": "轨交设备Ⅱ",
            "801077": "工程机械",
            "801078": "自动化设备",
            "801081": "半导体",
            "801082": "其他电子Ⅱ",
            "801083": "元件",
            "801084": "光学光电子",
            "801085": "消费电子",
            "801086": "电子化学品Ⅱ",
            "801092": "汽车服务",
            "801093": "汽车零部件",
            "801095": "乘用车",
            "801096": "商用车",
            "801101": "计算机设备",
            "801102": "通信设备",
            "801103": "IT服务Ⅱ",
            "801104": "软件开发",
            "801111": "白色家电",
            "801112": "黑色家电",
            "801113": "小家电",
            "801114": "厨卫电器",
            "801115": "照明设备Ⅱ",
            "801116": "家电零部件Ⅱ",
            "801124": "食品加工",
            "801125": "白酒Ⅱ",
            "801126": "非白酒",
            "801127": "饮料乳品",
            "801128": "休闲食品",
            "801129": "调味发酵品Ⅱ",
            "801131": "纺织制造",
            "801132": "服装家纺",
            "801133": "饰品",
            "801141": "包装印刷",
            "801142": "家居用品",
            "801143": "造纸",
            "801145": "文娱用品",
            "801151": "化学制药",
            "801152": "生物制品",
            "801153": "医疗器械",
            "801154": "医药商业",
            "801155": "中药Ⅱ",
            "801156": "医疗服务",
            "801161": "电力",
            "801163": "燃气Ⅱ",
            "801178": "物流",
            "801179": "铁路公路",
            "801181": "房地产开发",
            "801183": "房地产服务",
            "801191": "多元金融",
            "801193": "证券Ⅱ",
            "801194": "保险Ⅱ",
            "801202": "贸易Ⅱ",
            "801203": "一般零售",
            "801204": "专业连锁Ⅱ",
            "801206": "互联网电商",
            "801218": "专业服务",
            "801219": "酒店餐饮",
            "801223": "通信服务",
            "801231": "综合Ⅱ",
            "801711": "水泥",
            "801712": "玻璃玻纤",
            "801713": "装修建材",
            "801721": "房屋建设Ⅱ",
            "801722": "装修装饰Ⅱ",
            "801723": "基础建设",
            "801724": "专业工程",
            "801726": "工程咨询服务Ⅱ",
            "801731": "电机Ⅱ",
            "801733": "其他电源设备Ⅱ",
            "801735": "光伏设备",
            "801736": "风电设备",
            "801737": "电池",
            "801738": "电网设备",
            "801741": "航天装备Ⅱ",
            "801742": "航空装备Ⅱ",
            "801743": "地面兵装Ⅱ",
            "801744": "航海装备Ⅱ",
            "801745": "军工电子Ⅱ",
            "801764": "游戏Ⅱ",
            "801765": "广告营销",
            "801766": "影视院线",
            "801767": "数字媒体",
            "801769": "出版",
            "801782": "国有大型银行Ⅱ",
            "801783": "股份制银行Ⅱ",
            "801784": "城商行Ⅱ",
            "801785": "农商行Ⅱ",
            "801881": "摩托车及其他",
            "801951": "煤炭开采",
            "801952": "焦炭Ⅱ",
            "801962": "油服工程",
            "801963": "炼化及贸易",
            "801971": "环境治理",
            "801972": "环保设备Ⅱ",
            "801981": "个护用品",
            "801982": "化妆品",
            "801991": "航空机场",
            "801992": "航运港口",
            "801993": "旅游及景区",
            "801994": "教育",
            "801995": "电视广播Ⅱ",
        }
        # industry_dict = {'801011': '林业Ⅱ', '801012': '农产品加工', '801013': '农业综合Ⅱ', '801014': '饲料Ⅱ',
        #                  '801015': '渔业', '801016': '种植业', '801017': '养殖业', '801018': '动物保健Ⅱ',
        #                  '801021': '煤炭开采Ⅱ', '801022': '其他采掘Ⅱ', '801023': '石油开采Ⅱ', '801024': '采掘服务Ⅱ',
        #                  '801032': '化学纤维', '801033': '化学原料', '801034': '化学制品', '801035': '石油化工',
        #                  '801036': '塑料Ⅱ', '801037': '橡胶', '801041': '钢铁Ⅱ', '801051': '金属非金属新材料',
        #                  '801053': '黄金Ⅱ', '801054': '稀有金属', '801055': '工业金属', '801072': '通用机械',
        #                  '801073': '仪器仪表Ⅱ', '801074': '专用设备', '801075': '金属制品Ⅱ', '801076': '运输设备Ⅱ',
        #                  '801081': '半导体', '801082': '其他电子Ⅱ', '801083': '元件Ⅱ', '801084': '光学光电子',
        #                  '801085': '电子制造Ⅱ', '801092': '汽车服务Ⅱ', '801093': '汽车零部件Ⅱ', '801094': '汽车整车',
        #                  '801101': '计算机设备Ⅱ', '801102': '通信设备', '801111': '白色家电', '801112': '视听器材',
        #                  '801123': '饮料制造', '801124': '食品加工', '801131': '纺织制造', '801132': '服装家纺',
        #                  '801141': '包装印刷Ⅱ', '801142': '家用轻工', '801143': '造纸Ⅱ', '801144': '其他轻工制造Ⅱ',
        #                  '801151': '化学制药', '801152': '生物制品Ⅱ', '801153': '医疗器械Ⅱ', '801154': '医药商业Ⅱ',
        #                  '801155': '中药Ⅱ', '801156': '医疗服务Ⅱ', '801161': '电力', '801162': '环保工程及服务Ⅱ',
        #                  '801163': '燃气Ⅱ', '801164': '水务Ⅱ', '801171': '港口Ⅱ', '801172': '公交Ⅱ', '801173': '航空运输Ⅱ',
        #                  '801174': '机场Ⅱ', '801175': '高速公路Ⅱ', '801176': '航运Ⅱ', '801177': '铁路运输Ⅱ',
        #                  '801178': '物流Ⅱ', '801181': '房地产开发Ⅱ', '801182': '园区开发Ⅱ', '801191': '多元金融Ⅱ',
        #                  '801192': '银行Ⅱ', '801193': '券商Ⅱ', '801194': '保险Ⅱ', '801202': '贸易Ⅱ', '801203': '一般零售',
        #                  '801204': '专业零售', '801205': '商业物业经营', '801211': '餐饮Ⅱ', '801212': '景点',
        #                  '801213': '酒店Ⅱ', '801214': '旅游综合Ⅱ', '801215': '其他休闲服务Ⅱ', '801222': '计算机应用',
        #                  '801223': '通信运营Ⅱ', '801231': '综合Ⅱ', '801711': '水泥制造Ⅱ', '801712': '玻璃制造Ⅱ',
        #                  '801713': '其他建材Ⅱ', '801721': '房屋建设Ⅱ', '801722': '装修装饰Ⅱ', '801723': '基础建设',
        #                  '801724': '专业工程', '801725': '园林工程Ⅱ', '801731': '电机Ⅱ', '801732': '电气自动化设备',
        #                  '801733': '电源设备', '801734': '高低压设备', '801741': '航天装备Ⅱ', '801742': '航空装备Ⅱ',
        #                  '801743': '地面兵装Ⅱ', '801744': '船舶制造Ⅱ', '801751': '营销传播', '801752': '互联网传媒',
        #                  '801761': '文化传媒', '801881': '其他交运设备Ⅱ'}
    elif level_num == 4:
        industry_dict = {
            "850111": "种子",
            "850112": "粮食种植",
            "850113": "其他种植业",
            "850121": "海洋捕捞",
            "850122": "水产养殖",
            "850131": "林业Ⅲ",
            "850135": "食品及饲料添加剂",
            "850136": "有机硅",
            "850142": "畜禽饲料",
            "850151": "果蔬加工",
            "850152": "粮油加工",
            "850154": "其他农产品加工",
            "850172": "生猪养殖",
            "850173": "肉鸡养殖",
            "850181": "动物保健Ⅲ",
            "850232": "汽车经销商",
            "850233": "汽车综合服务",
            "850321": "纯碱",
            "850322": "氯碱",
            "850323": "无机盐",
            "850324": "其他化学原料",
            "850325": "煤化工",
            "850326": "钛白粉",
            "850331": "氮肥",
            "850332": "磷肥及磷化工",
            "850333": "农药",
            "850335": "涂料油墨",
            "850336": "钾肥",
            "850337": "民爆制品",
            "850338": "纺织化学制品",
            "850339": "其他化学制品",
            "850341": "涤纶",
            "850343": "粘胶",
            "850344": "其他化学纤维",
            "850345": "氨纶",
            "850351": "其他塑料制品",
            "850353": "改性塑料",
            "850354": "合成树脂",
            "850355": "膜材料",
            "850362": "其他橡胶制品",
            "850363": "炭黑",
            "850372": "聚氨酯",
            "850381": "复合肥",
            "850382": "氟化工",
            "850412": "特钢Ⅲ",
            "850442": "板材",
            "850521": "其他金属新材料",
            "850522": "磁性材料",
            "850523": "非金属材料Ⅲ",
            "850531": "黄金",
            "850541": "稀土",
            "850542": "钨",
            "850543": "锂",
            "850544": "其他小金属",
            "850551": "铝",
            "850552": "铜",
            "850553": "铅锌",
            "850614": "其他建材",
            "850615": "耐火材料",
            "850616": "管材",
            "850623": "房屋建设Ⅲ",
            "850702": "安防设备",
            "850703": "其他计算机设备",
            "850711": "机床工具",
            "850713": "磨具磨料",
            "850715": "制冷空调设备",
            "850716": "其他通用设备",
            "850721": "纺织服装设备",
            "850723": "农用机械",
            "850725": "能源及重型设备",
            "850726": "印刷包装机械",
            "850727": "其他专用设备",
            "850728": "楼宇设备",
            "850731": "仪器仪表",
            "850741": "电机Ⅲ",
            "850751": "金属制品",
            "850771": "工程机械整机",
            "850772": "工程机械器件",
            "850781": "机器人",
            "850782": "工控设备",
            "850783": "激光设备",
            "850784": "其他自动化设备",
            "850812": "分立器件",
            "850813": "半导体材料",
            "850814": "数字芯片设计",
            "850815": "模拟芯片设计",
            "850817": "集成电路封测",
            "850818": "半导体设备",
            "850822": "印制电路板",
            "850823": "被动元件",
            "850831": "面板",
            "850832": "LED",
            "850833": "光学元件",
            "850841": "其他电子Ⅲ",
            "850853": "品牌消费电子",
            "850854": "消费电子零部件及组装",
            "850861": "电子化学品Ⅲ",
            "850912": "商用载货车",
            "850913": "商用载客车",
            "850922": "车身附件及饰件",
            "850923": "底盘与发动机系统",
            "850924": "轮胎轮毂",
            "850925": "其他汽车零部件",
            "850926": "汽车电子电气系统",
            "850935": "航海装备Ⅲ",
            "850936": "轨交设备Ⅲ",
            "850952": "综合乘用车",
            "851024": "通信网络设备及器件",
            "851025": "通信线缆及配套",
            "851026": "通信终端及配件",
            "851027": "其他通信设备",
            "851041": "垂直应用软件",
            "851042": "横向通用软件",
            "851112": "空调",
            "851116": "冰洗",
            "851121": "彩电",
            "851122": "其他黑色家电",
            "851131": "厨房小家电",
            "851141": "厨房电器",
            "851151": "照明设备Ⅲ",
            "851161": "家电零部件Ⅲ",
            "851232": "啤酒",
            "851233": "其他酒类",
            "851241": "肉制品",
            "851242": "调味发酵品Ⅲ",
            "851243": "乳品",
            "851244": "其他食品",
            "851246": "预加工食品",
            "851247": "保健品",
            "851251": "白酒Ⅲ",
            "851271": "软饮料",
            "851281": "零食",
            "851282": "烘焙食品",
            "851312": "棉纺",
            "851314": "印染",
            "851315": "辅料",
            "851316": "其他纺织",
            "851325": "鞋帽及其他",
            "851326": "家纺",
            "851329": "非运动服装",
            "851331": "钟表珠宝",
            "851412": "大宗用纸",
            "851413": "特种纸",
            "851422": "印刷",
            "851423": "金属包装",
            "851424": "塑料包装",
            "851425": "纸包装",
            "851436": "瓷砖地板",
            "851437": "成品家居",
            "851438": "定制家居",
            "851439": "卫浴制品",
            "851452": "娱乐用品",
            "851491": "其他家居用品",
            "851511": "原料药",
            "851512": "化学制剂",
            "851521": "中药Ⅲ",
            "851522": "血液制品",
            "851523": "疫苗",
            "851524": "其他生物制品",
            "851532": "医疗设备",
            "851533": "医疗耗材",
            "851534": "体外诊断",
            "851542": "医药流通",
            "851543": "线下药店",
            "851563": "医疗研发外包",
            "851564": "医院",
            "851610": "电能综合服务",
            "851611": "火力发电",
            "851612": "水力发电",
            "851614": "热力服务",
            "851616": "光伏发电",
            "851617": "风力发电",
            "851631": "燃气Ⅲ",
            "851711": "港口",
            "851721": "公交",
            "851731": "高速公路",
            "851741": "航空运输",
            "851751": "机场",
            "851761": "航运",
            "851771": "铁路运输",
            "851782": "原材料供应链服务",
            "851783": "中间产品及消费品供应链服务",
            "851784": "快递",
            "851785": "跨境物流",
            "851786": "仓储物流",
            "851787": "公路货运",
            "851811": "住宅开发",
            "851812": "商业地产",
            "851813": "产业地产",
            "851831": "物业管理",
            "851922": "金融控股",
            "851927": "资产管理",
            "851931": "证券Ⅲ",
            "851941": "保险Ⅲ",
            "852021": "贸易Ⅲ",
            "852031": "百货",
            "852032": "超市",
            "852033": "多业态零售",
            "852034": "商业物业经营",
            "852041": "专业连锁Ⅲ",
            "852062": "跨境电商",
            "852063": "电商服务",
            "852111": "人工景区",
            "852112": "自然景区",
            "852121": "酒店",
            "852131": "旅游综合",
            "852141": "餐饮",
            "852182": "检测服务",
            "852183": "会展服务",
            "852213": "通信工程及服务",
            "852214": "通信应用增值服务",
            "852226": "IT服务Ⅲ",
            "852311": "综合Ⅲ",
            "857111": "水泥制造",
            "857112": "水泥制品",
            "857121": "玻璃制造",
            "857122": "玻纤制造",
            "857221": "装修装饰Ⅲ",
            "857236": "基建市政工程",
            "857241": "钢结构",
            "857242": "化学工程",
            "857243": "国际工程",
            "857244": "其他专业工程",
            "857251": "园林工程",
            "857261": "工程咨询服务Ⅲ",
            "857321": "电网自动化设备",
            "857323": "电工仪器仪表",
            "857331": "综合电力设备商",
            "857334": "火电设备",
            "857336": "其他电源设备Ⅲ",
            "857344": "线缆部件及其他",
            "857352": "光伏电池组件",
            "857354": "光伏辅材",
            "857355": "光伏加工设备",
            "857362": "风电零部件",
            "857371": "锂电池",
            "857372": "电池化学品",
            "857373": "锂电专用设备",
            "857375": "蓄电池及其他电池",
            "857381": "输变电设备",
            "857382": "配电设备",
            "857411": "航天装备Ⅲ",
            "857421": "航空装备Ⅲ",
            "857431": "地面兵装Ⅲ",
            "857451": "军工电子Ⅲ",
            "857641": "游戏Ⅲ",
            "857651": "营销代理",
            "857661": "影视动漫制作",
            "857674": "门户网站",
            "857691": "教育出版",
            "857692": "大众出版",
            "857821": "国有大型银行Ⅲ",
            "857831": "股份制银行Ⅲ",
            "857841": "城商行Ⅲ",
            "857851": "农商行Ⅲ",
            "858811": "其他运输设备",
            "858812": "摩托车",
            "859511": "动力煤",
            "859512": "焦煤",
            "859521": "焦炭Ⅲ",
            "859621": "油田服务",
            "859622": "油气及炼化工程",
            "859631": "炼油化工",
            "859632": "油品石化贸易",
            "859633": "其他石化",
            "859711": "大气治理",
            "859712": "水务及水治理",
            "859713": "固废治理",
            "859714": "综合环境治理",
            "859721": "环保设备Ⅲ",
            "859811": "生活用纸",
            "859821": "化妆品制造及其他",
            "859822": "品牌化妆品",
            "859852": "培训教育",
            "859951": "电视广播Ⅲ",
        }

        # industry_dict = {'850111': '种子生产', '850112': '粮食种植', '850113': '其他种植业', '850121': '海洋捕捞',
        #                  '850122': '水产养殖', '850131': '林业Ⅲ', '850141': '饲料Ⅲ', '850151': '果蔬加工',
        #                  '850152': '粮油加工', '850154': '其他农产品加工', '850161': '农业综合Ⅲ', '850171': '畜禽养殖Ⅲ',
        #                  '850181': '动物保健Ⅲ', '850211': '石油开采Ⅲ', '850221': '煤炭开采Ⅲ', '850222': '焦炭加工',
        #                  '850231': '其他采掘Ⅲ', '850241': '油气钻采服务', '850242': '其他采掘服务', '850311': '石油加工',
        #                  '850313': '石油贸易', '850321': '纯碱', '850322': '氯碱', '850323': '无机盐',
        #                  '850324': '其他化学原料', '850331': '氮肥', '850332': '磷肥', '850333': '农药',
        #                  '850334': '日用化学产品', '850335': '涂料油漆油墨制造', '850336': '钾肥', '850337': '民爆用品',
        #                  '850338': '纺织化学用品', '850339': '其他化学制品', '850341': '涤纶', '850342': '维纶',
        #                  '850343': '粘胶', '850344': '其他纤维', '850345': '氨纶', '850351': '其他塑料制品',
        #                  '850352': '合成革', '850353': '改性塑料', '850361': '轮胎', '850362': '其他橡胶制品',
        #                  '850363': '炭黑', '850372': '聚氨酯', '850373': '玻纤', '850381': '复合肥',
        #                  '850382': '氟化工及制冷剂', '850383': '磷化工及磷酸盐', '850411': '普钢', '850412': '特钢',
        #                  '850521': '金属新材料Ⅲ', '850522': '磁性材料', '850523': '非金属新材料', '850531': '黄金Ⅲ',
        #                  '850541': '稀土', '850542': '钨', '850543': '锂', '850544': '其他稀有小金属', '850551': '铝',
        #                  '850552': '铜', '850553': '铅锌', '850611': '玻璃制造Ⅲ', '850612': '水泥制造Ⅲ',
        #                  '850614': '其他建材Ⅲ', '850615': '耐火材料', '850616': '管材', '850623': '房屋建设Ⅲ',
        #                  '850711': '机床工具', '850712': '机械基础件', '850713': '磨具磨料', '850714': '内燃机',
        #                  '850715': '制冷空调设备', '850716': '其它通用机械', '850721': '纺织服装设备', '850722': '工程机械',
        #                  '850723': '农用机械', '850724': '重型机械', '850725': '冶金矿采化工设备', '850726': '印刷包装机械',
        #                  '850727': '其它专用机械', '850728': '楼宇设备', '850729': '环保设备', '850731': '仪器仪表Ⅲ',
        #                  '850741': '电机Ⅲ', '850751': '金属制品Ⅲ', '850811': '集成电路', '850812': '分立器件',
        #                  '850813': '半导体材料', '850822': '印制电路板', '850823': '被动元件', '850831': '显示器件Ⅲ',
        #                  '850832': 'LED', '850833': '光学元件', '850841': '其他电子Ⅲ', '850851': '电子系统组装',
        #                  '850852': '电子零部件制造', '850911': '乘用车', '850912': '商用载货车', '850913': '商用载客车',
        #                  '850921': '汽车零部件Ⅲ', '850935': '船舶制造', '850936': '铁路设备', '850941': '汽车服务Ⅲ',
        #                  '851012': '终端设备', '851013': '通信传输设备', '851014': '通信配套服务', '851021': '计算机设备Ⅲ',
        #                  '851111': '冰箱', '851112': '空调', '851113': '洗衣机', '851114': '小家电', '851115': '家电零部件',
        #                  '851121': '彩电', '851122': '其它视听器材', '851231': '白酒', '851232': '啤酒',
        #                  '851233': '其他酒类', '851234': '软饮料', '851235': '葡萄酒', '851236': '黄酒',
        #                  '851241': '肉制品', '851242': '调味发酵品', '851243': '乳品', '851244': '食品综合',
        #                  '851311': '毛纺', '851312': '棉纺', '851313': '丝绸', '851314': '印染', '851315': '辅料',
        #                  '851316': '其他纺织', '851322': '男装', '851323': '女装', '851324': '休闲服装', '851325': '鞋帽',
        #                  '851326': '家纺', '851327': '其他服装', '851411': '造纸Ⅲ', '851421': '包装印刷Ⅲ', '851432': '家具',
        #                  '851433': '其他家用轻工', '851434': '珠宝首饰', '851435': '文娱用品', '851441': '其他轻工制造Ⅲ',
        #                  '851511': '化学原料药', '851512': '化学制剂', '851521': '中药Ⅲ', '851531': '生物制品Ⅲ',
        #                  '851541': '医药商业Ⅲ', '851551': '医疗器械Ⅲ', '851561': '医疗服务Ⅲ', '851611': '火电',
        #                  '851612': '水电', '851613': '燃机发电', '851614': '热电', '851615': '新能源发电',
        #                  '851621': '水务Ⅲ', '851631': '燃气Ⅲ', '851641': '环保工程及服务Ⅲ', '851711': '港口Ⅲ',
        #                  '851721': '公交Ⅲ', '851731': '高速公路Ⅲ', '851741': '航空运输Ⅲ', '851751': '机场Ⅲ',
        #                  '851761': '航运Ⅲ', '851771': '铁路运输Ⅲ', '851781': '物流Ⅲ', '851811': '房地产开发Ⅲ',
        #                  '851821': '园区开发Ⅲ', '851911': '银行Ⅲ', '851921': '多元金融Ⅲ', '851931': '证券Ⅲ',
        #                  '851941': '保险Ⅲ', '852021': '贸易Ⅲ', '852031': '百货', '852032': '超市', '852033': '多业态零售',
        #                  '852041': '专业连锁', '852051': '一般物业经营', '852052': '专业市场', '852111': '人工景点',
        #                  '852112': '自然景点', '852121': '酒店Ⅲ', '852131': '旅游综合Ⅲ', '852141': '餐饮Ⅲ',
        #                  '852151': '其他休闲服务Ⅲ', '852211': '通信运营Ⅲ', '852221': '互联网信息服务',
        #                  '852222': '移动互联网服务', '852223': '其他互联网服务', '852224': '有线电视网络',
        #                  '852225': '软件开发', '852226': 'IT服务', '852241': '平面媒体', '852242': '影视动漫',
        #                  '852243': '营销服务', '852244': '其他文化传媒', '852311': '综合Ⅲ', '857221': '装修装饰Ⅲ',
        #                  '857231': '城轨建设', '857232': '路桥施工', '857233': '水利工程', '857234': '铁路建设',
        #                  '857235': '其他基础建设', '857241': '钢结构', '857242': '化学工程', '857243': '国际工程承包',
        #                  '857244': '其他专业工程', '857251': '园林工程Ⅲ', '857321': '电网自动化', '857322': '工控自动化',
        #                  '857323': '计量仪表', '857331': '综合电力设备商', '857332': '风电设备', '857333': '光伏设备',
        #                  '857334': '火电设备', '857335': '储能设备', '857336': '其它电源设备', '857341': '高压设备',
        #                  '857342': '中压设备', '857343': '低压设备', '857344': '线缆部件及其他', '857411': '航天装备Ⅲ',
        #                  '857421': '航空装备Ⅲ', '857431': '地面兵装Ⅲ', '858811': '其他交运设备Ⅲ'}

    return industry_dict


def cj_ind_code_to_name(level_num=2):
    """

    :param level_num: 长江一级为2，长江二级为3，长江三级为4
    :return:
    str = ''
    for i in range(industry_sw2.shape[0]):
        str = str + "'" + industry_sw2.index[i] + "': '" + industry_sw2.iloc[i,0] + "', "
    """
    if level_num == 2:
        industry_dict = {
            "001010.CJ": "煤炭",
            "001011.CJ": "油气石化",
            "001012.CJ": "非金属材料",
            "001013.CJ": "化学品",
            "001014.CJ": "金属材料及矿业",
            "001015.CJ": "纸类及包装",
            "001016.CJ": "电力及新能源设备",
            "001017.CJ": "机械设备",
            "001018.CJ": "建筑产品",
            "001019.CJ": "建筑工程",
            "001020.CJ": "国防军工",
            "001021.CJ": "交通运输",
            "001022.CJ": "环保",
            "001023.CJ": "检测服务",
            "001024.CJ": "纺织服装",
            "001025.CJ": "家电制造",
            "001026.CJ": "家用装饰及休闲",
            "001027.CJ": "汽车",
            "001028.CJ": "社会服务",
            "001029.CJ": "商业贸易",
            "001030.CJ": "农产品",
            "001031.CJ": "食品饮料",
            "001032.CJ": "医疗保健",
            "001033.CJ": "银行",
            "001034.CJ": "综合金融",
            "001035.CJ": "保险",
            "001036.CJ": "电子",
            "001037.CJ": "计算机",
            "001038.CJ": "电信业务",
            "001039.CJ": "传媒互联网",
            "001040.CJ": "公用事业",
            "001041.CJ": "房地产",
        }

    return industry_dict
