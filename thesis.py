import pandas as pd
import requests

print("正在获取全球国家底表...")
url = "http://api.worldbank.org/v2/country?format=json&per_page=300"
try:
    response = requests.get(url)
    data = response.json()[1]
    countries = [
        {'Country_Name': item['name'], 'ISO3_Code': item['id'], 'Region': item['region']['value']}
        for item in data if item['region']['value'] != 'Aggregates'
    ]
    df = pd.DataFrame(countries)
except Exception as e:
    print("获取国家列表失败，生成基础空表...")
    df = pd.DataFrame(columns=['Country_Name', 'ISO3_Code', 'Region'])

print("正在为你构建全维度回归变量矩阵...")

# ==========================================
# 1. 因变量 (Y)
# ==========================================
df['Y_Initial_Tariff_Threat'] = ""    # 美国最初威胁的关税率 (%)
df['Y_Final_Implemented_Tariff'] = "" # 最终实际落地的关税率 (%)

# ==========================================
# 2. 经济与产业链自变量 (X_Econ)
# ==========================================
df['X_Econ_Export_Dep_US_Pct'] = ""   # 对美出口依赖度 (对美出口/总出口)
df['X_Econ_Export_Dep_CN_Pct'] = ""   # 对华出口依赖度
df['X_Econ_Export_HHI'] = ""          # 出口集中度指数 (HHI)
df['X_Econ_HighTech_Dep_US'] = ""     # 对美高科技产品依赖度
df['X_Econ_Foreign_Value_Added'] = "" # 出口中的国外增加值比重 (TiVA)
df['X_Econ_Critical_Minerals'] = ""   # 是否掌控关键矿产 (1=是, 0=否)

# ==========================================
# 3. 政治与安全自变量 (X_Pol)
# ==========================================
df['X_Pol_US_Military_Bases'] = ""    # 境内美军基地数量
df['X_Pol_Alliance_Level'] = ""       # 联盟等级 (3=五眼, 2=核心盟友, 1=普通, 0=敌对)
df['X_Pol_UN_Voting_Align'] = ""      # 联合国投票与美一致性 (0-1)
df['X_Pol_Entity_List_Count'] = ""    # 被列入美国实体清单的企业数量
df['X_Pol_Ideology_Score'] = ""       # 意识形态得分 (Polity V / Freedom House)
df['X_Pol_Distance_to_US_km'] = ""    # 首都距华盛顿距离 (公里)

# ==========================================
# 4. 软实力与文化自变量 (X_Cult)
# ==========================================
df['X_Cult_Starbucks_McD_Count'] = "" # 星巴克/麦当劳门店数量 (文化代理变量)

# ==========================================
# 5. 博弈与谈判协变量 (X_Negot)
# ==========================================
df['X_Negot_Lobbying_US_Mil'] = ""    # 在美游说支出 (百万美元)
df['X_Negot_Retaliation_Level'] = ""  # 反制烈度 (0=无, 1=口头/WTO, 2=对等关税, 3=超额报复)
df['X_Negot_Purchase_Commit_Bil'] = ""# 承诺采购美国农产/能源金额 (十亿美元)
df['X_Negot_FDI_to_US_Bil'] = ""      # 承诺对美投资金额 (十亿美元)
df['X_Negot_Signed_Framework'] = ""   # 是否最终签署互惠框架协议 (1=是, 0=否)

# 保存为 Excel 数据收集模板
output_file = "Ultimate_Regression_Dataset_Template.xlsx"
df.to_excel(output_file, index=False)

print(f"\n✅ 终极论文数据收集模板已生成：{output_file}")
print("你可以用 Excel 打开它，这就是你接下来几周收集数据的'大本营'。")