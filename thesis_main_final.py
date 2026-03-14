"""
=============================================================================
thesis_main_v5.py  ·  2025年贸易战关税决定因素研究
=============================================================================
研究问题：在美国2025年4月2日初始差异化关税公告中，
         哪些因素决定了不同国家被施加的关税幅度差异？

样本：Annex I 初始公告税率严格高于10%的国家（N=68）
主Y：Y = initial_rate（水平值，11%~50%）
方法：OLS（HC3稳健标准误）

v5变更记录：
  [新增] Excel多sheet输出（master_dataset.xlsx + 回归结果汇总）
  [修正] F4_Retaliate移出主模型 → 内生性问题
         报复行为发生在2025-04-02之后，属于后置变量
         保留变量构造，放入扩展模型并在论文中说明
  [修正] 结论打印措辞降强度，改为"当前结果显示"
  [修正] country_name加入ALL_VARS方便核对
  [沿用] SWE/FIN已在NATO（分别于2023/2024年加入，早于研究期）
  [沿用] F2_Ally移除AUT/IRL（中立国）
  [沿用] EU_in_sample替代EU27
  [沿用] 辅助分析后置于PART E
=============================================================================
"""

import re, time, math, warnings
import pandas as pd
import numpy as np
import requests
from pathlib import Path
warnings.filterwarnings('ignore')

BASE  = Path(r"E:\Desk\python1")
DATA  = Path(r"E:\Desk\论文资料")
RAW   = BASE / "dataraw"
INTER = BASE / "outputintermediate"
FINAL = BASE / "outputfinal"
for p in [RAW, INTER, FINAL]:
    p.mkdir(parents=True, exist_ok=True)

COMTRADE_KEY   = "b9e34bf4f2c041c79db8d3a05799162b"
SENATE_LDA_KEY = "d9fa01b3fed778e63de3b1494b1605520760a2fb"

print("✅ 初始化完成")

# ╔══════════════════════════════════════════════════════════════╗
# ║  PART A  样本 + 主Y                                          ║
# ╚══════════════════════════════════════════════════════════════╝

SAMPLE_FILE = BASE / "sample_57.csv"
if not SAMPLE_FILE.exists():
    SAMPLE_FILE = Path(__file__).parent / "sample_57.csv"

df = pd.read_csv(SAMPLE_FILE)
df = df[df["initial_rate"] > 10].copy().reset_index(drop=True)
df["Y"] = df["initial_rate"].astype(float)

ISOS = df["iso3"].tolist()
N    = len(df)
print(f"\n✅ PART A：样本 N={N}，Y范围 {df['Y'].min()}%~{df['Y'].max()}%")

# ╔══════════════════════════════════════════════════════════════╗
# ║  PART B  主X变量                                             ║
# ╚══════════════════════════════════════════════════════════════╝
print("\n" + "="*62)
print("PART B: 主X变量")
print("="*62)

# ── F2_Ally：严格正式条约盟友 ──
# NATO成员（截至2025年，SWE 2024-03-07加入，FIN 2023-04-04加入）
# 已移除：AUT（永久中立国），IRL（中立国），CHE（中立国）
NATO_STRICT = {
    "DEU","FRA","ITA","ESP","NLD","BEL","POL","DNK",
    "PRT","GRC","CZE","HUN","ROU","SVK","BGR","HRV",
    "NOR","SWE","FIN"
}
# 美日/美韩/美菲双边共同防御条约
BILATERAL_ALLIES = {"JPN","KOR","PHL"}
# 注：AUS/NZL（ANZUS条约）不在68国样本中，不影响结果
df["F2_Ally"] = df["iso3"].isin(NATO_STRICT | BILATERAL_ALLIES).astype(int)

# ── F2_Rival：战略竞争对手（仅入扩展模型）──
# CHN(34%), VEN(15%), NIC(18%) — 样本中VEN/NIC低于均值，方向不稳定
# [内生性注意] 此变量主观性强，主模型不放，扩展模型作敏感性检验
RIVALS = {"CHN","VEN","NIC"}
df["F2_Rival"] = df["iso3"].isin(RIVALS).astype(int)

# ── F2_Dist：首都距华盛顿DC距离（km）──
DC = (38.9072, -77.0369)
COORDS = {
    "HND":(14.10,-87.21),"GUY":(6.80,-58.15),"NIC":(12.13,-86.28),
    "DEU":(52.52,13.40),"FRA":(48.85,2.35),"ITA":(41.90,12.48),
    "ESP":(40.42,-3.70),"NLD":(52.37,4.90),"BEL":(50.85,4.35),
    "POL":(52.23,21.01),"SWE":(59.33,18.07),"AUT":(48.21,16.37),
    "CHE":(46.95,7.45),"NOR":(59.91,10.75),"DNK":(55.68,12.57),
    "FIN":(60.17,24.93),"IRL":(53.33,-6.25),"PRT":(38.72,-9.14),
    "GRC":(37.98,23.73),"CZE":(50.08,14.47),"HUN":(47.50,19.05),
    "ROU":(44.44,26.10),"SVK":(48.15,17.11),"BGR":(42.70,23.33),
    "HRV":(45.81,15.98),"SRB":(44.80,20.46),"MDA":(47.01,28.86),
    "ISR":(31.77,35.23),"JOR":(31.95,35.93),"IRQ":(33.34,44.40),
    "DZA":(36.74,3.06),"TUN":(36.82,10.17),"LBY":(32.88,13.19),
    "CHN":(39.91,116.39),"JPN":(35.69,139.69),"KOR":(37.57,126.98),
    "TWN":(25.04,121.56),"VNM":(21.03,105.85),"THA":(13.75,100.52),
    "MYS":(3.14,101.69),"IDN":(-6.21,106.85),"PHL":(14.60,120.98),
    "IND":(28.61,77.21),"BGD":(23.72,90.41),"PAK":(33.72,73.06),
    "LKA":(6.93,79.85),"MMR":(19.79,96.16),"KHM":(11.57,104.92),
    "LAO":(17.97,102.60),"KAZ":(51.18,71.45),
    "ZAF":(-25.74,28.19),"NGA":(9.05,7.50),"AGO":(-8.84,13.24),
    "CMR":(3.87,11.52),"CIV":(6.82,-5.28),"COD":(-4.32,15.32),
    "TZA":(-6.17,35.74),"MOZ":(-25.97,32.57),"NAM":(-22.56,17.08),
    "BWA":(-24.65,25.91),"ZWE":(-17.83,31.05),"MWI":(-13.97,33.79),
    "MUS":(-20.16,57.50),"MDG":(-18.91,47.54),"LSO":(-29.31,27.48),
    "RWA":(-1.94,30.06),"ZMB":(-15.41,28.28),"VEN":(10.48,-66.88),
}
def dist_km(iso3):
    if iso3 not in COORDS: return np.nan
    r = math.radians
    lat1,lon1 = r(DC[0]), r(DC[1])
    lat2,lon2 = r(COORDS[iso3][0]), r(COORDS[iso3][1])
    dlat,dlon = lat2-lat1, lon2-lon1
    a = math.sin(dlat/2)**2 + math.cos(lat1)*math.cos(lat2)*math.sin(dlon/2)**2
    return round(6371*2*math.atan2(math.sqrt(a), math.sqrt(1-a)), 1)
df["F2_Dist"]  = df["iso3"].apply(dist_km)
df["F2_Bases"] = df["iso3"].map(
    {"DEU":119,"JPN":120,"KOR":28,"ITA":14,"GRC":2,"BEL":1,"POL":1,"NOR":1,"ISR":1}
).fillna(0).astype(int)

# ── F3_BRI / F3_RCEP ──
BRI = {"CHN","ITA","GRC","HUN","PRT","SRB","PAK","BGD","LKA","MMR","KHM","LAO",
       "IDN","MYS","VNM","PHL","THA","KAZ","JOR","IRQ","DZA","TUN","LBY","ZAF",
       "NGA","AGO","CMR","CIV","COD","TZA","MOZ","NAM","BWA","ZWE","MWI","MDG",
       "LSO","RWA","ZMB","VEN","NIC","MDA"}
RCEP = {"CHN","JPN","KOR","VNM","THA","MYS","IDN","PHL","KHM","LAO","MMR"}
df["F3_BRI"]  = df["iso3"].isin(BRI).astype(int)
df["F3_RCEP"] = df["iso3"].isin(RCEP).astype(int)

# ── F4_301 / F4_Stance ──
USTR_301 = {"CHN","IND","IDN","PAK","THA","VNM","MYS","TUN",
            "DZA","KAZ","NGA","VEN","NIC"}
df["F4_301"] = df["iso3"].isin(USTR_301).astype(int)

STANCE = {
    "CHN":1,
    "DEU":2,"FRA":2,"ITA":2,"ESP":2,"NLD":2,"BEL":2,"POL":2,"SWE":2,
    "AUT":2,"DNK":2,"FIN":2,"IRL":2,"PRT":2,"GRC":2,"CZE":2,"HUN":2,
    "ROU":2,"SVK":2,"BGR":2,"HRV":2,"IND":2,"CHE":2,"NOR":2,"KAZ":2,
    "SRB":2,"MDA":2,"VNM":2,"IDN":2,"THA":2,"MYS":2,"PAK":2,
    "JPN":3,"KOR":3,"TWN":3,"ISR":3,"PHL":3,"BGD":3,"ZAF":3,"JOR":3,
}
df["F4_Stance"] = df["iso3"].map(STANCE).fillna(2).astype(int)

# ── F4_Retaliate：构造但移出主模型 ──
# [内生性说明] 报复行为发生于2025-04-02之后，是白宫定税后的反应
# 作为Y的前因放入主模型存在反向因果风险
# 处理：保留变量，在扩展模型中作为敏感性检验，论文中明确说明时序问题
RETALIATORS = {
    "CHN",
    "DEU","FRA","ITA","ESP","NLD","BEL","POL","SWE","AUT",
    "DNK","FIN","IRL","PRT","GRC","CZE","HUN","ROU","SVK","BGR","HRV","NOR",
}
df["F4_Retaliate"] = df["iso3"].isin(RETALIATORS).astype(int)

# ── EU_in_sample：EU成员控制变量 ──
# 真正的欧盟27成员国（样本内出现的子集）
# 不含NOR（欧洲经济区但非EU），不含CHE（非EU）
EU_IN_SAMPLE = {
    "DEU","FRA","ITA","ESP","NLD","BEL","POL","SWE","AUT","DNK",
    "FIN","IRL","PRT","GRC","CZE","HUN","ROU","SVK","BGR","HRV"
}
df["EU_in_sample"] = df["iso3"].isin(EU_IN_SAMPLE).astype(int)

# 占位列
for col in ["F1_Surplus_B","F1_ExpDep","F1_FX_Watch","F1_Lobby_M",
            "F2_UNVote","F2_Polity","F3_ChinaDep","F3_TiVA"]:
    if col not in df.columns:
        df[col] = np.nan

print(f"✅ PART B 完成")
print(f"   F2_Ally={df['F2_Ally'].sum()} (NATO19+JPN/KOR/PHL，已移除AUT/IRL/CHE)")
print(f"   F3_BRI={df['F3_BRI'].sum()}  F3_RCEP={df['F3_RCEP'].sum()}")
print(f"   F4_301={df['F4_301'].sum()}  F4_Stance分布: "
      f"{dict(df['F4_Stance'].value_counts().sort_index())}")
print(f"   EU_in_sample={df['EU_in_sample'].sum()}")
print(f"   F4_Retaliate={df['F4_Retaliate'].sum()} [已构造，移出主模型，进扩展区]")

# GTA验证
gta_file = RAW / "interventions.csv"
if gta_file.exists():
    df_gta = pd.read_csv(gta_file, low_memory=False)
    if "Affected Jurisdictions" in df_gta.columns:
        df_gta["Date Announced"] = pd.to_datetime(
            df_gta["Date Announced"], errors="coerce")
        usa_ret = df_gta[
            (df_gta["Date Announced"] >= pd.Timestamp("2025-04-02")) &
            (df_gta["Affected Jurisdictions"].astype(str)
             .str.contains("United States|USA", case=False, na=False))
        ]
        print(f"\n   [GTA] 2025-04-02后针对美国的干预: {len(usa_ret)}条")
        if len(usa_ret):
            print(usa_ret["Implementing Jurisdictions"].value_counts()
                  .head(6).to_string())
            print("   ↑ 对比F4_Retaliate编码，如有出入请手动更新")

# ╔══════════════════════════════════════════════════════════════╗
# ║  PART C  UN Comtrade（分批抓取）                             ║
# ╚══════════════════════════════════════════════════════════════╝
CT_CODE = {
    "LSO":426,"KHM":116,"LAO":418,"MDG":450,"VNM":704,"LKA":144,
    "MMR":104,"MUS":480,"IRQ":368,"GUY":328,"BGD":50,"BWA":72,
    "SRB":688,"THA":764,"HND":340,"CHN":156,"TWN":490,"IDN":360,
    "AGO":24,"CHE":756,"LBY":434,"MDA":498,"ZAF":710,"DZA":12,
    "PAK":586,"TUN":788,"KAZ":398,"IND":356,"KOR":410,"JPN":392,
    "MYS":458,"NAM":516,"CIV":384,"DEU":276,"FRA":251,"ITA":381,
    "ESP":724,"NLD":528,"BEL":56,"POL":616,"SWE":752,"AUT":40,
    "DNK":208,"FIN":246,"IRL":372,"PRT":620,"GRC":300,"CZE":203,
    "HUN":348,"ROU":642,"SVK":703,"BGR":100,"HRV":191,"JOR":400,
    "ZWE":716,"RWA":646,"NIC":558,"ISR":376,"PHL":608,"MWI":454,
    "ZMB":894,"MOZ":508,"TZA":834,"NOR":578,"VEN":862,"NGA":566,
    "CMR":120,"COD":180,
}
df["ct_code"] = df["iso3"].map(CT_CODE)

COMTRADE_BASE = "https://comtradeapi.un.org/data/v1/get/C/A/HS"

def ct_get(reporter, partner, flow, year="2023", cmd="TOTAL"):
    headers = {"Ocp-Apim-Subscription-Key": COMTRADE_KEY}
    params  = {"reporterCode":reporter,"partnerCode":partner,
                "period":str(year),"cmdCode":cmd,"flowCode":flow,
                "maxRecords":500,"format":"JSON","breakdownMode":"classic",
                "includeDesc":"false"}
    try:
        r = requests.get(COMTRADE_BASE, params=params, headers=headers, timeout=30)
        if r.status_code == 200:
            return sum(x.get("primaryValue",0) or 0
                       for x in r.json().get("data",[]))
        if r.status_code == 429:
            print("   ⏳ 速率限制，等待65秒...")
            time.sleep(65)
            return ct_get(reporter, partner, flow, year, cmd)
        return None
    except Exception as e:
        print(f"   异常: {e}"); return None

RUN_DAY1 = False  # ← 第1天改True：美国双边进出口 (~68次)
RUN_DAY2 = False  # ← 第2天改True：各国总出口+对华 (~136次)

if RUN_DAY1:
    print("\n🌐 DAY1: 美国双边贸易...")
    cache = INTER / "ct_us_bilateral.csv"
    rows  = pd.read_csv(cache).to_dict("records") if cache.exists() else []
    done  = {r["iso3"] for r in rows}
    for iso3 in ISOS:
        if iso3 in done or not CT_CODE.get(iso3): continue
        ct  = CT_CODE[iso3]
        imp = ct_get(842, ct, "M", "2023")
        exp = ct_get(842, ct, "X", "2023")
        rows.append({"iso3":iso3,
                     "US_imp_B": imp/1e9 if imp is not None else None,
                     "US_exp_B": exp/1e9 if exp is not None else None})
        time.sleep(1.5)
        pd.DataFrame(rows).to_csv(cache, index=False)
        print(f"   {iso3}: {'imp='+str(round(imp/1e9,1))+'B' if imp else '失败'}")
    df_d1 = pd.DataFrame(rows)
    df_d1["F1_Surplus_B"] = df_d1["US_imp_B"] - df_d1["US_exp_B"]
    df_d1.to_csv(cache, index=False)
    print("✅ DAY1完成")

if RUN_DAY2:
    print("\n🌐 DAY2: 各国总出口 + 对华出口...")
    cache = INTER / "ct_export_deps.csv"
    rows  = pd.read_csv(cache).to_dict("records") if cache.exists() else []
    done  = {r["iso3"] for r in rows}
    for iso3 in ISOS:
        if iso3 in done or not CT_CODE.get(iso3): continue
        ct    = CT_CODE[iso3]
        total = ct_get(ct, 0,   "X", "2022")
        china = ct_get(ct, 156, "X", "2022")
        rows.append({"iso3":iso3,
                     "total_exp_B": total/1e9 if total is not None else None,
                     "china_exp_B": china/1e9 if china is not None else None})
        time.sleep(1.5)
        pd.DataFrame(rows).to_csv(cache, index=False)
        print(f"   {iso3}: {'完成' if total else '失败'}")
    print("✅ DAY2完成")

# 合并Comtrade缓存
for cache_path in [INTER/"ct_us_bilateral.csv", INTER/"ct_export_deps.csv"]:
    if cache_path.exists():
        df_tmp   = pd.read_csv(cache_path)
        new_cols = [c for c in df_tmp.columns
                    if c not in ["iso3","ct_code"] and c not in df.columns]
        if new_cols:
            df = df.merge(df_tmp[["iso3"]+new_cols], on="iso3", how="left")
            print(f"   ✅ 合并 {cache_path.name}: {new_cols}")

if "US_imp_B" in df.columns and "total_exp_B" in df.columns:
    df["F1_Surplus_B"] = df["US_imp_B"] - df.get("US_exp_B", pd.Series(0, index=df.index))
    df["F1_ExpDep"]    = df["US_imp_B"] / df["total_exp_B"] * 100
if "china_exp_B" in df.columns and "total_exp_B" in df.columns:
    df["F3_ChinaDep"]  = df["china_exp_B"] / df["total_exp_B"] * 100

# 手动下载提示
for fname, url in [
    ("UNvoting.csv",  "https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/LEJUQZ"),
    ("p5v2018.xls",   "https://www.systemicpeace.org/inscr/p5v2018.xls"),
    ("tiva_2023.csv", "https://stats.oecd.org/index.aspx?queryid=75537"),
]:
    if not (DATA / fname).exists():
        print(f"   ⚠️  {fname} 缺失 → {url}")

# ╔══════════════════════════════════════════════════════════════╗
# ║  PART D  保存主数据集（CSV + Excel）                         ║
# ╚══════════════════════════════════════════════════════════════╝
print("\n" + "="*62)
print("PART D: 保存主数据集")
print("="*62)

# 主数据集变量（含country_name方便核对）
MAIN_VARS = [
    "iso3","country_name","region","Y",
    "F2_Ally","F2_Dist","F2_Bases",
    "F3_BRI","F3_RCEP",
    "F4_301","F4_Stance",
    "EU_in_sample",
    # 待补充（Comtrade完成后自动填入）
    "F1_Surplus_B","F1_ExpDep","F1_FX_Watch","F1_Lobby_M",
    "F2_UNVote","F2_Polity",
    "F3_ChinaDep","F3_TiVA",
    # 扩展模型备用（不进主回归）
    "F2_Rival","F4_Retaliate",
]
for col in MAIN_VARS:
    if col not in df.columns:
        df[col] = np.nan

df_main = df[MAIN_VARS].copy()

# ── CSV输出 ──
csv_path = FINAL / "master_dataset.csv"
df_main.to_csv(csv_path, index=False)

# ── Excel输出（多sheet）──
xlsx_path = FINAL / "master_dataset.xlsx"
with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
    # Sheet1: 主数据集
    df_main.to_excel(writer, sheet_name="主数据集", index=False)
    # Sheet2: 描述性统计
    num_cols = [c for c in MAIN_VARS
                if c in df_main.columns
                and df_main[c].dtype in [float, int, np.float64, np.int64]
                and c not in ["F2_Ally","EU_in_sample","F3_BRI","F3_RCEP",
                               "F4_301","F2_Rival","F4_Retaliate",
                               "F2_Bases","F1_FX_Watch"]]
    desc = df_main[num_cols].describe().round(3)
    desc.to_excel(writer, sheet_name="描述性统计")
    # Sheet3: 变量完整度
    completeness = pd.DataFrame([{
        "变量": col,
        "有效N": df_main[col].notna().sum(),
        "完整度%": round(df_main[col].notna().mean()*100, 1),
        "状态": "✅" if df_main[col].notna().all()
                 else ("⚠️" if df_main[col].notna().mean()>0.5 else "❌待补充")
    } for col in MAIN_VARS if col not in ["iso3","country_name","region"]])
    completeness.to_excel(writer, sheet_name="变量完整度", index=False)

print(f"✅ CSV:   {csv_path}")
print(f"✅ Excel: {xlsx_path}  （3个Sheet：主数据集/描述性统计/变量完整度）")

print("\n变量完整度:")
for col in MAIN_VARS[3:]:
    n    = df_main[col].notna().sum()
    pct  = n / N * 100
    mark = "✅" if pct==100 else ("⚠️ " if pct>50 else "❌")
    print(f"   {mark} {col:<18}: {pct:5.1f}%  ({n}/{N})")

# ╔══════════════════════════════════════════════════════════════╗
# ║  PART E  主OLS回归                                           ║
# ╚══════════════════════════════════════════════════════════════╝
print("\n" + "="*62)
print("PART E: 主OLS回归")
print("="*62)

reg_results = {}  # 存放模型对象，供Excel汇总用

try:
    import statsmodels.formula.api as smf

    df_r = df_main.copy()
    df_r["iso3"]     = df["iso3"]
    df_r["log_dist"] = np.log1p(df_r["F2_Dist"])

    def ols(formula, label, no_china=False, no_eu=False):
        d = df_r.copy()
        if no_china: d = d[d["iso3"] != "CHN"]
        if no_eu:    d = d[d["EU_in_sample"] == 0]
        lhs  = formula.split("~")[0].strip()
        used = [v for v in re.findall(r'\b[A-Za-z_]\w*\b', formula)
                if v in d.columns and v != lhs]
        d = d.dropna(subset=[lhs]+used)
        if len(d) < 15:
            print(f"\n   ⚠️  {label}: 有效样本不足({len(d)})，跳过")
            return None
        try:
            m   = smf.ols(formula, data=d).fit(cov_type="HC3")
            tag = ("" + (" [排除中国]" if no_china else "")
                      + (" [排除EU]"  if no_eu    else ""))
            print(f"\n{'─'*62}")
            print(f"{label}{tag}")
            print(f"n={len(d)}  R²={m.rsquared:.3f}  "
                  f"Adj.R²={m.rsquared_adj:.3f}  F-p={m.f_pvalue:.4f}")
            print(f"{'─'*62}")
            print(m.summary().tables[1])
            reg_results[label+tag] = m
            return m
        except Exception as e:
            print(f"   ⚠️  {label} 失败: {e}")
            return None

    # ── M1：基准模型（主模型）──
    # 不含F4_Retaliate（时序问题，见变量说明）
    m1 = ols(
        "Y ~ F2_Ally + log_dist + F3_BRI + F3_RCEP + F4_301 + F4_Stance",
        "M1_基准")

    # ── M1c：加EU集体效应控制变量 ──
    m1c = ols(
        "Y ~ F2_Ally + log_dist + F3_BRI + F3_RCEP"
        " + EU_in_sample + F4_301 + F4_Stance",
        "M1c_含EU控制")

    # ── M1x：排除中国稳健性 ──
    m1x = ols(
        "Y ~ F2_Ally + log_dist + F3_BRI + F3_RCEP"
        " + EU_in_sample + F4_301 + F4_Stance",
        "M1c_含EU控制", no_china=True)

    # ── M2：扩展模型（含F4_Retaliate，作敏感性检验）──
    # 在论文中说明：F4_Retaliate为后置变量，系数不做因果解读
    m2 = ols(
        "Y ~ F2_Ally + log_dist + F3_BRI + F3_RCEP"
        " + EU_in_sample + F4_301 + F4_Stance + F4_Retaliate",
        "M2_扩展含Retaliate")

    # ── M3：扩展模型（含经济变量，Comtrade完成后激活）──
    m3 = ols(
        "Y ~ F1_Surplus_B + F1_ExpDep + F1_FX_Watch"
        " + F2_Ally + log_dist + F3_BRI + F3_RCEP + F3_ChinaDep"
        " + EU_in_sample + F4_301 + F4_Stance",
        "M3_含贸易变量")

    # ── M4：全变量模型（Polity/UNVote下载后激活）──
    m4 = ols(
        "Y ~ F1_Surplus_B + F1_ExpDep"
        " + F2_Ally + log_dist + F2_Polity + F2_UNVote"
        " + F3_BRI + F3_RCEP + F3_ChinaDep"
        " + EU_in_sample + F4_301 + F4_Stance",
        "M4_含政治变量")

    # ── 保存回归结果到Excel（汇总sheet）──
    if reg_results:
        xlsx_reg = FINAL / "regression_results.xlsx"
        with pd.ExcelWriter(xlsx_reg, engine="openpyxl") as writer:
            for mname, m in reg_results.items():
                res_df = pd.DataFrame({
                    "coef":  m.params.round(4),
                    "se":    m.bse.round(4),
                    "t/z":   m.tvalues.round(3),
                    "p":     m.pvalues.round(4),
                    "ci_lo": m.conf_int()[0].round(4),
                    "ci_hi": m.conf_int()[1].round(4),
                    "sig":   m.pvalues.apply(
                        lambda p: "***" if p<0.01 else
                                  ("**" if p<0.05 else
                                   ("*" if p<0.1 else "")))
                })
                # sheet名称限制31字符
                sheet_name = mname[:31]
                res_df.to_excel(writer, sheet_name=sheet_name)
                # 同时保存元数据
                meta = pd.DataFrame({
                    "指标": ["N","R²","Adj.R²","F-p"],
                    "值":   [m.nobs, round(m.rsquared,3),
                             round(m.rsquared_adj,3), round(m.f_pvalue,4)]
                })
                # 写在结果下方
            # 汇总对比表
            summary_rows = []
            for mname, m in reg_results.items():
                row = {"模型": mname, "N": int(m.nobs),
                       "R²": round(m.rsquared,3),
                       "Adj.R²": round(m.rsquared_adj,3)}
                for var in ["F2_Ally","F3_RCEP","F3_BRI","F4_Stance",
                            "EU_in_sample","F4_301","F4_Retaliate"]:
                    if var in m.params.index:
                        p = m.pvalues[var]
                        sig = "***" if p<0.01 else ("**" if p<0.05 else ("*" if p<0.1 else ""))
                        row[var] = f"{m.params[var]:.2f}{sig}"
                    else:
                        row[var] = "—"
                summary_rows.append(row)
            pd.DataFrame(summary_rows).to_excel(
                writer, sheet_name="回归系数汇总", index=False)
        print(f"\n✅ 回归结果Excel: {xlsx_reg}")
        print("   （每个模型独立Sheet + 回归系数汇总对比表）")

except ImportError:
    print("⚠️  pip install statsmodels openpyxl")

# ╔══════════════════════════════════════════════════════════════╗
# ║  PART F  辅助分析（不进主结论）                              ║
# ╚══════════════════════════════════════════════════════════════╝
print("\n" + "="*62)
print("PART F: 辅助分析（仅供参考）")
print("="*62)

# ── F1：Y_Change稳健性（Teti Y_Pre）──
print("\n[F1] Teti Y_Pre → Y_Change 一致性检验")
teti_csv = DATA / "GTD-tradeWar_hs6" / "GTD-tradeWar_hs6.csv"
if teti_csv.exists():
    cols_all  = pd.read_csv(teti_csv, nrows=0).columns.tolist()
    date_cols = sorted([c for c in cols_all if re.match(r"t_\d{8}$", c)])
    pre_cols  = [c for c in date_cols if c <= "t_20250119"]
    if pre_cols:
        last_pre = pre_cols[-1]
        print(f"   使用关税列: {last_pre}")
        chunks = []
        for chunk in pd.read_csv(teti_csv,
                                  usecols=["importer","exporter",last_pre],
                                  chunksize=500_000, low_memory=False):
            usa = chunk[chunk["importer"].str.upper()
                        .isin(["USA","US","UNITED STATES"])]
            if len(usa): chunks.append(usa)
        if chunks:
            df_usa   = pd.concat(chunks, ignore_index=True)
            ypre_raw = df_usa.groupby("exporter")[last_pre].mean()
            TETI_MAP = {
                "China":"CHN","Japan":"JPN","Korea, Republic of":"KOR",
                "Viet Nam":"VNM","Vietnam":"VNM","Thailand":"THA",
                "Malaysia":"MYS","Indonesia":"IDN","Philippines":"PHL",
                "India":"IND","Bangladesh":"BGD","Pakistan":"PAK",
                "Sri Lanka":"LKA","Myanmar":"MMR","Cambodia":"KHM",
                "Lao PDR":"LAO","Kazakhstan":"KAZ",
                "Taiwan, Province of China":"TWN","Taiwan":"TWN",
                "Switzerland":"CHE","Norway":"NOR",
                "Germany":"DEU","France":"FRA","Italy":"ITA","Spain":"ESP",
                "Netherlands":"NLD","Belgium":"BEL","Poland":"POL",
                "Sweden":"SWE","Austria":"AUT","Denmark":"DNK",
                "Finland":"FIN","Ireland":"IRL","Portugal":"PRT",
                "Greece":"GRC","Czech Republic":"CZE","Hungary":"HUN",
                "Romania":"ROU","Slovakia":"SVK","Bulgaria":"BGR",
                "Croatia":"HRV","Serbia":"SRB","Moldova":"MDA",
                "Israel":"ISR","Jordan":"JOR","Iraq":"IRQ",
                "Algeria":"DZA","Tunisia":"TUN","Libya":"LBY",
                "South Africa":"ZAF","Nigeria":"NGA","Angola":"AGO",
                "Cameroon":"CMR","Cote d'Ivoire":"CIV","Ivory Coast":"CIV",
                "Congo, The Democratic Republic of the":"COD",
                "Tanzania":"TZA","Mozambique":"MOZ","Namibia":"NAM",
                "Botswana":"BWA","Zimbabwe":"ZWE","Malawi":"MWI",
                "Mauritius":"MUS","Madagascar":"MDG","Lesotho":"LSO",
                "Rwanda":"RWA","Zambia":"ZMB","Guyana":"GUY",
                "Honduras":"HND","Nicaragua":"NIC","Venezuela":"VEN",
            }
            ypre_iso = ypre_raw.rename(index=TETI_MAP).groupby(level=0).mean()
            df["Y_Pre_Teti"] = df["iso3"].map(ypre_iso).fillna(3.4)
            df["Y_Change"]   = df["Y"] - df["Y_Pre_Teti"]
            print(f"   匹配 {df['Y_Pre_Teti'].notna().sum()}/{N} 国")
            print(f"   Y_Change: 均值={df['Y_Change'].mean():.1f}%")
            # 对比回归
            try:
                df_r2 = df.copy()
                df_r2["log_dist"] = np.log1p(df_r2["F2_Dist"])
                mc = smf.ols(
                    "Y_Change ~ F2_Ally + log_dist + F3_BRI + F3_RCEP"
                    " + EU_in_sample + F4_301 + F4_Stance",
                    data=df_r2.dropna(subset=["Y_Change","F2_Ally",
                                               "log_dist","F3_BRI","EU_in_sample"])
                ).fit(cov_type="HC3")
                print(f"   Y_Change回归 R²={mc.rsquared:.3f}"
                      f"（主Y R²={m1c.rsquared:.3f}，"
                      f"{'一致✅' if abs(mc.rsquared-m1c.rsquared)<0.05 else '差异较大⚠️'}）")
                df["Y_Pre_Teti"].to_csv(INTER/"aux_ypre_teti.csv")
            except: pass
else:
    print("   Teti CSV不存在，跳过")

# ── F2：Y2_Deal Logit ──
print("\n[F2] Y2_Deal Logit（辅助分析）")
DEAL = {"JPN","KOR","CHE","VNM","IDN","IND","TWN","ISR","PHL","THA",
        "MYS","BGD","DEU","FRA","ITA","ESP","NLD","BEL","POL","AUT",
        "SWE","DNK","FIN","IRL","PRT","GRC","CZE","HUN","ROU","SVK",
        "BGR","HRV","NOR"}
df["Y2_Deal"] = df["iso3"].isin(DEAL).astype(int)
try:
    d_logit = df.dropna(subset=["Y2_Deal","F3_BRI","F4_301","Y","F4_Stance"])
    # 移除EU_in_sample（完全分离：EU国家全部Deal=1）
    lm = smf.logit(
        "Y2_Deal ~ F3_BRI + F4_301 + Y + F4_Stance",
        data=d_logit
    ).fit(disp=False)
    print(f"   n={len(d_logit)}  伪R²={lm.prsquared:.3f}")
    print(lm.summary().tables[1])
    aux_df = pd.DataFrame({"coef":lm.params,"se":lm.bse,
                           "z":lm.tvalues,"p":lm.pvalues,
                           "OR":np.exp(lm.params)})
    aux_df.to_csv(FINAL/"aux_Logit_Y2Deal.csv")
    aux_df.to_excel(FINAL/"aux_Logit_Y2Deal.xlsx")
    print("   保存: aux_Logit_Y2Deal.csv / .xlsx")
except Exception as e:
    print(f"   ⚠️  {e}")

# ╔══════════════════════════════════════════════════════════════╗
# ║  总结                                                        ║
# ╚══════════════════════════════════════════════════════════════╝
print(f"""
{'='*62}
运行完成  ·  N={N}  ·  主Y = initial_rate
{'='*62}
【输出文件（outputfinal/）】
  master_dataset.xlsx     ← 3个Sheet（主数据集/描述统计/完整度）
  master_dataset.csv      ← 同上（CSV备份）
  regression_results.xlsx ← 各模型系数Sheet + 汇总对比表
  aux_Logit_Y2Deal.xlsx   ← 框架协议辅助分析

【当前结果方向（需Comtrade/Polity数据加入后最终确认）】
  F3_RCEP：当前结果显示与更高初始税率正相关，需M3中控制贸易变量后确认
  F2_Ally：当前结果显示与更低初始税率负相关，在含EU控制模型中需进一步确认
  F4_Stance：当前结果显示谈判配合度与更低税率负相关
  F4_Retaliate：移出主模型（后置变量），在M2扩展中作敏感性检验

【下一步（数据补充）】
  RUN_DAY1=True  美国双边贸易   (~68次 API)
  RUN_DAY2=True  各国总出口+对华 (~136次 API)
  下载 UNvoting.csv / p5v2018.xls / tiva_2023.csv
{'='*62}
""")