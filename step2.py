"""
Step 2: 抓取 World Bank 数据（已移除WITS）
═══════════════════════════════════════════════════════════════
抓取变量：
  F1_ManufShare   制造业出口占总出口%     TX.VAL.MANF.ZS.UN
  WB_GDP_B        GDP（$十亿）           NY.GDP.MKTP.CD
  WB_Pop_M        人口（百万）           SP.POP.TOTL
  WB_TradeGDP     贸易/GDP%             NE.TRD.GNFS.ZS
  Y_Pre_WB        [可选]美国进口关税均值  TM.TAX.MRCH.WM.AR.ZS
                  → 注意：此指标是进口国自身的税率，不是美国对他们的
                    实际作为参考，Pre-Trump美国对外税率用手动数据

关于WITS（为什么不用）：
  - WITS是世界银行的数据整合网站，本质上是通过网页展示数据
  - 其API已限制访问（403错误），不再提供programmatic access
  - 替代方案：直接用 World Bank API（api.worldbank.org）
    同样来自WTO/ITC/UNCTAD数据库，数据完全相同
  - 历史MFN税率改用手动整理数据（见step1脚本）

运行时间：约2-3分钟（无需API Key）
═══════════════════════════════════════════════════════════════
"""

import requests
import pandas as pd
import time

SAMPLE_COUNTRIES = [
    "AFG","ALB","DZA","AGO","ARG","ARM","AUS","AUT","AZE","BGD",
    "BLR","BEL","BOL","BRA","BGR","KHM","CAN","CHL","CHN","COL",
    "COD","CRI","HRV","CZE","DNK","DOM","ECU","EGY","SLV","ETH",
    "FIN","FRA","GEO","DEU","GHA","GRC","GTM","HND","HKG","HUN",
    "IND","IDN","IRN","IRL","ISR","ITA","JPN","JOR","KAZ","KEN",
    "KOR","KWT","LAO","LBN","LTU","LUX","MYS","MLT","MEX","MDA",
    "MNG","MAR","MOZ","MMR","NPL","NLD","NZL","NGA","NOR","OMN",
    "PAK","PAN","PRY","PER","PHL","POL","PRT","QAT","ROU","RUS",
    "SAU","SEN","SRB","SGP","SVK","SVN","ZAF","ESP","LKA","SWE",
    "CHE","TWN","TZA","THA","TUN","TUR","UGA","UKR","ARE","GBR",
    "URY","UZB","VEN","VNM","ZMB","ZWE",
]

WB_BASE = "https://api.worldbank.org/v2"
YEAR    = 2023

# 要抓的指标（全部来自 api.worldbank.org，无需Key）
INDICATORS = {
    "TX.VAL.MANF.ZS.UN": ("F1_ManufShare", 1,    "制造业出口占总出口%"),
    "NY.GDP.MKTP.CD":    ("WB_GDP_B",      1e-9, "GDP $十亿"),
    "SP.POP.TOTL":       ("WB_Pop_M",      1e-6, "人口 百万"),
    "NE.TRD.GNFS.ZS":   ("WB_TradeGDP",   1,    "贸易/GDP%"),
}

results = {iso3: {"ISO3": iso3} for iso3 in SAMPLE_COUNTRIES}

print("=" * 55)
print("Step 2: World Bank 数据抓取（已移除WITS）")
print(f"年份: {YEAR} | 国家数: {len(SAMPLE_COUNTRIES)}")
print("=" * 55)

for ind, (col, scale, desc) in INDICATORS.items():
    print(f"\n  抓取 {col} — {desc}")
    url = (f"{WB_BASE}/country/all/indicator/{ind}"
           f"?format=json&date={YEAR}&per_page=300&mrv=1")
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        data = r.json()
        n = 0
        if data and len(data) > 1 and data[1]:
            for row in data[1]:
                iso3 = row.get("countryiso3code","")
                val  = row.get("value")
                if iso3 in results and val is not None:
                    results[iso3][col] = round(float(val) * scale, 4)
                    n += 1
        print(f"  ✓ {n} 个国家")
    except Exception as e:
        print(f"  ✗ 失败: {e}")
    time.sleep(1)

df = pd.DataFrame(list(results.values()))

# ─── 质量检查 ────────────────────────────────────────────────
print(f"\n{'='*55}")
print(f"Step 2 完成")
for col, (varname, _, desc) in INDICATORS.items():
    if varname in df.columns:
        n_ok   = df[varname].notna().sum()
        n_miss = len(df) - n_ok
        print(f"  {varname:18s}: {n_ok:3d}有数据  {n_miss:3d}缺失")

df.to_csv("step2_worldbank.csv", index=False, encoding="utf-8-sig")
print(f"\n✅ 保存 → step2_worldbank.csv")
print(f"""
📌 你的审查清单:
   F1_ManufShare: 中国约93%, 越南约85%, 德国约80%
   WB_GDP_B:      美国约25000B, 中国约18000B, 德国约4500B
   WB_Pop_M:      中国约1400M, 印度约1400M, 美国约340M
""")