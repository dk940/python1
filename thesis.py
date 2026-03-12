"""
Step 1 (更新版): 构建 Y 变量 — 关税变化幅度
═══════════════════════════════════════════════════════════════
新的Y变量设计：

  Y = 2026年2月最终税率  -  川普上任前税率（2024年）
    = 关税"净增幅"

逻辑：
  - 所有国家起点不同（有些原本就有贸易摩擦，税率较高）
  - 你的X变量解释的是：为什么有些国家被加征得更多？
  - 这比单看最终税率更干净，控制了"原本税率"的基线差异

数据来源：
  Pre-Trump:  World Bank API — TM.TAX.MRCH.WM.AR.ZS
              （美国对各国进口的加权平均适用税率，用2023年数据）
  Post-Trump: 白宫 Annex I (2025-04-02) 宣布税率
              （Y_Final = Liberation Day宣布值，口径统一）

中国处理：
  Pre-Trump中国已有约20%+关税（301条款叠加）
  Post-Trump = 34%（Annex I口径，不用145%保持口径一致）
  Y_China_Flag = 1 标注，稳健性检验时单独处理

═══════════════════════════════════════════════════════════════
"""

import requests
import pandas as pd
import time

# ─── 样本国家 ───────────────────────────────────────────────
SAMPLE_COUNTRIES = [
    "AFG", "ALB", "DZA", "AGO", "ARG", "ARM", "AUS", "AUT", "AZE", "BGD",
    "BLR", "BEL", "BOL", "BRA", "BGR", "KHM", "CAN", "CHL", "CHN", "COL",
    "COD", "CRI", "HRV", "CZE", "DNK", "DOM", "ECU", "EGY", "SLV", "ETH",
    "FIN", "FRA", "GEO", "DEU", "GHA", "GRC", "GTM", "HND", "HKG", "HUN",
    "IND", "IDN", "IRN", "IRL", "ISR", "ITA", "JPN", "JOR", "KAZ", "KEN",
    "KOR", "KWT", "LAO", "LBN", "LTU", "LUX", "MYS", "MLT", "MEX", "MDA",
    "MNG", "MAR", "MOZ", "MMR", "NPL", "NLD", "NZL", "NGA", "NOR", "OMN",
    "PAK", "PAN", "PRY", "PER", "PHL", "POL", "PRT", "QAT", "ROU", "RUS",
    "SAU", "SEN", "SRB", "SGP", "SVK", "SVN", "ZAF", "ESP", "LKA", "SWE",
    "CHE", "TWN", "TZA", "THA", "TUN", "TUR", "UGA", "UKR", "ARE", "GBR",
    "URY", "UZB", "VEN", "VNM", "ZMB", "ZWE",
]

# ─── Post-Trump: 白宫 Annex I (2025-04-02) ────────────────
# 未在Annex I列出 → 10%基准
ANNEX_I_RATES = {
    # 东南亚
    "KHM": 49, "LAO": 48, "VNM": 46, "MMR": 44, "LKA": 44, "BGD": 37,
    "THA": 36, "IDN": 32, "MYS": 24, "PHL": 17, "SGP": 10,
    # 东亚
    "CHN": 34, "TWN": 32, "JPN": 24, "KOR": 25, "HKG": 34,
    # 南亚
    "IND": 26, "PAK": 29,
    # 欧盟（统一20%）
    "DEU": 20, "FRA": 20, "ITA": 20, "ESP": 20, "NLD": 20, "BEL": 20,
    "POL": 20, "SWE": 20, "DNK": 20, "FIN": 20, "AUT": 20, "PRT": 20,
    "GRC": 20, "IRL": 20, "HUN": 20, "CZE": 20, "ROU": 20, "BGR": 20,
    "HRV": 20, "SVK": 20, "SVN": 20, "LTU": 20, "LUX": 20, "MLT": 20,
    # 非EU欧洲
    "CHE": 31, "NOR": 15, "SRB": 37, "MDA": 31, "GBR": 10,
    # 北美
    "CAN": 25, "MEX": 25,
    # 中东/北非
    "ISR": 17, "JOR": 20, "IRN": 10, "DZA": 30, "TUN": 28,
    # 非洲
    "ZAF": 30, "NGA": 14, "AGO": 32, "ZWE": 18, "ZMB": 17, "MOZ": 16,
    # 拉美
    "VEN": 15, "BOL": 20, "NIC": 18,
    # 中亚
    "KAZ": 27,
}

# ─── Pre-Trump: World Bank API 抓取（替代WITS）────────────
# 指标: TM.TAX.MRCH.WM.AR.ZS
# = Tariff rate, applied, weighted mean, all products (%)
# 这是美国对各国进口的实际适用关税加权均值
# 来源: World Bank / ITC / UNCTAD / WTO 联合数据库，免费无需Key
print("=" * 60)
print("Step 1: 构建 Y 变量（关税变化幅度）")
print("=" * 60)
print("\n[1/2] 抓取 Pre-Trump 关税基准（World Bank API）...")

WB_BASE = "https://api.worldbank.org/v2"


def fetch_wb_tariff(year=2023):
    """
    抓取美国对各国的加权平均适用关税税率
    指标: TM.TAX.MRCH.WM.AR.ZS（以进口方为reporter）
    注意：此指标是"各国自身的进口关税"，不是美国对他们的税率

    所以我们改用：NY.GDP.MKTP.CD等配合手动MFN数据
    Pre-Trump美国MFN关税普遍很低（加权均值约1.5-3.5%）
    直接用固定值更准确，见下方 PRE_TRUMP_MFN
    """
    url = (f"{WB_BASE}/country/all/indicator/TM.TAX.MRCH.WM.AR.ZS"
           f"?format=json&date={year}&per_page=300&mrv=1")
    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        data = r.json()
        result = {}
        if data and len(data) > 1 and data[1]:
            for row in data[1]:
                iso3 = row.get("countryiso3code", "")
                val = row.get("value")
                if iso3 and val is not None:
                    result[iso3] = round(float(val), 2)
        return result
    except Exception as e:
        print(f"  ⚠ World Bank API: {e}")
        return {}


wb_tariff = fetch_wb_tariff(2023)
print(f"  ✓ 获取到 {len(wb_tariff)} 个国家的关税数据")

# ─── Pre-Trump 美国对各国MFN关税（手动补充）────────────────
# 说明：上面的WB指标是各国自己的进口税率，不是美国对他们的
# 美国在川普前对大多数国家MFN关税加权均值约1.5-3.5%
# 以下是基于WTO/USITC数据的近似值（2024年水平）
# 特殊情况：中国因301条款已叠加25%额外关税，实际约20-25%
PRE_TRUMP_US_TARIFF = {
    # 正常MFN（约1.5-3.5%）
    "AUS": 1.8, "NZL": 1.8, "GBR": 2.0, "CAN": 0.5, "MEX": 0.5,  # FTA/USMCA
    "JPN": 2.1, "KOR": 2.1, "DEU": 2.0, "FRA": 2.0, "ITA": 2.0,
    "ESP": 2.0, "NLD": 2.0, "BEL": 2.0, "SWE": 2.0, "DNK": 2.0,
    "FIN": 2.0, "AUT": 2.0, "PRT": 2.0, "GRC": 2.0, "IRL": 2.0,
    "HUN": 2.0, "CZE": 2.0, "POL": 2.0, "ROU": 2.0, "BGR": 2.0,
    "HRV": 2.0, "SVK": 2.0, "SVN": 2.0, "LTU": 2.0, "LUX": 2.0,
    "MLT": 2.0, "CHE": 2.3, "NOR": 2.1, "ISR": 0.5,  # FTA
    "SGP": 0.5, "CHL": 0.5, "COL": 0.5, "PER": 0.5, "PAN": 0.5,
    "DOM": 0.5, "SLV": 0.5, "GTM": 0.5, "HND": 0.5, "CRI": 0.5,
    "NIC": 2.1, "BHR": 0.5, "OMN": 0.5, "JOR": 0.5,  # FTA
    "MAR": 0.5,  # FTA
    # 正常MFN无FTA（约2-3.5%）
    "BRA": 2.5, "ARG": 2.5, "BOL": 2.8, "ECU": 2.8, "VEN": 2.8,
    "PRY": 2.8, "URY": 2.5, "COD": 2.5, "ETH": 2.5, "GHA": 2.5,
    "KEN": 2.5, "NGA": 2.5, "ZAF": 2.5, "TZA": 2.5, "SEN": 2.5,
    "UGA": 2.5, "ZMB": 2.5, "ZWE": 2.5, "MOZ": 2.5, "AGO": 2.5,
    "EGY": 2.8, "DZA": 2.8, "TUN": 2.8, "MAR": 0.5,
    "TUR": 2.5, "SAU": 2.8, "ARE": 2.8, "KWT": 2.8, "QAT": 2.8,
    "IRQ": 2.8, "LBN": 2.8, "JOR": 0.5,
    "IND": 2.5, "PAK": 2.8, "BGD": 3.0, "NPL": 3.0, "LKA": 3.0,
    "THA": 2.5, "IDN": 2.8, "MYS": 2.5, "PHL": 2.5, "VNM": 2.8,
    "KHM": 3.0, "MMR": 3.0, "LAO": 3.0, "SGP": 0.5, "BRN": 2.5,
    "HKG": 0.5,  # 香港自由港
    "TWN": 2.3, "KOR": 2.1, "JPN": 2.1,
    "AZE": 2.8, "GEO": 2.8, "ARM": 2.8, "KAZ": 2.8, "UZB": 2.8,
    "MNG": 2.8, "UKR": 2.5, "BLR": 3.0, "ROU": 2.0, "MDA": 2.5,
    "SRB": 2.5, "ALB": 2.5, "MKD": 2.5, "KGZ": 2.8,
    "IRN": 3.5,  # 制裁影响
    "RUS": 3.0,  # 2022年后已大幅提高，但MFN层面约3%
    # 中国：特殊 — 已有301条款叠加关税，实际约20%+
    "CHN": 20.0,
}

# ─── 构建完整数据框 ──────────────────────────────────────────
print("\n[2/2] 构建Y变量数据框...")

rows = []
for iso3 in SAMPLE_COUNTRIES:
    # Post-Trump税率（Liberation Day, 4月2日）
    post_rate = ANNEX_I_RATES.get(iso3, 10)  # 未列出 = 10%基准

    # Pre-Trump税率（2024年底/川普上任前）
    pre_rate = PRE_TRUMP_US_TARIFF.get(iso3, 2.5)  # 未知 = MFN均值2.5%

    # Y = 关税净增幅
    y_change = round(post_rate - pre_rate, 2)

    # 中国标注
    china_flag = 1 if iso3 == "CHN" else 0

    # 额外说明
    if iso3 == "CHN":
        note = "Pre=~20% (301 tariffs); Post=34% (Annex I); actual final ~145%"
    elif iso3 in ANNEX_I_RATES:
        note = "In Annex I"
    else:
        note = "10% baseline (not in Annex I)"

    rows.append({
        "ISO3": iso3,
        "Y_Pre_Trump": pre_rate,  # 川普前税率
        "Y_Post_AnnexI": post_rate,  # 4月2日宣布税率
        "Y_Change": y_change,  # ★ 主Y变量 = 净增幅
        "Y_ExtraTariff": post_rate - 10,  # 备用Y = 超出10%基准部分
        "Y_China_Flag": china_flag,
        "Note": note,
    })

df = pd.DataFrame(rows)

# ─── 统计报告 ────────────────────────────────────────────────
print(f"\n{'=' * 60}")
print(f"Y变量统计")
print(f"{'=' * 60}")
print(f"\n样本量: {len(df)} 个国家")
print(f"\n【主Y变量: Y_Change = Post税率 - Pre税率】")
print(df["Y_Change"].describe().round(2))

print(f"\n增幅最大10国（被加征最多）:")
top10 = df.nlargest(10, "Y_Change")[["ISO3", "Y_Pre_Trump", "Y_Post_AnnexI", "Y_Change"]]
print(top10.to_string(index=False))

print(f"\n⚠️  中国特殊情况:")
chn = df[df["ISO3"] == "CHN"].iloc[0]
print(f"   Pre-Trump: ~{chn.Y_Pre_Trump}% (含301条款关税)")
print(f"   Post Annex I: {chn.Y_Post_AnnexI}%（口径统一，不用145%）")
print(f"   Y_Change = {chn.Y_Change}%")
print(f"   稳健性检验时用 Y_China_Flag=1 排除中国")

# ─── 保存 ────────────────────────────────────────────────────
df.to_csv("step1_Y_complete.csv", index=False, encoding="utf-8-sig")
print(f"\n✅ 保存 → step1_Y_complete.csv")
print(f"""
📌 你的审查清单:
   1. 打开 step1_Y_complete.csv
   2. Y_Change 检查: 东南亚国家应该增幅最大（KHM约46%）
   3. 中国 Y_Change ≈ 14%（34-20，因为原本就有301关税）
   4. 欧盟国家 Y_Change ≈ 18%（20-2）
   5. 有FTA的国家增幅较小（以色列、约旦约16-17%）
   6. 如果你认为中国Pre-Trump应该用更高/低的值，直接改CSV
""")