"""
=============================================================================
thesis_data_api_day2.py  ·  API抓取：DAY1+DAY2 终极修复版
=============================================================================
修复清单：
  1. [DAY1] 由于 raw_inputs 中 US_imp_B 全为 NaN，重新抓取美国双边进出口
     - 回退顺序：2023 → 2022 → 2021
     - 写回列：US_imp_B, US_exp_B, year_us_bilateral, F1_Surplus_B
  2. [DAY2] 加入 breakdownMode=classic，防止多条记录重复累加
     - 这是 DEU 出现 13566B 天文数字的根本原因
  3. [DAY2] 回退：2022 → 2021 → 2020
     - 写回列：total_exp_B, exports_to_china_B, year_exports, day2_year_china, day2_note
  4. 比例变量：F1_ExpDep = US_imp_B / total_exp_B（写主数据集）
               F3_ChinaDep = exports_to_china_B / total_exp_B（写主数据集）
  5. 安全写回：全程 iso3.map().combine_first()，绝不改变行数
=============================================================================
"""

import time
import requests
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

# ============================================================
# 0. 配置区
# ============================================================
BASE = Path("/home/user/python1")
INTER = BASE / "outputintermediate"
FINAL = BASE / "outputfinal"
INTER.mkdir(parents=True, exist_ok=True)

WORKBOOK = FINAL / "final.xlsx"
CACHE_DAY1 = INTER / "ct_bilateral_cache_day1.csv"
CACHE_DAY2 = INTER / "ct_export_deps_cache_day2.csv"

COMTRADE_KEY = "b9e34bf4f2c041c79db8d3a05799162b"
COMTRADE_BASE = "https://comtradeapi.un.org/data/v1/get/C/A/HS"

DAY1_YEAR_CANDIDATES = ["2023", "2022", "2021"]
DAY2_YEAR_CANDIDATES = ["2022", "2021", "2020"]

US_CODE = 842  # 美国 Comtrade 代码

# ============================================================
# 1. 读取 Excel（四个 Sheet）
# ============================================================
print(f"📂 读取工作簿: {WORKBOOK} ...")
sheets = pd.read_excel(WORKBOOK, sheet_name=None)
df_main     = sheets["主数据集"].copy()
df_raw      = sheets.get("raw_inputs", pd.DataFrame()).copy()
df_codebook = sheets.get("变量说明", pd.DataFrame()).copy()
df_todo     = sheets.get("数据收集清单", pd.DataFrame()).copy()

# 行数检查
if len(df_main) != 68 or len(df_raw) != 68:
    print(f"⚠️ 警告：行数异常！主数据集={len(df_main)}, raw_inputs={len(df_raw)}")
if not df_main["iso3"].is_unique:
    raise ValueError("❌ 致命错误：主数据集 iso3 不唯一！")
if not df_raw["iso3"].is_unique:
    raise ValueError("❌ 致命错误：raw_inputs iso3 不唯一！")

print(f"   ✅ 结构检查通过：主数据集={len(df_main)}行，raw_inputs={len(df_raw)}行")

# 检测 US_imp_B 当前状态，决定是否重跑 DAY1
imp_col = "US_imp_B_2023" if "US_imp_B_2023" in df_raw.columns else "US_imp_B"
if imp_col in df_raw.columns:
    us_imp_valid = pd.to_numeric(df_raw[imp_col], errors="coerce").notna().sum()
    print(f"   📊 当前 {imp_col} 非空数量: {us_imp_valid} / 68")
    need_day1 = us_imp_valid < 60  # 超过 8 个缺失则重抓
else:
    print(f"   ⚠️ raw_inputs 中找不到 US_imp_B 类列，执行完整 DAY1 抓取")
    need_day1 = True

# ============================================================
# 2. Comtrade 国家代码映射表
# ============================================================
CT_CODE = {
    "LSO": 426, "KHM": 116, "LAO": 418, "MDG": 450, "VNM": 704,
    "LKA": 144, "MMR": 104, "MUS": 480, "IRQ": 368, "GUY": 328,
    "BGD":  50, "BWA":  72, "SRB": 688, "THA": 764, "HND": 340,
    "CHN": 156, "TWN": 490, "IDN": 360, "AGO":  24, "CHE": 756,
    "LBY": 434, "MDA": 498, "ZAF": 710, "DZA":  12, "PAK": 586,
    "TUN": 788, "KAZ": 398, "IND": 356, "KOR": 410, "JPN": 392,
    "MYS": 458, "NAM": 516, "CIV": 384, "DEU": 276, "FRA": 251,
    "ITA": 381, "ESP": 724, "NLD": 528, "BEL":  56, "POL": 616,
    "SWE": 752, "AUT":  40, "DNK": 208, "FIN": 246, "IRL": 372,
    "PRT": 620, "GRC": 300, "CZE": 203, "HUN": 348, "ROU": 642,
    "SVK": 703, "BGR": 100, "HRV": 191, "JOR": 400, "ZWE": 716,
    "RWA": 646, "NIC": 558, "ISR": 376, "PHL": 608, "MWI": 454,
    "ZMB": 894, "MOZ": 508, "TZA": 834, "NOR": 578, "VEN": 862,
    "NGA": 566, "CMR": 120, "COD": 180,
}

# ============================================================
# 3. 核心 API 请求函数（含 breakdownMode=classic 修复）
# ============================================================
def fetch_trade(reporter_code: int, partner_code: int, flow: str, year: str):
    """
    获取单次贸易数据。
    关键修复：breakdownMode=classic 确保返回单一汇总行，避免按运输方式/海关程序的
    多重切片被 sum() 重复累加（这是 DEU=13566B 的根本原因）。
    flow: "M"=进口, "X"=出口
    返回: float（USD原始值）或 None
    """
    headers = {"Ocp-Apim-Subscription-Key": COMTRADE_KEY}
    params = {
        "reporterCode":  reporter_code,
        "partnerCode":   partner_code,
        "period":        year,
        "cmdCode":       "TOTAL",
        "flowCode":      flow,
        "breakdownMode": "classic",   # ← 核心修复：强制经典汇总模式
        "maxRecords":    500,
        "format":        "JSON",
    }
    try:
        r = requests.get(COMTRADE_BASE, params=params, headers=headers, timeout=25)
        if r.status_code == 200:
            data = r.json().get("data", [])
            if not data:
                return None
            # classic 模式下 TOTAL 应只有 1 条；取 primaryValue 最大值（防御性处理）
            vals = [x.get("primaryValue", 0) or 0 for x in data]
            return max(vals) if vals else None
        if r.status_code == 429:
            print("   ⏳ 触发429限流，等待 65 秒...")
            time.sleep(65)
            return fetch_trade(reporter_code, partner_code, flow, year)
        print(f"   ⚠️ HTTP {r.status_code}: {r.text[:200]}")
        return None
    except Exception as e:
        print(f"   ⚠️ 请求异常: {e}")
        return None


def fetch_with_fallback(reporter_code: int, partner_code: int, flow: str,
                        year_list: list, label: str):
    """带年份回退的抓取，返回 (原始值USD, 使用的年份字符串)"""
    for y in year_list:
        val = fetch_trade(reporter_code, partner_code, flow, y)
        time.sleep(1.2)
        if val is not None and val > 0:
            return val, y
    print(f"   ❌ {label} 连续 {len(year_list)} 年无数据")
    return None, "N/A"


# ============================================================
# 4. DAY1：抓取美国双边进出口（若 US_imp_B 全为 NaN）
# ============================================================
if need_day1:
    print(f"\n{'='*60}")
    print("🔄 开始 DAY1：美国双边进出口（回退 2023→2022→2021）")
    print(f"{'='*60}")

    if CACHE_DAY1.exists():
        df_d1_cache = pd.read_csv(CACHE_DAY1)
        d1_results  = df_d1_cache.to_dict("records")
        d1_done     = set(df_d1_cache["iso3"].astype(str))
        print(f"   ✅ 已载入 DAY1 断点缓存，跳过 {len(d1_done)} 个国家")
    else:
        d1_results = []
        d1_done    = set()

    for iso3 in df_main["iso3"].dropna().astype(str):
        if iso3 in d1_done:
            continue
        ct_code = CT_CODE.get(iso3)
        if not ct_code:
            print(f"   ⚠️ [{iso3}] 无 Comtrade 代码，跳过")
            continue

        print(f"\n🔄 [{iso3}] DAY1 查询...")
        # 美国视角：flowCode=M(进口)表示美国从该国进口；flowCode=X表示美国出口到该国
        imp_val, imp_yr = fetch_with_fallback(US_CODE, ct_code, "M", DAY1_YEAR_CANDIDATES, f"{iso3}美国进口")
        exp_val, exp_yr = fetch_with_fallback(US_CODE, ct_code, "X", DAY1_YEAR_CANDIDATES, f"{iso3}美国出口")

        us_imp_b  = round(imp_val / 1e9, 4) if imp_val else np.nan
        us_exp_b  = round(exp_val / 1e9, 4) if exp_val else np.nan
        surplus_b = (round(us_imp_b - us_exp_b, 4)
                     if pd.notna(us_imp_b) and pd.notna(us_exp_b) else np.nan)

        d1_results.append({
            "iso3":               iso3,
            "US_imp_B":           us_imp_b,
            "US_exp_B":           us_exp_b,
            "year_us_bilateral":  imp_yr if imp_yr != "N/A" else exp_yr,
            "F1_Surplus_B":       surplus_b,
        })
        pd.DataFrame(d1_results).to_csv(CACHE_DAY1, index=False)
        print(f"   ✅ 进口({imp_yr}): {us_imp_b}B | 出口({exp_yr}): {us_exp_b}B | 逆差: {surplus_b}B")

    df_day1 = pd.DataFrame(d1_results)

    # 写回 raw_inputs
    for col in ["US_imp_B", "US_exp_B", "year_us_bilateral"]:
        m = dict(zip(df_day1["iso3"], df_day1[col]))
        if col not in df_raw.columns:
            df_raw[col] = np.nan
        df_raw[col] = df_raw["iso3"].map(m).combine_first(df_raw[col])

    # 写回 主数据集
    surplus_m = dict(zip(df_day1["iso3"], df_day1["F1_Surplus_B"]))
    if "F1_Surplus_B" not in df_main.columns:
        df_main["F1_Surplus_B"] = np.nan
    df_main["F1_Surplus_B"] = df_main["iso3"].map(surplus_m).combine_first(df_main["F1_Surplus_B"])

    valid_now = pd.to_numeric(df_raw["US_imp_B"], errors="coerce").notna().sum()
    print(f"\n   ✅ DAY1 写回完成（US_imp_B 非空: {valid_now}/68）")
else:
    print("\n⏩ DAY1 数据充足，跳过重新抓取")
    df_day1 = pd.DataFrame()


# ============================================================
# 5. DAY2：各国总出口 + 对华出口（回退 2022→2021→2020）
# ============================================================
print(f"\n{'='*60}")
print("🌐 开始 DAY2：各国总出口 + 对华出口（回退 2022→2021→2020）")
print(f"{'='*60}")

if CACHE_DAY2.exists():
    df_d2_cache = pd.read_csv(CACHE_DAY2)
    d2_results  = df_d2_cache.to_dict("records")
    d2_done     = set(df_d2_cache["iso3"].astype(str))
    print(f"   ✅ 已载入 DAY2 断点缓存，跳过 {len(d2_done)} 个国家")
else:
    d2_results = []
    d2_done    = set()

# 用 DAY1 刚写回的 US_imp_B 作为 F1_ExpDep 的分子
us_imp_series = pd.to_numeric(df_raw.get("US_imp_B", pd.Series(dtype=float)), errors="coerce")
us_imp_map    = dict(zip(df_raw["iso3"], us_imp_series))

for iso3 in df_main["iso3"].dropna().astype(str):
    if iso3 in d2_done:
        continue
    ct_code = CT_CODE.get(iso3)
    if not ct_code:
        print(f"   ⚠️ [{iso3}] 无 Comtrade 代码，跳过")
        continue

    print(f"\n🔄 [{iso3}] DAY2 查询...")
    # 各国为报告方，出口到全球(partnerCode=0)
    total_val, yr_total = fetch_with_fallback(ct_code, 0,   "X", DAY2_YEAR_CANDIDATES, f"{iso3}总出口")
    # 各国为报告方，出口到中国(partnerCode=156)
    china_val, yr_china = fetch_with_fallback(ct_code, 156, "X", DAY2_YEAR_CANDIDATES, f"{iso3}对华出口")

    total_b = round(total_val / 1e9, 4) if total_val else np.nan
    china_b = round(china_val / 1e9, 4) if china_val else np.nan
    us_imp_b = us_imp_map.get(iso3, np.nan)

    # 比例计算（0~1）
    f1_expdep   = (round(us_imp_b / total_b, 4)
                   if pd.notna(total_b) and total_b > 0 and pd.notna(us_imp_b) else np.nan)
    f3_chinadep = (round(china_b  / total_b, 4)
                   if pd.notna(total_b) and total_b > 0 and pd.notna(china_b)  else np.nan)

    d2_results.append({
        "iso3":                iso3,
        "total_exp_B":         total_b,
        "exports_to_china_B":  china_b,    # 匹配 raw_inputs 现有列名
        "year_exports":        yr_total,   # 匹配 raw_inputs 现有列名
        "day2_year_china":     yr_china,
        "day2_note":           f"Total:{yr_total}|China:{yr_china}",
        "F1_ExpDep":           f1_expdep,
        "F3_ChinaDep":         f3_chinadep,
        "updated_at":          datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    })
    pd.DataFrame(d2_results).to_csv(CACHE_DAY2, index=False)

    print(f"   ✅ Total({yr_total}): {total_b}B | China({yr_china}): {china_b}B")
    print(f"   📊 F1_ExpDep={f1_expdep} | F3_ChinaDep={f3_chinadep}")

df_day2 = pd.DataFrame(d2_results)
if df_day2.empty:
    raise SystemExit("⚠️ DAY2 无新结果，请检查 API Key 或网络。")

# ============================================================
# 6. 精准列映射写回（iso3.map().combine_first()）
# ============================================================
print("\n📝 精准写回 raw_inputs ...")
for col in ["total_exp_B", "exports_to_china_B", "year_exports",
            "day2_year_china", "day2_note", "updated_at"]:
    m = dict(zip(df_day2["iso3"], df_day2[col]))
    if col not in df_raw.columns:
        df_raw[col] = np.nan
    df_raw[col] = df_raw["iso3"].map(m).combine_first(df_raw[col])

print("📝 精准写回 主数据集 ...")
for col in ["F1_ExpDep", "F3_ChinaDep"]:
    m = dict(zip(df_day2["iso3"], df_day2[col]))
    if col not in df_main.columns:
        df_main[col] = np.nan
    df_main[col] = df_main["iso3"].map(m).combine_first(df_main[col])

# ============================================================
# 7. 行数核验
# ============================================================
assert len(df_main) == 68, f"❌ 主数据集行数变化：{len(df_main)}"
assert len(df_raw)  == 68, f"❌ raw_inputs 行数变化：{len(df_raw)}"
print(f"   ✅ 行数核验通过：68行×68行")

# ============================================================
# 8. 数据质量摘要
# ============================================================
exp_dep_valid   = pd.to_numeric(df_main.get("F1_ExpDep",    pd.Series()), errors="coerce").notna().sum()
china_dep_valid = pd.to_numeric(df_main.get("F3_ChinaDep",  pd.Series()), errors="coerce").notna().sum()
surplus_valid   = pd.to_numeric(df_main.get("F1_Surplus_B", pd.Series()), errors="coerce").notna().sum()
us_imp_valid2   = pd.to_numeric(df_raw.get("US_imp_B",      pd.Series()), errors="coerce").notna().sum()

print(f"\n📊 数据质量摘要:")
print(f"   US_imp_B  (raw_inputs)  非空: {us_imp_valid2}/68")
print(f"   F1_Surplus_B (主数据集) 非空: {surplus_valid}/68")
print(f"   F1_ExpDep    (主数据集) 非空: {exp_dep_valid}/68")
print(f"   F3_ChinaDep  (主数据集) 非空: {china_dep_valid}/68")

# F1_ExpDep 范围检查
if exp_dep_valid > 0:
    vals = pd.to_numeric(df_main["F1_ExpDep"], errors="coerce").dropna()
    out_of_range = ((vals < 0) | (vals > 1)).sum()
    if out_of_range > 0:
        bad_idx = df_main.index[pd.to_numeric(df_main["F1_ExpDep"], errors="coerce").pipe(
            lambda s: (s < 0) | (s > 1)
        ).fillna(False)]
        print(f"   ⚠️ F1_ExpDep 超出[0,1]范围 {out_of_range} 个：")
        print(df_main.loc[bad_idx, ["iso3", "F1_ExpDep"]])

# ============================================================
# 9. 安全保存（覆写全部四个 Sheet）
# ============================================================
print(f"\n💾 保存到 {WORKBOOK.name} ...")
with pd.ExcelWriter(WORKBOOK, engine="openpyxl", mode="w") as writer:
    df_main.to_excel(writer,     sheet_name="主数据集",     index=False)
    df_raw.to_excel(writer,      sheet_name="raw_inputs",   index=False)
    if not df_codebook.empty:
        df_codebook.to_excel(writer, sheet_name="变量说明",     index=False)
    if not df_todo.empty:
        for idx, row in df_todo.iterrows():
            if pd.isna(row.get("目标变量")):
                continue
            target = str(row["目标变量"])
            if any(v in target for v in ["F1_ExpDep", "F3_ChinaDep", "F1_Surplus_B", "US_imp_B"]):
                status_cols = [c for c in df_todo.columns if "状态" in c or "操作" in c]
                if status_cols:
                    df_todo.at[idx, status_cols[0]] = "✅ 已完成"
        df_todo.to_excel(writer, sheet_name="数据收集清单", index=False)

print(f"\n🎉 全部完成！DAY1+DAY2 数据已写回：{WORKBOOK}")
print("下一步：Polity5 (F2_Polity) → Voeten UN Voting (F2_UNVote)")
