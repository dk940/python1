"""
=============================================================================
thesis_data_api_day1.py  ·  API 抓取：美国双边贸易逆差数据（更新 final.xlsx）
=============================================================================
任务：
1. 抓取美国对样本国 2023 年进口额 / 出口额
2. 计算 F1_Surplus_B = US_imp_B_2023 - US_exp_B_2023
3. 将原始结果写入 raw_inputs
4. 仅将 F1_Surplus_B 回填到 主数据集
=============================================================================
"""

import time
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime

# ============================================================
# 0. 配置区
# ============================================================
BASE = Path(r"E:\Desk\thesis")
INTER = BASE / "outputintermediate"
FINAL = BASE / "outputfinal"
INTER.mkdir(parents=True, exist_ok=True)

WORKBOOK = FINAL / "final.xlsx"
CACHE_FILE = INTER / "ct_us_bilateral_cache_day1.csv"

COMTRADE_KEY = "b9e34bf4f2c041c79db8d3a05799162b"
COMTRADE_BASE = "https://comtradeapi.un.org/data/v1/get/C/A/HS"

YEAR = "2023"
SOURCE_TAG = f"UN Comtrade DAY1 {YEAR}"

# ============================================================
# 1. 读取工作簿
# ============================================================
if not WORKBOOK.exists():
    raise FileNotFoundError(f"找不到工作簿: {WORKBOOK}")

sheets = pd.read_excel(WORKBOOK, sheet_name=None)

if "主数据集" not in sheets:
    raise ValueError("final.xlsx 缺少 sheet：主数据集")

df_main = sheets["主数据集"].copy()
df_codebook = sheets.get("变量说明", pd.DataFrame())
df_todo = sheets.get("数据收集清单", pd.DataFrame())
df_raw = sheets.get("raw_inputs", pd.DataFrame())

required_cols = ["iso3"]
for col in required_cols:
    if col not in df_main.columns:
        raise ValueError(f"主数据集缺少必要列: {col}")

if "F1_Surplus_B" not in df_main.columns:
    df_main["F1_Surplus_B"] = pd.NA

# 如果 raw_inputs 不存在或为空，初始化
raw_required_cols = [
    "iso3", "source_task", "year",
    "US_imp_B_2023", "US_exp_B_2023", "F1_Surplus_B",
    "source", "updated_at", "note"
]
if df_raw.empty:
    df_raw = pd.DataFrame(columns=raw_required_cols)
else:
    for col in raw_required_cols:
        if col not in df_raw.columns:
            df_raw[col] = pd.NA

print(f"✅ 已读取工作簿: {WORKBOOK.name}")
print(f"   主数据集样本数: {len(df_main)}")

# ============================================================
# 2. Comtrade 国家代码映射
# ============================================================
CT_CODE = {
    "LSO":426,"KHM":116,"LAO":418,"MDG":450,"VNM":704,"LKA":144,"MMR":104,"MUS":480,
    "IRQ":368,"GUY":328,"BGD":50,"BWA":72,"SRB":688,"THA":764,"HND":340,"CHN":156,
    "TWN":490,"IDN":360,"AGO":24,"CHE":756,"LBY":434,"MDA":498,"ZAF":710,"DZA":12,
    "PAK":586,"TUN":788,"KAZ":398,"IND":356,"KOR":410,"JPN":392,"MYS":458,"NAM":516,
    "CIV":384,"DEU":276,"FRA":251,"ITA":381,"ESP":724,"NLD":528,"BEL":56,"POL":616,
    "SWE":752,"AUT":40,"DNK":208,"FIN":246,"IRL":372,"PRT":620,"GRC":300,"CZE":203,
    "HUN":348,"ROU":642,"SVK":703,"BGR":100,"HRV":191,"JOR":400,"ZWE":716,"RWA":646,
    "NIC":558,"ISR":376,"PHL":608,"MWI":454,"ZMB":894,"MOZ":508,"TZA":834,"NOR":578,
    "VEN":862,"NGA":566,"CMR":120,"COD":180
}

# ============================================================
# 3. API 请求函数
# ============================================================
def fetch_flow(partner_code: int, flow_code: str, year: str = "2023"):
    headers = {"Ocp-Apim-Subscription-Key": COMTRADE_KEY}
    params = {
        "reporterCode": 842,          # USA
        "partnerCode": partner_code,
        "period": year,
        "cmdCode": "TOTAL",
        "flowCode": flow_code,        # M / X
        "maxRecords": 500,
        "format": "JSON",
        "breakdownMode": "classic",
        "includeDesc": "false"
    }
    try:
        r = requests.get(COMTRADE_BASE, params=params, headers=headers, timeout=25)
        if r.status_code == 200:
            recs = r.json().get("data", [])
            return sum(x.get("primaryValue", 0) or 0 for x in recs)

        if r.status_code == 429:
            print("   ⏳ 触发 429，休眠 65 秒后重试...")
            time.sleep(65)
            return fetch_flow(partner_code, flow_code, year)

        print(f"   ⚠️ HTTP {r.status_code}")
        return None

    except Exception as e:
        print(f"   ⚠️ 请求异常: {e}")
        return None


def get_us_bilateral_trade(partner_code: int, year: str = "2023"):
    us_imp = fetch_flow(partner_code, "M", year)
    time.sleep(1.5)
    us_exp = fetch_flow(partner_code, "X", year)
    time.sleep(1.5)
    return us_imp, us_exp

# ============================================================
# 4. 加载缓存（断点续传）
# ============================================================
if CACHE_FILE.exists():
    df_cache = pd.read_csv(CACHE_FILE)
    results = df_cache.to_dict("records")
    done_isos = set(df_cache["iso3"].astype(str).tolist())
    print(f"✅ 已加载缓存，已完成 {len(done_isos)} 个国家")
else:
    results = []
    done_isos = set()

# ============================================================
# 5. 批量抓取
# ============================================================
print("\n🌐 开始抓取 DAY1：美国双边进出口数据")

for iso3 in df_main["iso3"].dropna().astype(str):
    if iso3 in done_isos:
        continue

    ct_code = CT_CODE.get(iso3)
    if not ct_code:
        print(f"   ⏭️ {iso3}: 缺少 Comtrade code，跳过")
        continue

    print(f"   🔄 {iso3} ...", end=" ")
    us_imp, us_exp = get_us_bilateral_trade(ct_code, YEAR)

    if us_imp is None or us_exp is None:
        print("❌ 失败")
        continue

    imp_b = round(us_imp / 1e9, 4)
    exp_b = round(us_exp / 1e9, 4)
    surplus_b = round(imp_b - exp_b, 4)

    row = {
        "iso3": iso3,
        "source_task": "DAY1_US_bilateral_trade",
        "year": int(YEAR),
        "US_imp_B_2023": imp_b,
        "US_exp_B_2023": exp_b,
        "F1_Surplus_B": surplus_b,
        "source": SOURCE_TAG,
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "note": ""
    }
    results.append(row)

    pd.DataFrame(results).to_csv(CACHE_FILE, index=False)

    print(f"✅ imp={imp_b:.2f}B exp={exp_b:.2f}B surplus={surplus_b:.2f}B")

# ============================================================
# 6. 整理 DAY1 结果
# ============================================================
df_day1 = pd.DataFrame(results)

if df_day1.empty:
    print("\n⚠️ 本次没有抓到新结果，停止写回。")
    raise SystemExit

# 去重：同一 iso3 保留最后一次
df_day1 = df_day1.drop_duplicates(subset=["iso3"], keep="last").copy()

# ============================================================
# 7. 更新 raw_inputs
# ============================================================
# 先删除已有同任务旧记录，再追加新记录
if not df_raw.empty and "source_task" in df_raw.columns:
    df_raw = df_raw[df_raw["source_task"] != "DAY1_US_bilateral_trade"].copy()

df_raw_updated = pd.concat([df_raw, df_day1], ignore_index=True)

# ============================================================
# 8. 回填 主数据集（只更新 F1_Surplus_B）
# ============================================================
surplus_map = dict(zip(df_day1["iso3"], df_day1["F1_Surplus_B"]))

df_main["F1_Surplus_B"] = df_main["iso3"].map(surplus_map).combine_first(df_main["F1_Surplus_B"])

# ============================================================
# 9. 可选：更新 数据收集清单
# ============================================================
if not df_todo.empty:
    possible_name_cols = [c for c in df_todo.columns if "变量" in str(c) or "var" in str(c).lower()]
    possible_status_cols = [c for c in df_todo.columns if "状态" in str(c) or "status" in str(c).lower()]
    possible_note_cols = [c for c in df_todo.columns if "备注" in str(c) or "note" in str(c).lower()]

    if possible_name_cols and possible_status_cols:
        name_col = possible_name_cols[0]
        status_col = possible_status_cols[0]
        note_col = possible_note_cols[0] if possible_note_cols else None

        mask = df_todo[name_col].astype(str).str.strip().eq("F1_Surplus_B")
        if mask.any():
            df_todo.loc[mask, status_col] = "已完成"
            if note_col:
                df_todo.loc[mask, note_col] = f"{SOURCE_TAG} 已抓取并回填"

# ============================================================
# 10. 写回 final.xlsx（保留全部 sheet）
# ============================================================
with pd.ExcelWriter(WORKBOOK, engine="openpyxl", mode="w") as writer:
    df_main.to_excel(writer, sheet_name="主数据集", index=False)

    if not df_codebook.empty:
        df_codebook.to_excel(writer, sheet_name="变量说明", index=False)

    if not df_todo.empty:
        df_todo.to_excel(writer, sheet_name="数据收集清单", index=False)

    df_raw_updated.to_excel(writer, sheet_name="raw_inputs", index=False)

print(f"\n🎉 DAY1 完成，已写回: {WORKBOOK}")
print("   raw_inputs 已新增 US_imp_B_2023 / US_exp_B_2023 / F1_Surplus_B")
print("   主数据集 已回填 F1_Surplus_B")