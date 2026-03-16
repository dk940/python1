"""
=============================================================================
thesis_data_emergency_restore.py  ·  一键急救：从缓存恢复 DAY1 并修复依赖度 (完整版)
=============================================================================
任务：
1. 从 ct_us_bilateral_cache_day1.csv 读取丢失的 DAY1 数据。
2. 完美回填到 final.xlsx 的 raw_inputs 和 主数据集 中。
3. 结合 DAY2 已经拿到的总出口，重新计算 F1_ExpDep。
4. 完整保留所有 Sheet 结构并安全落盘。
=============================================================================
"""
import pandas as pd
import numpy as np
from pathlib import Path

# ============================================================
# 1. 配置路径
# ============================================================
BASE = Path(r"E:\Desk\python1")
WORKBOOK = BASE / "outputfinal" / "final.xlsx"
DAY1_CACHE = BASE / "outputintermediate" / "ct_us_bilateral_cache_day1.csv"

print(f"📂 读取工作簿: {WORKBOOK.name}")
try:
    sheets = pd.read_excel(WORKBOOK, sheet_name=None)
    df_main = sheets["主数据集"]
    df_raw = sheets["raw_inputs"]
except Exception as e:
    raise RuntimeError(f"❌ 读取 Excel 失败，请确保文件没有在 Excel 中处于打开锁定状态。错误: {e}")

# ============================================================
# 2. 验证缓存是否存在
# ============================================================
if not DAY1_CACHE.exists():
    raise FileNotFoundError(f"❌ 找不到 DAY1 缓存文件：{DAY1_CACHE}。无法执行急救。")

print("🚑 正在从本地缓存中唤醒 DAY1 数据...")
df_day1 = pd.read_csv(DAY1_CACHE)

# 智能识别缓存和目标表里的列名（防止由于版本变化导致的 KeyError）
cache_imp_col = "US_imp_B_2023" if "US_imp_B_2023" in df_day1.columns else "US_imp_B"
cache_exp_col = "US_exp_B_2023" if "US_exp_B_2023" in df_day1.columns else "US_exp_B"

raw_imp_col = "US_imp_B_2023" if "US_imp_B_2023" in df_raw.columns else "US_imp_B"
raw_exp_col = "US_exp_B_2023" if "US_exp_B_2023" in df_raw.columns else "US_exp_B"

# ============================================================
# 3. 映射回 raw_inputs (原料回仓)
# ============================================================
print(f"💉 正在将 DAY1 原始进出口数据重新注入 raw_inputs (列: {raw_imp_col})...")
if cache_imp_col in df_day1.columns:
    m_imp = dict(zip(df_day1["iso3"], df_day1[cache_imp_col]))
    if raw_imp_col not in df_raw.columns: df_raw[raw_imp_col] = np.nan
    df_raw[raw_imp_col] = df_raw["iso3"].map(m_imp).combine_first(df_raw[raw_imp_col])

if cache_exp_col in df_day1.columns:
    m_exp = dict(zip(df_day1["iso3"], df_day1[cache_exp_col]))
    if raw_exp_col not in df_raw.columns: df_raw[raw_exp_col] = np.nan
    df_raw[raw_exp_col] = df_raw["iso3"].map(m_exp).combine_first(df_raw[raw_exp_col])

# ============================================================
# 4. 映射逆差到 主数据集 (成品上桌)
# ============================================================
print("💉 正在修复 主数据集 的 F1_Surplus_B...")
if "F1_Surplus_B" in df_day1.columns:
    m_surplus = dict(zip(df_day1["iso3"], df_day1["F1_Surplus_B"]))
    if "F1_Surplus_B" not in df_main.columns:
        df_main["F1_Surplus_B"] = np.nan
    df_main["F1_Surplus_B"] = df_main["iso3"].map(m_surplus).combine_first(df_main["F1_Surplus_B"])

# ============================================================
# 5. 重新计算 F1_ExpDep (对美出口依赖度)
# ============================================================
print("🧮 正在使用 DAY1(恢复的分子) 和 DAY2(已有的分母) 重新计算对美依赖度...")
fix_count = 0

# 智能识别 DAY2 的总出口列名
tot_col = "total_exp_B_2022" if "total_exp_B_2022" in df_raw.columns else "total_exp_B"

# 构建快速查找字典，加速计算
imp_dict = dict(zip(df_raw["iso3"], pd.to_numeric(df_raw[raw_imp_col], errors='coerce')))
tot_dict = dict(zip(df_raw["iso3"], pd.to_numeric(df_raw[tot_col], errors='coerce')))

if "F1_ExpDep" not in df_main.columns:
    df_main["F1_ExpDep"] = np.nan

for idx, row in df_main.iterrows():
    iso = row["iso3"]
    imp = imp_dict.get(iso)
    tot = tot_dict.get(iso)

    if pd.notna(imp) and pd.notna(tot) and tot > 0:
        f1_expdep = round(imp / tot, 4)  # 计算比例，保留4位小数
        df_main.at[idx, "F1_ExpDep"] = f1_expdep
        fix_count += 1

print(f"✅ 成功抢救并计算了 {fix_count} 个国家的 F1_ExpDep！")

# ============================================================
# 6. 完整、安全的保存逻辑
# ============================================================
print("📝 正在安全保存回 final.xlsx ...")
with pd.ExcelWriter(WORKBOOK, engine="openpyxl", mode="w") as writer:
    for sheet_name, df in sheets.items():
        # 这里自动包含了被修改过的 df_main 和 df_raw，以及完好无损的其他表
        df.to_excel(writer, sheet_name=sheet_name, index=False)

print("🎉 满血复活！急救完成，现在去打开 final.xlsx 检查成果吧！")