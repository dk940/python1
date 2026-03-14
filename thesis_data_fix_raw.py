"""
=============================================================================
thesis_data_fix_raw.py  ·  修复 raw_inputs 的多行错位问题
=============================================================================
任务：将 raw_inputs 中同一个国家分散在多行的数据，压缩合并为完美的一行。
当前项目路径已更新为: E:\Desk\python1
=============================================================================
"""

import pandas as pd
from pathlib import Path

# ============================================================
# 0. 配置区
# ============================================================
BASE = Path(r"E:\Desk\python1")
FINAL = BASE / "outputfinal"
WORKBOOK = FINAL / "final.xlsx"

print(f"📂 正在读取工作簿: {WORKBOOK.name} ...")
try:
    sheets = pd.read_excel(WORKBOOK, sheet_name=None)
    df_raw = sheets.get("raw_inputs", pd.DataFrame())
except FileNotFoundError:
    print(f"❌ 找不到文件: {WORKBOOK}，请确认 final.xlsx 是否在这个目录下。")
    exit()

if df_raw.empty:
    print("❌ 找不到 raw_inputs 表或表为空。")
    exit()

print(f"   压缩前：raw_inputs 共有 {len(df_raw)} 行。")

# ============================================================
# 1. 执行核心数据压缩 (Squash)
# ============================================================
# 按照 iso3 分组，提取每一列最后一个非空的值（合并分散的行）
df_raw_fixed = df_raw.groupby('iso3').agg(
    lambda x: x.dropna().iloc[-1] if len(x.dropna()) > 0 else pd.NA
).reset_index()

# 保持原有的列顺序
df_raw_fixed = df_raw_fixed[df_raw.columns]

print(f"✅ 压缩后：raw_inputs 恢复为 {len(df_raw_fixed)} 行（每个国家完美对应1行）。")

# ============================================================
# 2. 安全保存
# ============================================================
print("\n📝 正在将修复后的完美表格写回 Excel...")

with pd.ExcelWriter(WORKBOOK, engine="openpyxl", mode="w") as writer:
    for sheet_name, df in sheets.items():
        if sheet_name == "raw_inputs":
            df_raw_fixed.to_excel(writer, sheet_name=sheet_name, index=False)
        else:
            # 其他 sheet 原样写回
            df.to_excel(writer, sheet_name=sheet_name, index=False)

print(f"🎉 修复完成！快去打开 {WORKBOOK} 看看 raw_inputs 是不是变得极其整洁了！")