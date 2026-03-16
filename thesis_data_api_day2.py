import pandas as pd
df = pd.read_excel(r"E:\Desk\python1\outputfinal\final.xlsx", sheet_name="raw_inputs")
imp_col = "US_imp_B_2023" if "US_imp_B_2023" in df.columns else "US_imp_B"
print(df[["iso3", imp_col]].head(10))
print(f"非空数量: {pd.to_numeric(df[imp_col], errors='coerce').notna().sum()}")
