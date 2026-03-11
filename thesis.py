import pandas as pd

# 读取你现有的数据集（请根据你的实际路径调整文件名）
file_path = 'Ultimate_Regression_Dataset_Template.xlsx'
df = pd.read_excel(file_path)

# 所有 Sheet 都需要保留的基础列（国家标识和因变量）
base_cols = ['Country_Name', 'ISO3_Code', 'Region', 'Y_Initial_Tariff_Threat', 'Y_Final_Implemented_Tariff']

# 定义 6 大框架及包含的列名（包含已有列和将要抓取的新列）
frameworks = {
    '1_经济利益框架': [
        'X_Econ_Export_Dep_US_Pct', 'X_Econ_Export_HHI',
        'X_Econ_Trade_Surplus_Scale', 'X_Econ_HS_Code_Category',
        'X_Econ_US_Tariff_Share', 'X_Econ_Currency_Manipulator'
    ],
    '2_地缘政治框架': [
        'X_Pol_US_Military_Bases', 'X_Pol_Alliance_Level', 'X_Pol_UN_Voting_Align',
        'X_Pol_Distance_to_US_km', 'X_Pol_Ideology_Score', 'X_Cult_Starbucks_McD_Count',
        'X_Pol_NATO_US_Ally', 'X_Pol_Strategic_Competitor'
    ],
    '3_供应链竞争框架': [
        'X_Econ_Export_Dep_CN_Pct', 'X_Econ_HighTech_Dep_US', 'X_Econ_Foreign_Value_Added',
        'X_Econ_Critical_Minerals', 'X_Pol_Entity_List_Count',
        'X_Supply_China_Ind_Link', 'X_Supply_RCEP_BRI', 'X_Supply_Domestic_Value_Added',
        'X_Supply_Critical_Minerals_to_CN'
    ],
    '4_国内政治经济框架': [
        'X_Negot_Lobbying_US_Mil', 'X_Negot_Retaliation_Level',
        'X_Dom_Retaliation_Agri_Auto', 'X_Dom_Swing_State_Impact'
    ],
    '5_制度合规框架': [
        'X_Inst_IP_Protection', 'X_Inst_WTO_Member_Sued', 'X_Inst_Bilateral_FTA',
        'X_Inst_Non_Market_Economy', 'X_Inst_Additional_Clauses'
    ],
    '6_谈判行为框架': [
        'X_Negot_Purchase_Commit_Bil', 'X_Negot_FDI_to_US_Bil', 'X_Negot_Signed_Framework',
        'X_Negot_Attitude_Score', 'X_Negot_Struct_Reform_Accept'
    ]
}

# 创建并写入包含多个 Sheet 的 Excel 文件
output_file = 'Structured_Regression_Dataset.xlsx'
with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
    for sheet_name, cols in frameworks.items():
        # 筛选出原数据中已有的列
        existing_cols = [c for c in cols if c in df.columns]
        # 找出需要留空的新增列
        new_cols = [c for c in cols if c not in df.columns]

        # 创建新的 DataFrame 面板
        sheet_df = df[base_cols + existing_cols].copy()

        # 添加新的空列等待后续爬虫填充
        for c in new_cols:
            sheet_df[c] = None

        # 写入对应 Sheet
        sheet_df.to_excel(writer, sheet_name=sheet_name, index=False)

print(f"数据结构已成功分类并保存至 {output_file}")