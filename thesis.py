import pandas as pd
import requests
import os

# 1. 设置文件路径 - 匹配你文件夹里实际的文件名
template_file = "Ultimate_Regression_Dataset_Template.xlsx"
data_file = "interventions.csv"
output_file = "Updated_Thesis_Data.xlsx"

print("🚀 开始处理毕业论文数据...")

# 2. 读取你的初始模板 (注意：这里改成了 read_excel 来读取 .xlsx 文件)
if os.path.exists(template_file):
    try:
        # 你的模板是 Excel 格式，所以必须用 read_excel
        df = pd.read_excel(template_file)
        print(f"✅ 已加载模板，共 {len(df)} 行数据。")
    except Exception as e:
        print(f"❌ 读取 Excel 失败！错误信息: {e}")
        print("💡 提示：请确保你在 PyCharm 终端输入过 pip install openpyxl")
        exit()
else:
    print(f"❌ 找不到模板文件: {template_file}")
    # 打印当前文件夹下的所有文件，帮你排查问题
    print(f"💡 当前文件夹里的文件有: {os.listdir('.')}")
    exit()

# 3. 从 World Bank API 获取宏观数据
def get_wb_data(indicator, column_name):
    print(f"🌐 正在获取世界银行指标: {indicator}...")
    url = f"http://api.worldbank.org/v2/country/all/indicator/{indicator}?format=json&date=2023&per_page=300"
    try:
        response = requests.get(url)
        raw_data = response.json()[1]
        val_map = {item['countryiso3code']: item['value'] for item in raw_data if item['value'] is not None}
        df[column_name] = df['ISO3_Code'].map(val_map)
        print(f"   已填充 {column_name} 变量。")
    except Exception as e:
        print(f"   ⚠️ 获取 {indicator} 失败。")

# 填充：贸易占GDP比重
get_wb_data("NE.TRD.GNFS.ZS", "X_Econ_Export_Dep_US_Pct")

# 4. 整合你已有的 interventions.csv 数据 (这个是 CSV，所以用 read_csv)
if os.path.exists(data_file):
    print(f"📊 正在从 {data_file} 提取干预措施计数...")
    inter_df = pd.read_csv(data_file)
    # 统计每个国家出现的次数
    counts = inter_df['Implementing Jurisdiction'].value_counts()
    df['X_Negot_Retaliation_Level'] = df['Country_Name'].map(counts).fillna(0)
    print("   已根据干预记录填充反制烈度分值。")

# 5. 保存结果为新的 Excel
df.to_excel(output_file, index=False)
print(f"\n🎉 处理完成！请查看文件夹里的：{output_file}")