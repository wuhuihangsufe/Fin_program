import pandas as pd
# 0
program_path = 'D:\\China\\China_Data\\Fin_program\\'
Start_Date = '1997-01-01'
End_Date = '2019-06-30'

# Merge the IPO dates
ipo_date = pd.read_csv(program_path + 'rawdata\\Ashare_20190911.csv',encoding='gbk')
delist_stocks = pd.read_csv(program_path + 'rawdata\\delist_stock_20190911.csv',encoding='gbk')

stock_list = pd.concat([ipo_date.iloc[:, :3], delist_stocks.iloc[:, :3]])
stock_list.sort_index()
