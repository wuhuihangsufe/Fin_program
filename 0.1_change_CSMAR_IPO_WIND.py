'''
File information
input: ER_IPO.xlsx
output: A share IPO_list
'''

import pandas as pd

# 0
program_path = 'D:\\China\\China_Data\\Fin_program\\'
Start_Date = '1997-01-01'
End_Date = '2019-06-30'
save_list = ['permno', 'ShortName', 'ListedDate', 'DelistedDate',
             'IPOIndustryCode', 'IPOIndustryName', 'LatestIndustryName',
             'LatestIndustryCode']

IPO = pd.read_excel(program_path + 'rawdata\\ER_IPO.xlsx', encoding='utf-16')
IPO = IPO.iloc[2:,:]
IPO['ListedDate'] = pd.to_datetime(IPO['ListedDate'], format='%Y/%m/%d')
IPO['DelistedDate'] = pd.to_datetime(IPO['DelistedDate'], format='%Y/%m/%d')
IPO['permno'] = IPO['Symbol'].copy()
IPO['Symbol'] = IPO['Symbol'].apply(lambda x: x[:3])
# 保留A股
IPO = IPO[
    (IPO['permno'].str.startswith('0')) | (IPO['permno'].str.startswith('3')) | (IPO['permno'].str.startswith('6'))]
# 剔除 688开头的科创板
IPO = IPO[IPO['Symbol'] != '688']

# 调整和剔除代码变更
IPO['permno'].replace('000022', '001872', inplace=True)
IPO['permno'].replace('601313', '601360', inplace=True)

IPO.drop(index=IPO[IPO['permno'] == '601975'].index, inplace=True)
IPO['permno'].replace('600087', '601975', inplace=True)
IPO.drop(index=IPO[IPO['permno'] == '600849'].index, inplace=True)
# debug for 600018
temp_index = IPO[(IPO['permno'] == '600018') & (IPO['ListedDate'] == pd.datetime(2006, 10, 26))].index
IPO.loc[temp_index, 'permno'] = 'T00018'
IPO.drop(index=IPO[IPO['permno'] == 'T00018'].index, inplace=True)
IPO_sub = IPO.loc[:, save_list]

IPO_sub.to_pickle(program_path + 'temp\\IPO_list.pkl')

IPO_sub.to_csv(program_path + 'temp\\IPO_list.csv')
