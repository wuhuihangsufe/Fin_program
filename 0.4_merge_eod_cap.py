'''
File information
input:
Daily_return dataset file: Daily_return.pickle
Daily_return dataset file: Cap_data.pickle
output:
Daily_return_with_cap.pickle
'''
import pandas as pd
import datetime
# 0
program_path = 'D:\\China\\China_Data\\Fin_program\\'
Data_path = 'F:\\WindDataBase\\'
Start_Date = '1997-01-01'
End_Date = '2019-06-30'

cap_merge_var = "CHANGE_DT"
Eod_data_raw = pd.read_pickle(program_path + 'temp\\Daily_return.pickle')
Cap_data_raw_sub = pd.read_pickle(program_path + 'temp\\Cap_data.pickle')
Cap_data_raw_sub["TRADE_DT"] = Cap_data_raw_sub[cap_merge_var].copy()

# use tree file to merge incase of not trading day
# Cap_data_raw_sub1 = Cap_data_raw_sub.copy()
# Cap_data_raw_sub1["TRADE_DT"] = Cap_data_raw_sub1["TRADE_DT"] + datetime.timedelta(days=1)
# Cap_data_raw_sub2 = Cap_data_raw_sub.copy()
# Cap_data_raw_sub2["TRADE_DT"] = Cap_data_raw_sub2["TRADE_DT"] + datetime.timedelta(days=2)
# Cap_data_raw_sub3 = Cap_data_raw_sub.copy()
# Cap_data_raw_sub3["TRADE_DT"] = Cap_data_raw_sub3["TRADE_DT"] + datetime.timedelta(days=3)
#
# Cap_data_raw = Cap_data_raw_sub.append(Cap_data_raw_sub1)
# Cap_data_raw = Cap_data_raw.append(Cap_data_raw_sub2)
# Cap_data_raw = Cap_data_raw.append(Cap_data_raw_sub3)

# Try weekend ways clear
# Cap_data_raw_sub['weekday'] = Cap_data_raw_sub["TRADE_DT"].apply(lambda x: x.weekday())
# print(Cap_data_raw_sub['weekday'].value_counts())
# temp_index = Cap_data_raw_sub[Cap_data_raw_sub["weekday"] == 5].index
# Cap_data_raw_sub.loc[temp_index, "TRADE_DT"] = Cap_data_raw_sub.loc[temp_index, "TRADE_DT"] + datetime.timedelta(days=2)
# temp_index = Cap_data_raw_sub[Cap_data_raw_sub["weekday"] == 6].index
# Cap_data_raw_sub.loc[temp_index, "TRADE_DT"] = Cap_data_raw_sub.loc[temp_index, "TRADE_DT"] + datetime.timedelta(days=1)


Cap_data_raw_sub.drop_duplicates(["TRADE_DT", 'permno'], inplace=True, keep='last')


Merge_data = Eod_data_raw.merge(Cap_data_raw_sub, how='outer', on=["TRADE_DT", 'permno'],
                                indicator=True, validate='1:1')
del Eod_data_raw
del Cap_data_raw_sub
print(Merge_data['_merge'].value_counts())
# temp_check = Merge_data[Merge_data['_merge'] == 'right_only']
# Merge_data = Merge_data[Merge_data['_merge'] != 'right_only']

#
del Merge_data["CHANGE_DT1"]
del Merge_data["CHANGE_DT"]
del Merge_data["S_INFO_WINDCODE_y"]
Merge_data.sort_values(['permno', "TRADE_DT"], inplace=True)
Merge_data.reset_index(inplace=True, drop=True)



# fillna with first data
# TODO 如果第一个股票第一条股本数据缺失将会产生问题

fillna_data = pd.DataFrame()
cap_list = ['TOT_SHR', 'FLOAT_SHR', 'FLOAT_A_SHR', 'NON_TRADABLE_SHR', 'S_SHARE_TOTALA']
firm_permno_list = Merge_data['permno'].unique()
for i_firm in firm_permno_list:
    print(i_firm)
    temp_index = Merge_data[Merge_data['permno'] == i_firm].index
    temp_data = Merge_data.loc[temp_index, cap_list]
    temp_data.fillna(method='ffill', inplace=True, axis=0)
    fillna_data = fillna_data.append(temp_data)

Merge_data.loc[:,cap_list] = fillna_data.values
del fillna_data
# Merge_data.fillna(method='ffill', inplace=True, axis=0)
# temp_check = Merge_data[Merge_data['permno'] == '000001']
# temp_check.loc[4500:4600]
# temp_check.fillna(method='pad', inplace=True, axis=0)
Merge_data = Merge_data[Merge_data['_merge'] != 'right_only']
print(Merge_data['_merge'].value_counts())
del Merge_data['_merge']

Merge_data.loc[:,cap_list].isnull().sum()

Merge_data.to_pickle(program_path + 'temp\\Daily_return_with_cap.pkl')
