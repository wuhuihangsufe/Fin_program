'''
File information
input: ER_IPO.xlsx
output: A share IPO_list
'''

import pandas as pd

# 0
program_path = 'D:\\China\\China_Data\\Fin_program\\'
Data_path = 'F:\\WindDataBase\\'
Start_Date = '1997-01-01'
End_Date = '2019-06-30'

# 0.1 Save list of variables
save_list = ['S_INFO_WINDCODE', 'CHANGE_DT1', 'permno', 'TOT_SHR', 'FLOAT_SHR',
             'FLOAT_A_SHR', 'NON_TRADABLE_SHR', 'S_SHARE_TOTALA','CHANGE_DT']

# 0.2 load data
Cap_data_raw = pd.read_csv(Data_path + 'asharecapitalization.csv')
Cap_data_raw.sort_values(['WIND_CODE', 'CHANGE_DT1'], inplace=True)

Cap_data_raw['permno'] = Cap_data_raw['S_INFO_WINDCODE'].apply(lambda x: x[:6])
Cap_data_raw['permno'].replace('T00018', '600018', inplace=True)


def get_A_share(df_temp):
    df_temp = df_temp[(df_temp['permno'].str.startswith('0')) | (df_temp['permno'].str.startswith('3'))
                      | (df_temp['permno'].str.startswith('6'))]
    df_temp['Symbol'] = df_temp['permno'].apply(lambda x: x[:3])
    df_temp = df_temp[df_temp['Symbol'] != '688']
    del df_temp['Symbol']
    return df_temp


Cap_data_raw = get_A_share(df_temp=Cap_data_raw)
Cap_data_raw_sub = Cap_data_raw.loc[:, save_list]
Cap_data_raw_sub['CHANGE_DT'] = pd.to_datetime(Cap_data_raw_sub['CHANGE_DT'], format='%Y%m%d')
Cap_data_raw_sub['CHANGE_DT1'] = pd.to_datetime(Cap_data_raw_sub['CHANGE_DT1'], format='%Y%m%d')

Cap_data_raw_sub.to_pickle(program_path + 'temp\\Cap_data.pkl')
