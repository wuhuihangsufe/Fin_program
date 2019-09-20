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

# define save list
save_list = ['S_INFO_WINDCODE', 'TRADE_DT', 'permno', 'S_DQ_PRECLOSE', 'S_DQ_OPEN',
             'S_DQ_HIGH', 'S_DQ_LOW', 'S_DQ_CLOSE', 'S_DQ_CHANGE', 'S_DQ_VOLUME',
             'S_DQ_AMOUNT', 'S_DQ_ADJFACTOR', 'S_DQ_TRADESTATUS', 'adj_pct_chg']

# Load data
Eod_data_raw = pd.read_csv(Data_path + 'ashareeodprices.csv')
Eod_data_raw.sort_values(['S_INFO_WINDCODE', 'TRADE_DT'], inplace=True)

Eod_data_raw['permno'] = Eod_data_raw['S_INFO_WINDCODE'].apply(lambda x: x[:6])
Eod_data_raw['permno'].replace('T00018', '600018', inplace=True)
Eod_data_raw['permno'].replace('600087', '601975', inplace=True)

def get_A_share(df_temp):
    df_temp = df_temp[(df_temp['permno'].str.startswith('0')) | (df_temp['permno'].str.startswith('3'))
                      | (df_temp['permno'].str.startswith('6'))]
    df_temp['Symbol'] = df_temp['permno'].apply(lambda x: x[:3])
    df_temp = df_temp[df_temp['Symbol'] != '688']
    del df_temp['Symbol']
    return df_temp


Eod_data_raw = get_A_share(df_temp=Eod_data_raw)
Eod_data_raw['adj_pct_chg'] = (Eod_data_raw['S_DQ_ADJCLOSE'] - Eod_data_raw['S_DQ_ADJPRECLOSE'])/Eod_data_raw['S_DQ_ADJPRECLOSE']
Eod_data_raw['TRADE_DT'] = pd.to_datetime(Eod_data_raw['TRADE_DT'], format='%Y%m%d')
print('Missing return data number:', Eod_data_raw['adj_pct_chg'].isnull().sum())
print('Missing VOLUME data number:', Eod_data_raw['S_DQ_VOLUME'].isnull().sum())
print('Missing AMOUNT data number:', Eod_data_raw['S_DQ_AMOUNT'].isnull().sum())

Eod_data_raw = Eod_data_raw.loc[:, save_list]

Eod_data_raw.to_pickle(program_path + 'temp\\Daily_return..pkl')
