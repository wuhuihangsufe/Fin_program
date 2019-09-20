'''
File information
Introduction:
This file is used to transform form daily returns to monthly returns

Monthly returns file needs to following variables:
permno, cummulative monthly return, last day float share, last day close price;
Total Trading days, Last day tradable day, month, ipo_date

input:
Daily_return dataset file: Daily_return.pickle
Daily_return dataset file: Cap_data.pickle

output:
Daily_return_with_cap.pickle
'''

import pandas as pd
import numpy as np

program_path = 'D:\\China\\China_Data\\Fin_program\\'
Data_path = 'F:\\WindDataBase\\'
Start_Date = '1997-01-01'
End_Date = '2019-06-30'
# 0 laod data
Daily_return_with_cap = pd.read_pickle(program_path + 'temp\\Daily_return_with_cap.pickle')

# 1 Get permno month
# 1.1 Check for zero trading volume data
Daily_return_with_cap['month'] = Daily_return_with_cap['TRADE_DT'].apply(lambda x: x.strftime('%Y-%m'))
print('Trading days with zero trading volume', len(Daily_return_with_cap[Daily_return_with_cap['S_DQ_VOLUME'] == 0]))
print('Trading days with zero trading volume but still trading',
      len(Daily_return_with_cap[
              (Daily_return_with_cap['S_DQ_VOLUME'] == 0) & (Daily_return_with_cap['S_DQ_TRADESTATUS'] == '交易')]))
print('Trading days with zero trading volume and daily return is 0 but still trading',
      len(Daily_return_with_cap[
              (Daily_return_with_cap['S_DQ_VOLUME'] == 0) & (Daily_return_with_cap['S_DQ_TRADESTATUS'] == '交易')
              & (Daily_return_with_cap['adj_pct_chg'] == 0)]))
# temp_check = Daily_return_with_cap[(Daily_return_with_cap['S_DQ_VOLUME'] == 0) &
# (Daily_return_with_cap['S_DQ_TRADESTATUS'] == '交易')]

# 1.2 replace zero trading volume with np.NAN
# TODO this should miss replace 43 observations, let's fix this bug later

Daily_return_with_cap['S_DQ_VOLUME'].replace(0, np.nan, inplace=True)
Daily_return_with_cap_trading = Daily_return_with_cap.dropna().copy()

# 1.3 Get monthly trading days number
Monthly_return = Daily_return_with_cap_trading.groupby(['permno','month'])['S_DQ_VOLUME'].count().reset_index()
Monthly_return.rename(columns={'S_DQ_VOLUME': 'trading_days_number'}, inplace=True)

# 1.4 Get last trading day, last day float share, last day close price;
temp = Daily_return_with_cap_trading.loc[:, ['permno', 'month', 'TRADE_DT', 'TOT_SHR', 'FLOAT_SHR',
                                             'FLOAT_A_SHR', 'NON_TRADABLE_SHR', 'S_SHARE_TOTALA', 'S_DQ_CLOSE']]
temp.drop_duplicates(subset=['permno', 'month'], keep='last', inplace=True)
Monthly_return = Monthly_return.merge(temp, how='outer', on=['permno', 'month'],
                                indicator=True, validate='1:1')

# make sure only have both
print(Monthly_return['_merge'].value_counts())
del Monthly_return['_merge']

# 1.5 Get cummulative monthly return

Daily_return_with_cap_trading['adj_pct_chg'] = np.log(1+Daily_return_with_cap_trading['adj_pct_chg'])

temp = Daily_return_with_cap_trading.groupby(['permno','month'])['adj_pct_chg'].sum().reset_index()
temp['adj_pct_chg'] = np.exp(temp['adj_pct_chg']) - 1
Monthly_return = Monthly_return.merge(temp, how='outer', on=['permno', 'month'],
                                indicator=True, validate='1:1')

print(Monthly_return['_merge'].value_counts())
del Monthly_return['_merge']

# Merge the IPO dates
IPO_sub = pd.read_pickle(program_path + 'temp\\IPO_list.pkl')
Monthly_return_all = pd.merge(IPO_sub, Monthly_return, how='outer', on='permno',
                              indicator=True, validate='1:m')

print(Monthly_return_all['_merge'].value_counts())
del Monthly_return_all['_merge']

Monthly_return_all.to_pickle(program_path + 'temp\\Monthly_return_with_cap.pkl')
