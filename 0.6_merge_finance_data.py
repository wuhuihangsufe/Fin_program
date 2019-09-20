import pandas as pd
import numpy as np
from pandas.tseries.offsets import *

program_path = 'D:\\China\\China_Data\\Fin_program\\'
Data_path = 'F:\\WindDataBase\\'
Start_Date = '1997-01-01'
End_Date = '2019-06-30'

Monthly_return_all = pd.read_pickle(program_path + 'temp\\Monthly_return_with_cap.pkl')
# 1 BS data clean
# 1.1 laod data
balance = pd.read_csv(Data_path + 'asharebalancesheet.csv')
info_list1 = ['S_INFO_WINDCODE', 'ANN_DT', 'ACTUAL_ANN_DT', 'STATEMENT_TYPE', 'REPORT_PERIOD']
# 1.2 Select small dataset
bs_keep_list = ['S_INFO_WINDCODE', 'ANN_DT', 'ACTUAL_ANN_DT', 'STATEMENT_TYPE', 'REPORT_PERIOD',
                'TOT_SHRHLDR_EQY_INCL_MIN_INT', 'TOT_ASSETS', 'TOT_LIAB',
                'DVD_PAYABLE', 'INVENTORIES', 'TOT_CUR_LIAB', 'FIX_ASSETS',
                'ACCT_RCV', 'FIX_ASSETS', 'MONETARY_CAP', 'RCV_INVEST',
                'ST_BORROW', 'LT_BORROW', 'TOT_SHRHLDR_EQY_EXCL_MIN_INT',
                'MINORITY_INT', 'LT_BORROW', 'ST_BORROW', 'TOT_CUR_ASSETS',
                'TOT_CUR_LIAB', 'CASH_DEPOSITS_CENTRAL_BANK', 'FIX_ASSETS_DISP',
                'OTHER_EQUITY_TOOLS_P_SHR', 'R_AND_D_COSTS']

balance = balance.loc[:, bs_keep_list]
# 1.3 keep
balance['permno'] = balance['S_INFO_WINDCODE'].str.split('.', expand=True)[0]

# 转换日期格式
balance['REPORT_PERIOD'] = pd.to_datetime(balance['REPORT_PERIOD'], format='%Y%m%d')
balance['month'] = np.min(balance.loc[:, ['ANN_DT', 'ACTUAL_ANN_DT']], axis=1)
balance['month'] = balance['month'].apply(lambda x: str(x)[:4] + '-' + str(x)[4:6])
# 在存在更正前和更正后的数据情况下，保留未更正的数据
balance = balance.loc[(balance['STATEMENT_TYPE'] == 408005000) | (balance['STATEMENT_TYPE'] == 408001000)].sort_values(
    ['permno', 'month', 'STATEMENT_TYPE'])
balance.drop_duplicates(subset=['permno', 'month'], keep='last', inplace=True)


def get_A_share(df_temp):
    df_temp = df_temp[(df_temp['permno'].str.startswith('0')) | (df_temp['permno'].str.startswith('3'))
                      | (df_temp['permno'].str.startswith('6'))]
    df_temp['Symbol'] = df_temp['permno'].apply(lambda x: x[:3])
    df_temp = df_temp[df_temp['Symbol'] != '688']
    del df_temp['Symbol']
    return df_temp


balance = get_A_share(df_temp=balance)
Monthly_return_finance = Monthly_return_all.merge(balance, how='outer', on=['permno', 'month'],
                                                  indicator=True, validate='1:1')
print(Monthly_return_finance['_merge'].value_counts())
Monthly_return_finance.sort_values(['permno', 'month'], inplace=True)
Monthly_return_finance.reset_index(inplace=True, drop=True)
# permno = Monthly_return_finance[Monthly_return_finance['_merge'] == 'right_only']['permno'].unique()

fillna_data = pd.DataFrame()
firm_permno_list = Monthly_return_finance['permno'].unique()
for i_firm in firm_permno_list:
    print(i_firm)
    temp_index = Monthly_return_finance[Monthly_return_finance['permno'] == i_firm].index
    temp_data = Monthly_return_finance.loc[temp_index, bs_keep_list]
    temp_data.fillna(method='ffill', inplace=True, axis=0)
    fillna_data = fillna_data.append(temp_data)

Monthly_return_finance.loc[:, bs_keep_list] = fillna_data.values
Monthly_return_finance = Monthly_return_finance[Monthly_return_finance['_merge'] != 'right_only']

del fillna_data
del Monthly_return_finance['_merge']
del Monthly_return_finance['ANN_DT']
del Monthly_return_finance['ACTUAL_ANN_DT']

# 2  data clean income data
# 2.1 laod data
income = pd.read_csv(Data_path + 'ashareincome.csv')
# 1.2 Select small dataset

income_keep_list1 = ['NET_PROFIT_INCL_MIN_INT_INC', 'NET_PROFIT_EXCL_MIN_INT_INC', 'OPER_REV', 'LESS_OPER_COST',
                     'OPER_PROFIT', 'INC_TAX', 'TOT_PROFIT', 'LESS_GERL_ADMIN_EXP',
                     'LESS_SELLING_DIST_EXP', 'LESS_FIN_EXP', 'TOT_OPER_REV',
                     'OTHER_BUS_INC', 'TOT_OPER_COST', 'RD_EXPENSE',
                     'LESS_TAXES_SURCHARGES_OPS', 'PLUS_NON_OPER_REV',
                     'LESS_NON_OPER_EXP', 'EBIT', 'EBITDA']

income_keep_list = info_list1 + income_keep_list1
income = income.loc[:, income_keep_list]
# 1.3 keep
income['permno'] = income['S_INFO_WINDCODE'].str.split('.', expand=True)[0]

# 转换日期格式
income['REPORT_PERIOD'] = pd.to_datetime(income['REPORT_PERIOD'], format='%Y%m%d')
income['month'] = np.min(income.loc[:, ['ANN_DT', 'ACTUAL_ANN_DT']], axis=1)
income['month'] = income['month'].apply(lambda x: str(x)[:4] + '-' + str(x)[4:6])
# 在存在更正前和更正后的数据情况下，保留未更正的数据
income = income.loc[(income['STATEMENT_TYPE'] == 408005000) | (income['STATEMENT_TYPE'] == 408001000)].sort_values(
    ['permno', 'month', 'STATEMENT_TYPE'])
income.drop_duplicates(subset=['permno', 'month'], keep='last', inplace=True)

income = get_A_share(df_temp=income)

temp_column = income_keep_list1 + ['REPORT_PERIOD', 'permno']
lastqincome = income.loc[:, temp_column].copy()
lastqincome.loc[lastqincome['REPORT_PERIOD'] >= pd.datetime(2001, 12, 31), 'reportdate'] = lastqincome[
                                                                                               'REPORT_PERIOD'] + MonthEnd(
    3)
lastqincome.loc[lastqincome['REPORT_PERIOD'] < pd.datetime(2001, 12, 31), 'reportdate'] = lastqincome[
                                                                                              'REPORT_PERIOD'] + MonthEnd(
    6)

for k in income_keep_list1:
    lastqincome.rename(columns={k: k + 'last'}, inplace=True)

lastqincome.rename(columns={'REPORT_PERIOD': 'lastrepodate'}, inplace=True)
lastqincome.rename(columns={'reportdate': 'REPORT_PERIOD'}, inplace=True)
diffqincome = pd.merge(income, lastqincome, on=['permno', 'REPORT_PERIOD'], how='left')

for k in income_keep_list1:
    diffqincome[k + '_diff'] = diffqincome[k] - diffqincome[k + 'last']
    diffqincome.drop([k + 'last'], axis=1, inplace=True)

diffqincome.drop_duplicates(subset=['permno', 'month'], keep='last', inplace=True)
Monthly_return_finance = Monthly_return_all.merge(diffqincome, how='outer', on=['permno', 'month'],
                                                  indicator=True, validate='1:1')
print(Monthly_return_finance['_merge'].value_counts())
Monthly_return_finance.sort_values(['permno', 'month'], inplace=True)
Monthly_return_finance.reset_index(inplace=True, drop=True)
# permno = Monthly_return_finance[Monthly_return_finance['_merge'] == 'right_only']['permno'].unique()

fillna_data = pd.DataFrame()
firm_permno_list = Monthly_return_finance['permno'].unique()
income_keep_list2 = []
for k in income_keep_list1:
    income_keep_list2.append(k + '_diff')
income_keep_list2 = income_keep_list1 + income_keep_list2

for i_firm in firm_permno_list:
    print(i_firm)
    temp_index = Monthly_return_finance[Monthly_return_finance['permno'] == i_firm].index
    temp_data = Monthly_return_finance.loc[temp_index, income_keep_list2]
    temp_data.fillna(method='ffill', inplace=True, axis=0)
    fillna_data = fillna_data.append(temp_data)

Monthly_return_finance.loc[:, income_keep_list2] = fillna_data.values
Monthly_return_finance = Monthly_return_finance[Monthly_return_finance['_merge'] != 'right_only']

del fillna_data
del Monthly_return_finance['_merge']
del Monthly_return_finance['ANN_DT']
del Monthly_return_finance['ACTUAL_ANN_DT']

# 3  data clean cash flow data
# 3.1 laod data
cashflow = pd.read_csv(Data_path + 'asharecashflow.csv')
# 3.2 Select small dataset

cashflow_keep_list1 = ['NET_CASH_FLOWS_OPER_ACT', 'NET_CASH_FLOWS_FNC_ACT',
                       'NET_CASH_FLOWS_INV_ACT', 'FREE_CASH_FLOW',
                       'NET_INCR_CASH_CASH_EQU', 'PAY_ALL_TYP_TAX', 'CASH_PAID_INVEST',
                       'NET_CASH_PAY_AQUIS_SOBU', 'OTHER_CASH_PAY_RAL_INV_ACT',
                       'CASH_PAY_ACQ_CONST_FIOLTA', 'DEPR_FA_COGA_DPBA']

cashflow_keep_list = info_list1 + cashflow_keep_list1
cashflow = cashflow.loc[:, cashflow_keep_list]
# 1.3 keep
cashflow['permno'] = cashflow['S_INFO_WINDCODE'].str.split('.', expand=True)[0]

# 转换日期格式
cashflow['REPORT_PERIOD'] = pd.to_datetime(cashflow['REPORT_PERIOD'], format='%Y%m%d')
cashflow['month'] = np.min(cashflow.loc[:, ['ANN_DT', 'ACTUAL_ANN_DT']], axis=1)
cashflow['month'] = cashflow['month'].apply(lambda x: str(x)[:4] + '-' + str(x)[4:6])
# 在存在更正前和更正后的数据情况下，保留未更正的数据
cashflow = cashflow.loc[
    (cashflow['STATEMENT_TYPE'] == 408005000) | (cashflow['STATEMENT_TYPE'] == 408001000)].sort_values(
    ['permno', 'month', 'STATEMENT_TYPE'])
cashflow.drop_duplicates(subset=['permno', 'month'], keep='last', inplace=True)

cashflow = get_A_share(df_temp=cashflow)

temp_column = cashflow_keep_list1 + ['REPORT_PERIOD', 'permno']
lastqcashflow = cashflow.loc[:, temp_column].copy()
lastqcashflow.loc[lastqcashflow['REPORT_PERIOD'] >= pd.datetime(2001, 12, 31), 'reportdate'] = lastqcashflow[
                                                                                                   'REPORT_PERIOD'] + MonthEnd(
    3)
lastqcashflow.loc[lastqcashflow['REPORT_PERIOD'] < pd.datetime(2001, 12, 31), 'reportdate'] = lastqcashflow[
                                                                                                  'REPORT_PERIOD'] + MonthEnd(
    6)

for k in cashflow_keep_list1:
    lastqcashflow.rename(columns={k: k + 'last'}, inplace=True)

lastqcashflow.rename(columns={'REPORT_PERIOD': 'lastrepodate'}, inplace=True)
lastqcashflow.rename(columns={'reportdate': 'REPORT_PERIOD'}, inplace=True)
diffqcashflow = pd.merge(cashflow, lastqcashflow, on=['permno', 'REPORT_PERIOD'], how='left')

for k in cashflow_keep_list1:
    diffqcashflow[k + '_diff'] = diffqcashflow[k] - diffqcashflow[k + 'last']
    diffqcashflow.drop([k + 'last'], axis=1, inplace=True)

diffqcashflow.drop_duplicates(subset=['permno', 'month'], keep='last', inplace=True)
Monthly_return_finance = Monthly_return_finance.merge(diffqcashflow, how='outer', on=['permno', 'month'],
                                                      indicator=True, validate='1:1')
print(Monthly_return_finance['_merge'].value_counts())
Monthly_return_finance.sort_values(['permno', 'month'], inplace=True)
Monthly_return_finance.reset_index(inplace=True, drop=True)
# permno = Monthly_return_finance[Monthly_return_finance['_merge'] == 'right_only']['permno'].unique()

fillna_data = pd.DataFrame()
firm_permno_list = Monthly_return_finance['permno'].unique()
cashflow_keep_list2 = []
for k in cashflow_keep_list1:
    cashflow_keep_list2.append(k + '_diff')
cashflow_keep_list2 = cashflow_keep_list1 + cashflow_keep_list2

for i_firm in firm_permno_list:
    print(i_firm)
    temp_index = Monthly_return_finance[Monthly_return_finance['permno'] == i_firm].index
    temp_data = Monthly_return_finance.loc[temp_index, cashflow_keep_list2]
    temp_data.fillna(method='ffill', inplace=True, axis=0)
    fillna_data = fillna_data.append(temp_data)

Monthly_return_finance.loc[:, cashflow_keep_list2] = fillna_data.values
Monthly_return_finance = Monthly_return_finance[Monthly_return_finance['_merge'] != 'right_only']

del fillna_data
del Monthly_return_finance['_merge']
del Monthly_return_finance['ANN_DT']
del Monthly_return_finance['ACTUAL_ANN_DT']

Monthly_return_finance.to_pickle(program_path + 'temp\\Monthly_return_with_finance.pkl')
