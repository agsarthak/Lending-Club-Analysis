
# coding: utf-8

# #  Data Download and Pre-Processing - Accepted Loans
# ### This notebook is divided into following sections:
# ### 1. Importing libraries
# ### 2. Data Loading
# ### 3. Data Preparation
#     3.1 Missing Data Analysis - Removing columns
#     3.2 Missing Data Analysis - Removing rows
#     3.3 Missing Data Analysis - Filling NA values

# # 1. Import Libraries

# In[4]:

import pandas as pd
#pd.set_option('display.max_rows', 200)
#pd.set_option('display.max_columns', 200)
from matplotlib import style
from functools import partial
from operator import is_not
style.use('fivethirtyeight')
import warnings
warnings.filterwarnings('ignore')
import math
import numpy as np


# # Data loading
# In this section, we load the data from csv and concatenate all the csvs into one.

# In[5]:
    
def loan_preprocess_magic():
    
    ### Loading Dataframes
    data_2007_2011 = pd.read_csv('loan_data_downloaded_zips_unzipped/LoanStats3a.csv', header=1, skipfooter=4, engine='python')
    print('data_2007_2011 loaded')
    data_2012_2013 = pd.read_csv('loan_data_downloaded_zips_unzipped/LoanStats3b.csv', header=1, skipfooter=4, engine='python')
    data_2014 = pd.read_csv('loan_data_downloaded_zips_unzipped/LoanStats3c.csv', header=1, skipfooter=4, engine='python')
    data_2015 = pd.read_csv('loan_data_downloaded_zips_unzipped/LoanStats3d.csv', header=1, skipfooter=4, engine='python')
    data_2016q1 = pd.read_csv('loan_data_downloaded_zips_unzipped/LoanStats_2016Q1.csv', header=1, skipfooter=4, engine='python')
    data_2016q2 = pd.read_csv('loan_data_downloaded_zips_unzipped/LoanStats_2016Q2.csv', header=1, skipfooter=4, engine='python')
    data_2016q3 = pd.read_csv('loan_data_downloaded_zips_unzipped/LoanStats_2016Q3.csv', header=1, skipfooter=4, engine='python')
    data_2016q4 = pd.read_csv('loan_data_downloaded_zips_unzipped/LoanStats_2016Q4.csv', header=1, skipfooter=4, engine='python')
    
    print('All data files loaded in dataframe.')
    # In[6]:
    
    ### Concat all the dataframes
    all_data = pd.concat([data_2007_2011, data_2012_2013, data_2014, data_2015, data_2016q1, data_2016q2, data_2016q3, data_2016q4])
    
    print('Data successfully concatenated.')
    
    lst = [data_2007_2011, data_2012_2013, data_2014, data_2015, data_2016q1, data_2016q2, data_2016q3, data_2016q4]
    del lst
    
    # # 2. Data Preparation
    
    # **Change the datatype of few of the features.**
    
    # In[7]:
    
    # changing interest rate from str to float
    all_data['int_rate'] = all_data['int_rate'].str.slice(0,-1).astype('float')
    
    
    # ## Missing Data Analysis
    # In the below section, we will describe the various approach we have taken to handle missing values after careful analysis and observing the distriution of data.
    # In a nutshell, we have tried these three approaches:
    # + Removed columns
    # + Removed rows
    # + Filled nan values 
    
    # ### Functions to detect the precentage of missing values in each column.
    # After analyzing the percentage of missing data in each columns, we decided to remove columns which have more than 65% of missing values. Because filling that much amount of data is resuting in skewness of the distribution.
    
    # In[8]:
    
    def f(row):
        if row['percent'] > 65:
            return row['colName']
        
    def percentEmpty(df):
        dfEmpty=df.isnull().sum()
        totalRows=len(df)
        print("totalRows",totalRows)
        peDf = pd.DataFrame((dfEmpty/totalRows)*100, columns=['percent'])
        peDf['colName'] = peDf.index
        return peDf
    
    
    # ### Drop columns that have more than 65% missing values.
    
    # In[9]:
    
    findDropColumnDf = percentEmpty(all_data)
    arr = []
    arr = findDropColumnDf.apply(f, axis = 1)
    dropCols = list(filter(partial(is_not, None), arr.values))
    dropCols = dropCols+['url','title','next_pymnt_d']
    print(dropCols)
    
    ## Drop decided columns
    all_data = all_data.drop(dropCols, 1)
    
    print('Required columns dropped.')
    # ### Remove rows
    # Check percentage of missing values for the columns - *'member_id', 'revol_util', 'last_pymnt_d','last_credit_pull_d'*.
    
    # In[11]:
    
    #xx = all_data[['member_id', 'revol_util', 'last_pymnt_d','last_credit_pull_d']]
    #percentEmpty(xx)
    
    
    # ### Drop rows that have nan in the above mentioned 4 columns. This is just 0.15% of the complete data.
    
    # In[12]:
    
    all_data.dropna(subset=['revol_util', 'last_pymnt_d','last_credit_pull_d'], how='any', inplace=True)
    
    
    # ### Checking percentage of missing values for each column.
    
    # In[13]:
    
    #percentEmpty(all_data)
    
    
    # **As we can see that most of the variables have less than 5% of missing data, so we can easily fill those with the mean. 
    # For rest of the variables where there are high percentage of missing values, lets leave them right now as it is.**
    
    # ### Filling Missing Values with mean.
    # 
    # To fill missing values with mean, we checked the change in frequency distribution after filling the missing values with mean. This process was done for all the columns.
    
    # #### Plot Frequency Distribution before filling NaN, then plot it after filling NaN. The graph should not change. To keep this notebook concise, we are not showing all the graphs.
    
    # In[14]:
    
    #all_data['mo_sin_old_il_acct'].hist(bins=50)
    
    
    # In[15]:
    
   
    
    #replacing NAN with 0 as its the count of accounts opened
    all_data['acc_open_past_24mths'] = all_data['acc_open_past_24mths'].replace(np.nan, 0, regex=True)
    
    #replacing NAN with 0
    all_data['tot_cur_bal'] = all_data['tot_cur_bal'].replace(np.nan, 0, regex=True)
    
    #replacing NAN with 0 as its tot_cur_bal is zero
    all_data['avg_cur_bal'] = all_data['avg_cur_bal'].replace(np.nan, 0, regex=True)
    
    #replcaing NAN with 0 as its the count of charge offs of loan status
    all_data['chargeoff_within_12_mths'] = all_data['chargeoff_within_12_mths'].replace(np.nan, 0, regex=True)
    
    #replcaing NAN with 0 as its the count of charge offs of loan status for whom medical is excluded
    all_data['collections_12_mths_ex_med'] = all_data['collections_12_mths_ex_med'].replace(np.nan, 0, regex=True)
    
    #replacing empty employee title with NA string
    all_data['emp_title'] =all_data['emp_title'].replace(np.nan, 'NA', regex=True)
    
    #replacing NAN with 0
    all_data['bc_open_to_buy'] = all_data['bc_open_to_buy'].replace(np.nan, 0, regex=True)
    
    #replacing NAN with mean
    all_data['tot_hi_cred_lim'] = all_data['tot_hi_cred_lim'].replace(np.nan, all_data['tot_hi_cred_lim'].mean(), regex=True)
    
    #replacing NAN with 0 as its the ratio
    all_data['bc_util'] = all_data['bc_util'].replace(np.nan, all_data['bc_util'].mean(), regex=True)
    
    #replacing NAN with 0 as its the count of months
    all_data['mo_sin_old_il_acct'] = all_data['mo_sin_old_il_acct'].replace(np.nan, 0, regex=True)
    all_data['mo_sin_old_rev_tl_op'] = all_data['mo_sin_old_rev_tl_op'].replace(np.nan, 0, regex=True)
    all_data['mo_sin_rcnt_rev_tl_op'] = all_data['mo_sin_rcnt_rev_tl_op'].replace(np.nan, 0, regex=True)
    all_data['mo_sin_rcnt_tl'] = all_data['mo_sin_rcnt_tl'].replace(np.nan, 0, regex=True)
    all_data['mths_since_recent_bc'] = all_data['mths_since_recent_bc'].replace(np.nan, 0, regex=True)
    all_data['mths_since_recent_inq'] = all_data['mths_since_recent_inq'].replace(np.nan, 0, regex=True)
    
    #replacing NAN with 0 as its the count
    all_data['mort_acc'] = all_data['mort_acc'].replace(np.nan, 0, regex=True)
    all_data['num_accts_ever_120_pd'] = all_data['num_accts_ever_120_pd'].replace(np.nan, 0, regex=True)
    all_data['num_actv_bc_tl'] = all_data['num_actv_bc_tl'].replace(np.nan, 0, regex=True)
    all_data['num_actv_rev_tl'] = all_data['num_actv_rev_tl'].replace(np.nan, 0, regex=True)
    all_data['num_bc_sats'] = all_data['num_bc_sats'].replace(np.nan, 0, regex=True)
    all_data['num_bc_tl'] = all_data['num_bc_tl'].replace(np.nan, 0, regex=True)
    all_data['num_il_tl'] = all_data['num_il_tl'].replace(np.nan, 0, regex=True)
    all_data['num_op_rev_tl'] = all_data['num_op_rev_tl'].replace(np.nan, 0, regex=True)
    all_data['num_rev_accts'] = all_data['num_rev_accts'].replace(np.nan, 0, regex=True)
    all_data['num_rev_tl_bal_gt_0'] = all_data['num_rev_tl_bal_gt_0'].replace(np.nan, 0, regex=True)
    all_data['num_sats'] = all_data['num_sats'].replace(np.nan, 0, regex=True)
    all_data['num_tl_30dpd'] = all_data['num_tl_30dpd'].replace(np.nan, 0, regex=True)
    all_data['num_tl_90g_dpd_24m'] = all_data['num_tl_90g_dpd_24m'].replace(np.nan, 0, regex=True)
    all_data['num_tl_op_past_12m'] = all_data['num_tl_op_past_12m'].replace(np.nan, 0, regex=True)
    all_data['pub_rec_bankruptcies'] = all_data['pub_rec_bankruptcies'].replace(np.nan, 0, regex=True)
    all_data['tot_coll_amt'] = all_data['tot_coll_amt'].replace(np.nan, 0, regex=True)
    all_data['tax_liens'] = all_data['tax_liens'].replace(np.nan, 0, regex=True)
    
    #replacing NAN with mean as its the ration/percent data
    all_data['pct_tl_nvr_dlq'] = all_data['pct_tl_nvr_dlq'].replace(np.nan,all_data['pct_tl_nvr_dlq'].mean(), regex=True)
    all_data['percent_bc_gt_75'] = all_data['percent_bc_gt_75'].replace(np.nan,all_data['percent_bc_gt_75'].mean(), regex=True)
    all_data['total_bal_ex_mort'] = all_data['total_bal_ex_mort'].replace(np.nan,all_data['total_bal_ex_mort'].mean(), regex=True)
    all_data['total_bc_limit'] = all_data['total_bc_limit'].replace(np.nan,all_data['total_bc_limit'].mean(), regex=True)
    all_data['total_il_high_credit_limit'] = all_data['total_il_high_credit_limit'].replace(np.nan,all_data['total_il_high_credit_limit'].mean(), regex=True)
    all_data['total_rev_hi_lim'] = all_data['total_rev_hi_lim'].replace(np.nan,all_data['total_rev_hi_lim'].mean(), regex=True)
    
    
    # In[93]:
    
    
    def num_tl_120(row):
        if math.isnan(row['num_tl_120dpd_2m']):
            return row['num_tl_30dpd']
        else:
            return row['num_tl_120dpd_2m']
    
    
    # In[94]:
    
    all_data['num_tl_120dpd_2m']=all_data.apply(num_tl_120,axis=1)
    
    
    # In[ ]:
    
    #converting into category data types
    all_data['addr_state'] =all_data['addr_state'].astype('category')
    all_data['application_type'] =all_data['application_type'].astype('category')
    all_data['emp_length'] =all_data['emp_length'].astype('category')

    
    
    # In[17]:
    
    #percentEmpty(all_data)
    
    
    # In[18]:
    print('Writing cleaned file to disk.')
    ## Exporting cleaned file 
    all_data.to_csv('Cleaned_all_data.csv')

    print('File successfully exported. Program complete.')
