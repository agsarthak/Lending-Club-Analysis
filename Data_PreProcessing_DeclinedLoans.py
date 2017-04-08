
# coding: utf-8

# # Data Download and Pre-Processing - Declined Loans
# 
# ### This notebook is divided into following sections:
# ### 1. Importing libraries
# ### 2. Data Loading
# ### 3. Data Preparation
#     3.1 Missing Data Analysis - Removing columns
#     3.2 Missing Data Analysis - Removing rows
#     3.3 Missing Data Analysis - Filling NA values

# # 1. Import Libraries

# In[3]:

import pandas as pd
pd.set_option('display.max_rows', 200)
pd.set_option('display.max_columns', 200)
from matplotlib import style
style.use('fivethirtyeight')
import gc
import warnings
warnings.filterwarnings('ignore')


# In[4]:

gc.collect()

def declinedLoan_preprocess_magic():
    # # Data loading
    # In this section, we load the data from csv and concatenate all the csvs into one.
    
    # In[5]:
    
    ### Loading Dataframes
    rdata_2007_2012 = pd.read_csv('reject_loan_data_downloaded_zips_unzipped/RejectStatsA.csv', header=1, engine='python')
    rdata_2013_2014 = pd.read_csv('reject_loan_data_downloaded_zips_unzipped/RejectStatsB.csv', header=1, engine='python')
    rdata_2015 = pd.read_csv('reject_loan_data_downloaded_zips_unzipped/RejectStatsD.csv', header=1, engine='python')
    rdata_2016q1 = pd.read_csv('reject_loan_data_downloaded_zips_unzipped/RejectStats_2016Q1.csv', header=1, engine='python')
    rdata_2016q2 = pd.read_csv('reject_loan_data_downloaded_zips_unzipped/RejectStats_2016Q2.csv', header=1, engine='python')
    rdata_2016q3 = pd.read_csv('reject_loan_data_downloaded_zips_unzipped/RejectStats_2016Q3.csv', header=1, engine='python')
    rdata_2016q4 = pd.read_csv('reject_loan_data_downloaded_zips_unzipped/RejectStats_2016Q4.csv', header=1, engine='python')
    
    print('All data loaded in dataframe.')
    # In[6]:
    
    ### Concat all the dataframes
    rall_data = pd.concat([rdata_2007_2012, rdata_2013_2014, rdata_2015, rdata_2016q1, rdata_2016q2, rdata_2016q3, rdata_2016q4])
    
    print('All dataframes concatenated')
    
    lst = [rdata_2007_2012, rdata_2013_2014, rdata_2015, rdata_2016q1, rdata_2016q2, rdata_2016q3, rdata_2016q4]
    del lst
    
    # # 2. Data Preparation
    
    # **Check the Data.**
    
    # In[7]:
    
    #rall_data.info()
    
    
    # + We can see that there are around 11 million records.
    # + Total number of columns are 9. 
    # + Total data size is 845MB.
    
    # ## Missing Data Analysis
    # In the below section, we will check the percentage of missing data and then handle it accordingly.
    
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
    
    
    # ### Checking percentage of missing values for each column.
    
    # In[9]:
    
    #percentEmpty(rall_data)
    
    
    # We can observe above that except *Risk_Score* column very little data is absent.
    # 
    # Lets dive into each columns one by one.
    
    # #### Loan Title
    
    # In[10]:
    
    cnt_title_nan = rall_data['Loan Title'].isnull().sum()
    print('Number of missing values for Loan Title: ', cnt_title_nan)
    print('We can easily replace NaN with "No_Title"')
    
    
    # In[11]:
    
    rall_data['Loan Title'].fillna('No_Title', inplace=True)
    
    
    # #### Zip Code
    
    # In[12]:
    
    cnt_zip_nan = rall_data['Zip Code'].isnull().sum()
    print('Number of missing values for Zip Code: ', cnt_zip_nan)
    print('We can easily replace NaN with "No_Zip"')
    
    
    # In[13]:
    
    rall_data['Zip Code'].fillna('No_Zip', inplace=True)
    
    
    # #### State
    
    # In[14]:
    
    cnt_state_nan = rall_data['State'].isnull().sum()
    print('Number of missing values for State: ', cnt_state_nan)
    print('We can easily replace NaN with "No_State"')
    
    
    # In[15]:
    
    rall_data['State'].fillna('No_State', inplace=True)
    
    
    # In[16]:
    
    #percentEmpty(rall_data)
    
    
    # In[17]:
    
    #### change Debt-to-income ratio to float
    rall_data['Debt-To-Income Ratio'] = rall_data['Debt-To-Income Ratio'].str.slice(0,-1).astype('float')
    
    

    
    
    # In[25]:
    print('Writing file to disk started.')
    ## Exporting cleaned file 
    rall_data.to_csv('Cleaned_all_data_declined.csv')

    print('File successfully exported. Program ended.')