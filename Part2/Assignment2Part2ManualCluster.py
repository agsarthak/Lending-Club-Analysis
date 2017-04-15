
# coding: utf-8

# In[1]:

import pandas as pd
import numpy as np

pd.set_option('display.max_rows', 200)
pd.set_option('display.max_columns', 200)


# In[2]:

loanData = pd.read_csv('D:\\ADS\\Assignments\\Assignment2\\cleanDataForCluster.csv',encoding = 'iso-8859-1',index_col=0)
loanData


# In[3]:

groupdedDAta=list(loanData.groupby(loanData['grade']))
groupdedDAta


# In[4]:

gradeA = groupdedDAta[0][1]
gradeA.to_csv("gradeA.csv")


# In[5]:

gradeB = groupdedDAta[1][1]
gradeB.to_csv("gradeB.csv")


# In[6]:

gradeC = groupdedDAta[2][1]
gradeC.to_csv("gradeC.csv")


# In[7]:

gradeD = groupdedDAta[3][1]
gradeD.to_csv("gradeD.csv")


# In[8]:

gradeE = groupdedDAta[4][1]
gradeE.to_csv("gradeE.csv")


# In[9]:

gradeF = groupdedDAta[5][1]
gradeF.to_csv("gradeF.csv")


# In[10]:

gradeG = groupdedDAta[6][1]
gradeF.to_csv("gradeG.csv")

