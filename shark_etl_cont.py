#!/usr/bin/env python
# coding: utf-8

# In[1]:


import csv
from datetime import datetime, timedelta
import pyodbc


# In[2]:


conn = pyodbc.connect('DSN=kubricksql;UID=de14;PWD=password')
cur = conn.cursor()


# In[3]:


sharkfile = r'c:\data\GSAF5.csv'


# In[4]:


attack_dates = []
case = []
country =[]
activity = []
age = []
gender = []
isfatal = []
with open (sharkfile,encoding = 'latin') as f:
    reader = csv.DictReader(f)# dialect 
    for row in reader:
        attack_dates.append(row['Date'])
        case.append(row['Case Number'])
        country.append(row['Country'])
        activity.append(row['Activity'])
        age.append(row['Age'])
        gender.append(row['Sex '])
        isfatal.append(row['Fatal (Y/N)'])
        
    
    


# In[5]:


data = zip(attack_dates, case, country, activity, age, gender, isfatal)


# In[6]:


cur.execute('truncate table brigitte.shark')


# In[7]:


q = 'insert into brigitte.shark(attack_date, case_number, country, activity, age, gender, isfatal) values (?,?,?,?,?,?,?)' #use ? so it doesnt distrupt single quotes, which you will need in SQL


# In[8]:


for d in data:
        try:
            cur.execute(q,d)
            conn.commit()
        except:
            conn.rollback()


# In[ ]:




