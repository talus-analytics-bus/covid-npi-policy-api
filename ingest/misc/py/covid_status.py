import pandas as pd
import numpy as np
import os
from pprint import pprint
from airtable import Airtable
import pandas as pd
import numpy as np
import seaborn as sns
from datetime import datetime
from datetime import date
import plotly.graph_objects as go
import plotly.express as px
import matplotlib.pyplot as plt
import nltk
import string

def create_date_table(start='2020-03-01', end='2020-07-01'):
    df = pd.DataFrame({"Date": pd.date_range(start, end)})
    df["Week"] = df.Date.dt.weekofyear
    df["Year"] = df.Date.dt.year

    return df

def remove_punctuation(text):
    no_punct = " ".join([c for c in text if c not in string.punctuation])
    no_punct = no_punct.lower()
    return no_punct

#functions:
def check_expired (date1, date2):
    if (date1 - date2).days <= 0:
        expired = 1
    else:
        expired = 0
    return expired

base_key = 'appOtKBVJRyuH83wf'
table_name = 'Policy Database'
AIRTABLE_KEY = 'keypoSTyPHy3P2YFo'
airtable = Airtable(base_key, table_name, api_key=AIRTABLE_KEY)


#Columns needed from airtable:
cols = ['Authorizing level of government', 'Authorizing state/province, if applicable',
        'Affected level of government', 'Policy subcategory', 'Policy category','Policy subtarget',
                           'Unique ID', 'Policy relaxing or restricting',
        'Effective start date', 'Actual end date', 'Anticipated end date']

records = airtable.get_all()
df = pd.DataFrame.from_records((r['fields'] for r in records))
df = df[cols]


#Clean up the data and get variables of interest:
stay_at_home_cat = ['Private sector closures','School closures','Quarantine','Isolation','Mass gathering restrictions',
'Event delays or cancellations', 'Stay at home', 'Safer at home']

df= df[(df['Policy category'] == "Social distancing") &
        (df['Authorizing level of government'] == "State/Province (Intermediate area)") &
        (df['Affected level of government'].isnull()) &
       (df['Policy subcategory'].isin(stay_at_home_cat))]
#Getting proper end dates and creating feature flags:

df['date_start']= df['Effective start date'].apply(lambda t: -1 if pd.isnull(t) else datetime.strptime(t, '%Y-%m-%d').date())
df['anticipated_end_date'] = df['Anticipated end date'].apply(lambda t: -1
                                                              if pd.isnull(t) else datetime.strptime(t, '%Y-%m-%d').date())
df['date_end']= df['Actual end date'].apply(lambda t: -1 if pd.isnull(t) else datetime.strptime(t, '%Y-%m-%d').date())
df['end_date'] = np.where( (df['Actual end date'].isnull()) & (df['Anticipated end date'].notnull()),
                                                               df['anticipated_end_date'], df['date_end'])

df['has_stay_at_home'] = np.where((df['Policy subcategory'] == 'Stay at home')
                                  & (df['Policy relaxing or restricting']=='Restricting'), 1, 0)

df['has_safer_at_home'] = np.where(df['Policy subcategory'] == 'Safer at home', 1, 0)

df['Policy subtarget'].loc[df['Policy subtarget'].isnull()] = df['Policy subtarget'].loc[df['Policy subtarget'].isnull()].apply(lambda x: [])
df['subtarget_clean'] = df['Policy subtarget'].apply(lambda x: remove_punctuation(x))
df['general_population_flag'] = np.where(df['subtarget_clean'].str.contains('general population', regex=False),1,0)
df['non_essential_flag'] = np.where(df['subtarget_clean'].str.contains('non essential business', regex=False),1,0)
df['essential_flag'] = np.where(df['subtarget_clean'].str.contains('essential business', regex=False),1,0)
df['vulnerable_flag'] = np.where(df['subtarget_clean'].str.contains('older adults/individuals with underlying medical conditions', regex=False),1,0)

#adding today as end date if one doesn't exist
date_today = date.today()
df['end_date'] = np.where(df['end_date'] == -1, date_today, df['end_date'])

df.rename(columns={'Authorizing level of government':'level_of_gov',
    'Authorizing state/province, if applicable': "state",
                           'Policy subcategory': 'subcategory',
                   'Policy category': 'category',
                           'Unique ID': 'ID',
                  'Policy relaxing or restricting': 'intent'}, inplace = True)

#create a dataframe for each valid date:
date_df = pd.concat([pd.DataFrame({'date_var': pd.date_range(row.date_start, row.end_date, freq='d'),
               'state': row.state,
               'subcategory': row.subcategory,
               'intent': row.intent,
                'ID': row.ID }, columns=['date_var', 'state', 'subcategory', 'intent','ID'])
           for i, row in df.iterrows()], ignore_index=True)

date_table = create_date_table()

full_df = date_df.merge(df, on = ['state', 'subcategory', 'intent','ID'], how = 'left')



#for each row, indicate the closure status:

full_df['private_sector_closed'] = np.where((full_df['subcategory'] == 'Private sector closures') &
                                            (full_df['intent'] == 'Restricting') &
                                            ((full_df['vulnerable_flag'] != 1)), 1, 0)
#school closure:
full_df['school_closure'] = np.where((full_df['subcategory'] == 'School closures') &
                                            (full_df['intent'] == 'Restricting'), 1, 0)
#is quarantine or isolation for anyone:
#full_df['quarantine_or_isolation'] = np.where(((full_df['subcategory'] == 'Quarantine') |
#                                      (full_df['subcategory'] == 'Isolation')) &
#                                            (full_df['intent'] == 'Restricting'), 1, 0)

#event and mass gathering restrictions for non-vulnerable people:
full_df['event_gathering_restrict'] = np.where((full_df['intent'] == 'Restricting') &
                                      ((full_df['subcategory'] == 'Event delays or cancellations') |
                                      (full_df['subcategory'] == 'Mass gathering restrictions')) &
                                       ((full_df['vulnerable_flag'] != 1) ) , 1, 0)


full_df['private_sector_reopen'] = np.where((full_df['subcategory'] == 'Private sector closures') &
                                            (full_df['intent'] == 'Relaxing') &
                                            ((full_df['vulnerable_flag'] ==0 )), 1, 0)

full_df['event_gathering_reopen'] = np.where((full_df['intent'] == 'Relaxing') &
                                      ((full_df['subcategory'] == 'Event delays or cancellations') |
                                      (full_df['subcategory'] == 'Mass gathering restrictions')) &
                                       ((full_df['vulnerable_flag'] == 0) ), 1, 0)

#Aggregate the data by state and date:
date_agg = {'has_safer_at_home': 'sum',
           'has_stay_at_home':'sum',
           'private_sector_closed':'sum',
          'school_closure':'sum',
           'event_gathering_restrict': 'sum',
          'private_sector_reopen': 'sum',
           'event_gathering_reopen': 'sum',
           'ID': 'nunique'
           }
state_by_day = full_df.groupby(['state', 'date_var']).agg(date_agg).reset_index()
state_by_day['date_var'] = state_by_day['date_var'].dt.strftime('%Y-%m-%d')



#Apply all the rules
state_by_day['stay_at_home_status'] = np.where(
    ((state_by_day['private_sector_reopen'] ==0) & (state_by_day['event_gathering_reopen'] == 0)) &
    (((state_by_day['has_stay_at_home'] >0) & (state_by_day['has_safer_at_home'] == 0)) |
       ((state_by_day['private_sector_closed'] > 0) &
        (state_by_day['school_closure'] > 0) &
        (state_by_day['event_gathering_restrict'] > 0))), 1,0)

#if there are relaxing policies in place, you are considered 'safer at home'
state_by_day['safer_at_home_reopen'] = np.where(
    ((state_by_day['private_sector_reopen'] !=0) |
     (state_by_day['event_gathering_reopen'] != 0) |
    (state_by_day['has_safer_at_home'] > 0)), 1,0)


#safer at home by policy expiration- private sector and event gathering both expired, or stay at home expired:
state_by_day['safer_at_home_expire'] = np.where(
    ((state_by_day['private_sector_closed'] == 0) &
     (state_by_day['event_gathering_restrict'] == 0) &
    (state_by_day['has_stay_at_home'] ==0)), 1,0)

#only have fully open if school closures are lifted, for now?
state_by_day['new_open_status'] = np.where(
    ((state_by_day['safer_at_home_reopen'] ==0)  &
     (state_by_day['private_sector_closed'] == 0) &
     (state_by_day['stay_at_home_status'] == 0) &
     (state_by_day['school_closure'] == 0) &
      (state_by_day['event_gathering_restrict'] == 0)), 1,0)

state_by_day['prior_day_stay'] = state_by_day.stay_at_home_status.shift()
state_by_day['prior_day_safer_reopen'] = state_by_day.safer_at_home_reopen.shift()
state_by_day['prior_day_safer_expire'] = state_by_day.safer_at_home_expire.shift()

state_by_day['Status'] = 0
state_by_day['Status'] = np.where(
    ((state_by_day['new_open_status'] == 1) & (state_by_day['prior_day_safer_expire'] ==1)), 'New open',
    np.where(
        ((state_by_day['stay_at_home_status']==1) & (state_by_day['safer_at_home_reopen'] == 0)), 'Stay at home',
    np.where(
        ((state_by_day['safer_at_home_reopen'] == 1) & (state_by_day['stay_at_home_status']==1)), 'Safer at home',
    np.where(
        (state_by_day['safer_at_home_reopen'] == 1), 'Safer at home',
    np.where(
        ((state_by_day['safer_at_home_expire'] == 1) & (state_by_day['stay_at_home_status']==0)), 'Safer at home', 'Pre-covid')))))

base_key_update = 'appd8zCyjJqdgYL27'
table_name_update = 'Test_data'
AIRTABLE_KEY = 'keypoSTyPHy3P2YFo'
airtable_update = Airtable(base_key_update, table_name_update, api_key=AIRTABLE_KEY)
records2 = airtable_update.get_all()

#get the data that needs to be updated:
state_by_day['Location type'] = 'State'
data_test = state_by_day[['state', 'date_var', 'Location type', 'Status', 'has_safer_at_home', 'has_stay_at_home',
       'private_sector_closed', 'school_closure', 'event_gathering_restrict',
       'private_sector_reopen', 'event_gathering_reopen',
       'stay_at_home_status', 'safer_at_home_reopen',
       'new_open_status']]

data_test.columns = ('Name','Date','Location type','Status','Safer at home policy',
'Stay at home policy','Private sector closures','School closures','Event or gathering restrictions',
'Private sector reopening','Event reopening','Stay at home status','Safer at home reopening','New open status')

rec_updates = data_test.to_dict('records')
#will need to figure out how to overwrite or append. This takes a long time right now
airtable_update.batch_insert(rec_updates)
