#!/usr/bin/env python
# coding: utf-8

# # Setup

# ## Dependencies

# In[105]:


import pandas as pd
from datetime import datetime


# ## Import Data

# In[106]:


# imports the '[W] Customers Table Audit' data
project_data = "./data/customers_table.csv"

# imports '[W] Scope Improvements' data
improvements_data = "./data/improvement_table.csv"

# imports '[W] Teammate Audit' data
teammate_data = "./data/teammate_table.csv"


# # Parsing Data

# ## Separate 'In Production' Jobs
# 
# **Jobs where '[OB] Completed' and 'Job #' is not blank**

# In[107]:


project_df = pd.read_csv(
    project_data, dtype={'Claim #': str, 'Job #': str},
    parse_dates=[
        'Claim # Date',
        'FTA Scope. Req Date',
        'Submit for Estimate Date',
        '[OB] Created Scope Calc',
        '[B] Created Estimate Date',
        'Job Submittal Date',
        '[B] - Date Approved by BC',
        '[OB] Completed']
)

# created 'in_production_df' to hold all current jobs in production, to avoid 'NaN' date values
in_production_df = (project_df.loc[(project_df['[OB] Completed'].isnull() == False) & 
                                   (project_df["Job #"].isnull() == False), :])


# ## Separate 'Most Recent' Improvement
# 
# **Most recent FTA Scope Improvement submitted by the OB**

# In[108]:


# created 'improvements_df' to hold all dates of fta scope rejections to use correct date
improvements_df = pd.read_csv(
    improvements_data,
    dtype={'Claim #': str, 'Job #': str},
    parse_dates=['Created'])

# 'improvement_dates_df' holds all current jobs in production, to avoid 'NaN' date values
improvement_dates_df = improvements_df.loc[improvements_df["Job #"].isnull() == False, :]

# 'unique_improv_dates_df' holds most recent 'created' dates for improvements;
# 'idmax()' provides the most current date
unique_improv_dates_df = improvement_dates_df.loc[
    improvement_dates_df.groupby('Claim #')['Created'].idxmax()]


# renaming the 'unique improvements' df to make it easier to merge
unique_improv_dates_df = unique_improv_dates_df.rename(columns={"Created": "Improvement Date"})


# ## Separate 'Teammate' Information
# 
# **Only keep Jobs that are in production**

# In[109]:


# created 'teammate_df' to hold all teammate info for jobs
teammate_df = pd.read_csv(teammate_data, dtype= object)

#'production_teammate_df' holds teammate info for jobs in production
production_teammate_df = teammate_df.loc[ teammate_df["Job #"].isnull() == False, :]

# renames the columns in 'production_teammate_df' 
production_teammate_df = production_teammate_df.rename(columns = {
    "[BC] Name" : "BC Name",
    "Full Name" : "OB Name",
    "Full Name.1": "FTA Name",
    "Full Name.2": "GM Name"
})


# # Comparing Data

# ## Merge 'In Production' and 'Most Recent' dataframes
# **Merging dfs on the shared 'Claim #' Column**

# In[110]:


# merges 'in_production_df' and 'unique_improv_dates_df' on the 'claim #' column
merged_df = pd.merge(in_production_df, unique_improv_dates_df, how= 'left', on="Claim #")


# ## Compare the 'Submit' and 'Improvement' Dates
# **If 'Improvement' Date exists, use instead of 'Submit' Date**

# In[111]:


# list to store either 'submit' date or 'improvement' date to update 'workflow_df'
correct_submit_date = []

# counter to keep track of how many files have been updated with 'improvement' date
record_change_count = 0

# iterates over the 'merged_df' to address 'submit for estimate' and 'improvement' dates
for index, row in merged_df.iterrows():
    
    # if 'improvement' date is empty, use 'submit for estimate' for calculations
    if row['Improvement Date'] != row['Improvement Date'] :

        # adding the original date to the 'correct' list
        correct_submit_date.append(row['Submit for Estimate Date'])
    
    # ...if not, use 'improvement' date for calculations
    else:
        # adding the 'improvement' date to the 'correct' list
        correct_submit_date.append(row['Improvement Date'])
        record_change_count += 1


# ## Create 'Workflow' dataframe w/correct Dates
# **'Workflow' df contains jobs currently in production**

# In[112]:


#  using 'correct_submit_date' list; used to create the 'days' df
workflow_df = pd.DataFrame({
    'Claim #': merged_df['Claim #'],
    'Job #': merged_df['Job #_x'],
    'Claim # Date':merged_df['Claim # Date'],
    'FTA Scope. Req Date': merged_df['FTA Scope. Req Date'],
    'Submit for Estimate Date': correct_submit_date,
    '[OB] Created Scope Calc': merged_df['[OB] Created Scope Calc'],
    '[B] Created Estimate Date': merged_df['[B] Created Estimate Date'],
    'Job Submittal Date': merged_df['Job Submittal Date'],
    '[B] - Date Approved by BC': merged_df['[B] - Date Approved by BC'],
    '[OB] Completed': merged_df['[OB] Completed']
})


# ## Creating 'Workflow Days' Value from 'workflow_df'

# In[113]:


# list to store the 'date diffs' value for each step'
claim_num = []
job_num = []
rep_claim_diff = []
fta_scope_diff = []
ob_scope_diff = []
bc_estimate_diff = []
sup_pfynr_diff = []
bc_approval_diff = []
ob_order_build_diff = []
total_days = []

# iterating over the df to create 'date diff' variables
for index, row in workflow_df.iterrows():

    # creating 'date_diff' variables for each step in the workflow
    rep_claim_date_diff = (row["FTA Scope. Req Date"] - row["Claim # Date"]).days
    fta_date_diff = (row["Submit for Estimate Date"] - row["FTA Scope. Req Date"]).days
    ob_scope_date_diff = (row['[OB] Created Scope Calc'] - row['Submit for Estimate Date']).days
    bc_estimate_date_diff = (row['[B] Created Estimate Date'] - row['[OB] Created Scope Calc']).days
    sup_pfynr_date_diff = (row["Job Submittal Date"] - row["[B] Created Estimate Date"]).days
    bc_approval_date_diff = (row["[B] - Date Approved by BC"] - row["Job Submittal Date"]).days
    ob_orderbuild_date_diff = (row['[OB] Completed'] - row['[B] - Date Approved by BC']).days

    # adding up all of the 'date_diff' variables above and assigning to 'total_days_sum'
    day_diffs = [rep_claim_date_diff, fta_date_diff, ob_scope_date_diff, bc_estimate_date_diff,
                 sup_pfynr_date_diff, bc_approval_date_diff, ob_orderbuild_date_diff]
    total_days_sum = sum(day_diffs)

    # appending 'date diff' values to lists to create each df column
    claim_num.append(row["Claim #"])
    job_num.append(row["Job #"])
    rep_claim_diff.append(rep_claim_date_diff)
    fta_scope_diff.append(fta_date_diff)
    ob_scope_diff.append(ob_scope_date_diff)
    bc_estimate_diff.append(bc_estimate_date_diff)
    sup_pfynr_diff.append(sup_pfynr_date_diff)
    bc_approval_diff.append(bc_approval_date_diff)
    ob_order_build_diff.append(ob_orderbuild_date_diff)
    total_days.append(total_days_sum)

# creating the 'days_df' to hold all date values for each role in the project
days_df = pd.DataFrame({
    "claim_#": claim_num,
    "job_#": job_num,
    "rep_claim": rep_claim_diff,
    "fta_scope": fta_scope_diff,
    "ob_scope": ob_scope_diff,
    "bc_estimate": bc_estimate_diff,
    "sup_pfynr": sup_pfynr_diff,
    "bc_approval": bc_approval_diff,
    "ob_orderbuild": ob_order_build_diff,
    "total_days": total_days
})


# # Export Data

# In[49]:


# 'In Production','Workflow Days', and 'teammates' CSVs
workflow_df.to_csv("data/in_production.csv", index=False)
days_df.to_csv("data/workflow_days.csv", index=False)
production_teammate_df.to_csv("data/teammates.csv", index=False)


# # Terminal Report

# In[116]:

print("--------------------------------------------------")
print("WORKFLOW SUMMARY : beginning February 2019")
print("--------------------------------------------------")
print(f"RECORDS IN PRODUCTION ({round((len(in_production_df) / len(project_df)) * 100, 2)}%): {len(in_production_df)}")
print(f"UNIQUE FTA SCOPE REJECTIONS ({round((len(unique_improv_dates_df) / len(improvement_dates_df)) * 100, 2)}%): {len(unique_improv_dates_df)}")
print(f"'SUBMIT ESTIMATE' CHANGED ON RECORD ({round((record_change_count / len(in_production_df))* 100, 2)}%): {record_change_count}")
print(f"RECORDS WITH CREATED WORKFLOW INFORMATION: {len(days_df)}")


# In[ ]:




