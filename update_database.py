
############################## DEPENDENCIES ##############################
import pandas as pd
import sqlite3
from datetime import datetime

############################## IMPORT DATA AND 'PROJECT' DF CREATION ##############################
# import the 'customers table' data
project_data = "data/customers_table.csv"

# reading the 'project_data' into a df
project_df = pd.read_csv(
  project_data,
  dtype={'Claim #': str, 'Job #': str},

  # directing pd to create datetime values for the 'date' fields
  parse_dates=[
    'Claim # Date',
    'FTA Scope. Req Date',
    'Submit for Estimate Date', 
    '[OB] Created Scope Calc',
    '[B] Created Estimate Date', 
    'Job Submittal Date',
    '[B] - Date Approved by BC', 
    '[OB] Completed'])

############################## 'PRE/IN PRODUCTION' DF CREATION ##############################
# created 'in_production_df' to hold all current jobs in production, to avoid 'NaN' date values
in_production_df = (project_df.loc[
  (project_df['[OB] Completed'].isnull() == False) & (project_df["Job #"].isnull() == False), :])

# created 'pre_production_df' to hold all jobs not yet in production, still has 'NaN' date values
pre_production_df = (project_df.loc[(project_df["Job #"].isnull() == True), :])

############################## 'DAYS' DF CREATION ##############################
# lists to collect the 'date diffs'
claim_num = []                         
job_num = []
rep_claim_diff = []
fta_scopes_diff = []
ob_scope_diff = []
bc_estimate_diff = []
sup_pfynr_diff = []
bc_approvals_diff = []
ob_order_builds_diff = []
total_days = []

# iterating over the 'in_production_df' to create 'date diff' variables
for index, row in in_production_df.iterrows():

  # creating 'date_diff' variables for each step in the workflow
  rep_claim_date_diff = (row["FTA Scope. Req Date"] - row["Claim # Date"]).days
  fta_date_diff = (row["Submit for Estimate Date"] - row["FTA Scope. Req Date"]).days
  ob_scope_date_diff = (row['[OB] Created Scope Calc'] - row['Submit for Estimate Date']).days
  bc_estimate_date_diff = (row['[B] Created Estimate Date'] - row['[OB] Created Scope Calc']).days
  sup_pfynr_date_diff = (row["Job Submittal Date"] - row["[B] Created Estimate Date"]).days
  bc_approval_date_diff = (row["[B] - Date Approved by BC"] - row["Job Submittal Date"]).days
  ob_orderbuild_date_diff = (row['[OB] Completed'] - row['[B] - Date Approved by BC']).days
  
  # adding up all of the 'date_diff' variables above and assigning to 'total_days_sum'
  day_diffs = [
    rep_claim_date_diff, fta_date_diff, ob_scope_date_diff,
    bc_estimate_date_diff, sup_pfynr_date_diff, 
    bc_approval_date_diff, ob_orderbuild_date_diff
    ]
  
  total_days_sum = sum(day_diffs)
  
  # appending the 'claim #' and 'job #' values to ID records in table
  claim_num.append(row["Claim #"])
  job_num.append(row["Job #"])

  # appending each date value in the workflow
  rep_claim_diff.append(rep_claim_date_diff)
  fta_scopes_diff.append(fta_date_diff)
  ob_scope_diff.append(ob_scope_date_diff)
  bc_estimate_diff.append(bc_estimate_date_diff)
  sup_pfynr_diff.append(sup_pfynr_date_diff)
  bc_approvals_diff.append(bc_approval_date_diff)
  ob_order_builds_diff.append(ob_orderbuild_date_diff)

  #appending the sum of each date value in the workflow
  total_days.append(total_days_sum)

  # creating the 'days_df' to hold ID's and date values
days_df = pd.DataFrame({
    "claim_#" : claim_num,
    "job_#" : job_num,
    "rep_claim" : rep_claim_diff, 
    "fta_scope" : fta_scopes_diff,
    "ob_scope" : ob_scope_diff,
    "bc_estimate" : bc_estimate_diff,
    "sup_pfynr" : sup_pfynr_diff,
    "bc_approval" : bc_approvals_diff,
    "ob_orderbuild" : ob_order_builds_diff,
    "total_days" : total_days
})    

############################## EXPORT DATA ##############################
# writes the 'in_production_df', 'pre_production_df', and 'days_df' to csv files
in_production_df.to_csv("data/in_production.csv", index=False)
pre_production_df.to_csv("data/pre_production.csv", index=False)
days_df.to_csv("data/workflow_days.csv", index=False)