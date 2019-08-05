
## Dependencies
import pandas as pd
import datetime
from datetime import datetime
from datetime import timedelta
from datetime import date


############################## IMPORT DATA #########################

# imports the '[TVA] Workflow Analysis' data
all_project_data = "./data/workflow_analysis.csv"

# imports the '[TVA] Project Workflow Analysis' data
all_production_data = "./data/project_workflow_analysis.csv"

# imports '[TVA] Project Info Analysis' data
info_data = "./data/info_table.csv"

# imports '[TVA] FTA Scope Analysis' data
rejection_data = "./data/rejection_table.csv"

# imports '[TVA] GM Change Order Analysis' data
change_order_data = "./data/change_orders.csv"

# imports '[TVA] GM Labor Order Adjustment Analysis' data
labor_adjustment_data = "./data/labor_adjustments.csv"


############################## PARSE DATA #########################

## Workflow Analysis
project_df = pd.read_csv(
  all_project_data, 
  dtype={'Claim #':str,'Job #':str,'Branch':str,'Claim Status':str},
  parse_dates=[
    'Claim # Date','FTA Scope. Req Date','Submit for Estimate Date',
    '[OB] Created Scope Calc','[B] Created Estimate Date','Job Submittal Date',
    '[B] - Date Approved by BC','[OB] Completed','COC Rcvd Date [A]',
    'Job Docs Scanned','[B] Sent Invoice Packet to Ins Co','[B] Settled with Insurance'])

# having trouble recognizing 'coc' date as 'datetime', manually converted the dtype.
project_df['COC Rcvd Date [A]'] = pd.to_datetime(project_df['COC Rcvd Date [A]'], errors='coerce')

# storing all floored timestamps in a list
floored_ob_order_builds = []

# iterating over the 'project_df' to 'floor' or zero out each timestamp
for index, row in project_df.iterrows():
  
  # zeroing out the hours and minutes, appending value to 'floored' list
  ob_order_builds = row['[OB] Completed'].replace(hour=0, minute=0)
  floored_ob_order_builds.append(ob_order_builds)

# adding the floored list to the 'project_df'
project_df['[OB] Completed'] = floored_ob_order_builds

#########################
## Project Workflow Analysis
production_df = pd.read_csv(
  all_production_data, dtype={
    'Job #':str,
    'Supplier Name':str,
    'Building Department':str,
    'Permit Req?':str},
  parse_dates=[
    'Permit Applied [A]',
    'Order Date',
    'Permit Received',
    'OA Date',
    'Invoice Date',
    'Ntfd H.O. Dlvry',
    'Dlvry Start',
    'Ntfd H.O. Start',
    'Roof Start',
    'Roof Complete Date',
    'R4F',
    'Requested Final Insp',
    'Final Inspection Date',
  ]
)

# storing all floored timestamps in a list
floored_pa_oa_processed = []

# iterating over the 'production_df' to 'floor' or zero out each timestamp
for index, row in production_df.iterrows():
  
  # zeroing out the hours and minutes, appending value to 'floored' list
  pa_oa_processed = row['OA Date'].replace(hour=0, minute=0)
  floored_pa_oa_processed.append(pa_oa_processed)

# adding the floored list to the 'production_df'
production_df['OA Date'] = floored_pa_oa_processed

# data to be moved to the 'info' df
moving_data_df = production_df[['Job #','Supplier Name', 'Building Department', 'Permit Req?']]
del production_df['Supplier Name'], production_df['Building Department'], production_df['Permit Req?']

#########################
## FTA Scope Analysis
rejection_info_df = pd.read_csv(
  rejection_data,dtype={
    'Claim #': str, 
    'Job #': str},
  parse_dates=['Created'])

# storing all floored timestamps in a list
floored_fta_rejections = []

# iterating over the 'rejection_info_df' to 'floor' or zero out each timestamp
for index, row in rejection_info_df.iterrows():
  
  # zeroing out the hours and minutes, appending value to 'floored' list
  fta_rejections = row['Created'].replace(hour=0, minute=0)
  floored_fta_rejections.append(fta_rejections)

# adding the floored list to the 'rejection_info_df'
rejection_info_df['Created'] = floored_fta_rejections

### Latest Rejection
# 'idmax()' of the 'Created' column provides the most current rejection date
reject_df = rejection_info_df.loc[rejection_info_df.groupby('Claim #')['Created'].idxmax()]
reject_df = reject_df.rename(columns={"Created": "Scope Rejection Date"})

###  Multi-Rejection Counts
rejection_count_df = (rejection_info_df.groupby("Claim #").count())
rejection_count_df.reset_index(inplace=True)

# created a list to collect the boolean response to multi-rejection count
multi_reject_list = []

# iterates over 'Created' to determine if it has been multi-rejected
for index, row in rejection_count_df.iterrows():
  if row['Created'] <= 1:
    multi_reject = False 
    multi_reject_list.append(multi_reject)
  else:
    multi_reject = True
    multi_reject_list.append(multi_reject)

# adding the 'boolean' list to the 'improvements' df
rejection_count_df["Multi-rejected"] = multi_reject_list
rejection_count_df = rejection_count_df.rename(columns={"Created": "Scope Rejections"})

#########################
## GM Change Order Analysis
change_order_df = pd.read_csv(
    change_order_data,dtype={'Job #':str},parse_dates=['Created'])

# storing all floored timestamps in a list
floored_change_order = []

# iterating over the 'change_order_df' to 'floor' or zero out each timestamp
for index, row in change_order_df.iterrows():
  
  # zeroing out the hours and minutes, appending value to 'floored' list
  gm_change_order = row['Created'].replace(hour=0, minute=0)
  floored_change_order.append(gm_change_order)

# adding the floored list to the 'change_order_df'
change_order_df['Created'] = floored_change_order

## Change Order Date
co_date_df = change_order_df.loc[change_order_df.groupby('Job #')['Created'].idxmax()]
co_date_df = co_date_df.rename(columns={"Created": "GM Change Order Date"})

## Change Order Count
co_count_df = (change_order_df.groupby("Job #").count())
co_count_df.reset_index(inplace=True)
co_count_df = co_count_df.rename(columns={"Created": "Change Orders"})

#########################
## GM Labor Adjustment Analysis
labor_adjustment_df = pd.read_csv(labor_adjustment_data, dtype={'Order ID': str}, parse_dates=['Created'])

# storing all floored timestamps in a list
floored_labor_adjustment = []

# iterating over the 'labor_adjustment_df' to 'floor' or zero out each timestamp
for index, row in labor_adjustment_df.iterrows():
  
  # zeroing out the hours and minutes, appending value to 'floored' list
  gm_labor_adjustment = row['Created'].replace(hour=0, minute=0)
  floored_labor_adjustment.append(gm_change_order)

# adding the floored list to the 'labor_adjustment_df'
labor_adjustment_df['Created'] = floored_labor_adjustment

# list to store each 'job #' from string split
job_num_list = []

# splitting the 'order id' to get the job #
for index, row in labor_adjustment_df.iterrows():
    job_num = row['Order ID'].split('-')[0]
    job_num_list.append(job_num)

# creating a 'job #' column in the df    
labor_adjustment_df['Job #'] = job_num_list
del labor_adjustment_df['Order ID']

### Labor Adjustment Date
la_date_df = labor_adjustment_df.loc[labor_adjustment_df.groupby('Job #')['Created'].idxmax()]
la_date_df = la_date_df.rename(columns={"Created": "GM Labor Adjustment Date"})

### Labor Adjustment Count
la_count_df = (labor_adjustment_df.groupby("Job #").count())
la_count_df.reset_index(inplace=True)
la_count_df = la_count_df.rename(columns={"Created": "Labor Adjustments"})

## Project Info Analysis
info_df = pd.read_csv(info_data, dtype={'Job #':str})
info_df = info_df.rename(columns={
    'Sup Name': 'Sup',
    'Rep Name': 'Rep',
    '[BC] Name': 'BC',
    'Full Name': 'OB',
    'Full Name.1': 'FTA',
    'Full Name.2': 'GM'
})

# 'info_df' holds all non-date stamped project data
info_df = info_df.merge(
  # supplier name, building department, permit requirements
  moving_data_df, how='left', on='Job #').merge(
  # fta scope rejection count, multi-rejection boolean
  rejection_count_df, how='left', on='Claim #').merge(
  # change order count on the project
  co_count_df, how='left',on='Job #').merge(
  # labor adjustment count on the project
  la_count_df, how='left',on='Job #')


# Organizing Project Info Data
info_df = info_df[[
  'Claim #',
  'Job #',
  'Branch',
  'City',
  'Building Department',
  'Permit Req?',
  'Supplier Name',
  'Crew',
  'Insurance Company',
  'Multi-rejected',
  'Scope Rejections',
  'Change Orders',
  'Labor Adjustments',
  'Sup',
  'Rep',
  'FTA',
  'BC',
  'OB',
  'GM'
]]

############################## MERGE DATA #########################

# Merged the 'project df' merged with 'latest rejection date' merged with 
# rejection count and 'multi-rejection' boolean merged with 'production df' 
merged_df = project_df.merge(
  # date of FTA Scope Rejection
  reject_df, how='left', on='Claim #').merge(
  # workflow datestamps from production
  production_df, how='left', on=['Job #', 'Branch']).merge(
  # date of GM Change Order
  co_date_df, how='left', on='Job #').merge(
  # date of GM Labor Adjustment
  la_date_df, how='left', on='Job #')

# Renaming Merged Data
all_project_df = merged_df.rename(columns={
    'Claim # Date': 'Rep Agreement Signed',
    'FTA Scope. Req Date': 'Rep Claim Collected',
    'Scope Rejection Date': 'FTA Scope Rejected',
    'Submit for Estimate Date': 'FTA Scope Completed',
    '[B] Created Estimate Date':'BC Estimate Completed',
    '[OB] Created Scope Calc': 'OB Scope Completed',
    'Job Submittal Date': 'Sup Job Submitted',
    '[B] - Date Approved by BC': 'BC Approved for Production',
    '[OB] Completed': 'OB Order Built',
    'Permit Applied [A]': 'PA Permit Applied',
    'Order Date': 'GM Order Processed',
    'Permit Received': 'PA Permit Processed',
    'OA Date': 'PA OA Processed',
    'Invoice Date':'PA OA Invoiced',
    'Ntfd H.O. Dlvry': 'PA Notify of Delivery',
    'Dlvry Start': 'Delivery Date',
    'Ntfd H.O. Start': 'PA Notify of Start',
    'Roof Complete Date': 'Roof End',
    'R4F': 'GM Approved for Inspection',
    'Requested Final Insp': 'RA Inspection Requested',
    'Final Inspection Date': 'RA Inspection Processed',
    'COC Rcvd Date [A]': 'Rep COC Collected',
    'Job Docs Scanned': 'SA Job Docs Uploaded',
    '[B] Sent Invoice Packet to Ins Co': 'BC Project Invoiced',
    '[B] Settled with Insurance':'BC Project Closed'
})

# Organizing Merged Data to follow Workflow
all_project_df = all_project_df[[
    'Claim #',
    'Job #',
    'Branch',
    'Claim Status',
    'Rep Agreement Signed',
    'Rep Claim Collected',
    'FTA Scope Completed',
    'FTA Scope Rejected',
    'BC Estimate Completed',
    'OB Scope Completed',
    'Sup Job Submitted',
    'BC Approved for Production',
    'OB Order Built',
    'GM Order Processed',
    'PA Permit Applied',
    'PA Permit Processed',
    'PA OA Processed',
    'PA OA Invoiced',
    'PA Notify of Delivery',
    'PA Notify of Start',
    'Delivery Date',
    'Roof Start',
    'Roof End',
    'GM Approved for Inspection',
    'GM Change Order Date',
    'GM Labor Adjustment Date',
    'RA Inspection Requested',
    'RA Inspection Processed', 
    'Rep COC Collected',
    'SA Job Docs Uploaded',
    'BC Project Invoiced',
    'BC Project Closed'
]]

############################## COMPARE DATA #########################

# will store this project info
claim_num = []
branch_list = []
claim_status_list = []

# will store these date diffs
rep_claim_diff = []
fta_scope_diff = []
ob_scope_diff = []
bc_estimate_diff = []
sup_pfynr_diff = []
bc_approval_diff = []
ob_order_build_diff = []
gm_create_order_diff = []
pa_oa_processed_diff = []
pa_invoice_diff = []
gm_approval_diff = []
ra_request_inspection_diff = []
rep_coc_collected_diff = []
sa_docs_uploaded_diff = []
bc_project_invoiced_diff = []
bc_project_closed_diff = []

# this data applies to leadtimes, not workflow
pa_permit_applied_diff = []
pa_permit_processed_diff = []
pa_notify_delivery_diff = []
pa_notify_start_diff = []


# iterating over the df to create 'date diff' variables
for index, row in all_project_df.iterrows():

  # creating 'date_diff' variables for each step in the workflow
  rep_claim_date_diff = float((row['Rep Claim Collected'] - row['Rep Agreement Signed']).days)
  
####################################################################################################
  # if the bc estimate was created prior to July 16th...

  if row['BC Estimate Completed'] <= datetime(2019, 7, 15):    
    
    # and if the record did NOT had the FTA Scope Rejected...
    if row['FTA Scope Rejected'] != row['FTA Scope Rejected']:
      
      # then compare the date diffs using the 'fta scope completed' date field
      fta_date_diff = (row['FTA Scope Completed'] - row['Rep Claim Collected']).days
      ob_scope_date_diff = (row['OB Scope Completed'] - row['FTA Scope Completed']).days
      bc_estimate_date_diff = (row['BC Estimate Completed'] - row['OB Scope Completed']).days
      sup_pfynr_date_diff = (row['Sup Job Submitted'] - row['BC Estimate Completed']).days
      
    # but if the record has had the FTA Scope Rejected...
    else:
      # then compare the 'FTA Scope rejected' date field
      fta_date_diff = (row['FTA Scope Rejected'] - row['Rep Claim Collected']).days
      ob_scope_date_diff = (row['OB Scope Completed'] - row['FTA Scope Rejected']).days
      
      
      bc_estimate_date_diff = (row['BC Estimate Completed'] - row['OB Scope Completed']).days
      sup_pfynr_date_diff = (row['Sup Job Submitted'] - row['OB Scope Completed']).days
      
  # if the bc estimate was addressed during the 'blip' on 7/16-7/17...
  elif row['BC Estimate Completed'] == datetime(2019, 7, 16) or row['BC Estimate Completed'] == datetime(2019, 7, 17):
    
    # and if the record did NOT had the FTA Scope Rejected...
    if row['FTA Scope Rejected'] != row['FTA Scope Rejected']:
      # then compare the 'bc estimate' to the 'blip' date, and the 'ob scope' date to the new 'bc date'
      fta_date_diff = (row['FTA Scope Completed'] - row['Rep Claim Collected']).days
      bc_estimate_date_diff = (row['BC Estimate Completed'] - datetime(2019, 7, 16)).days
      ob_scope_date_diff = (row['OB Scope Completed'] - row['BC Estimate Completed']).days
      sup_pfynr_date_diff = (row['Sup Job Submitted'] - row['OB Scope Completed']).days
      
    # but if the record has had the FTA Scope rejected...
    else:
      # then compare the 'fta scope rejected' date field
      fta_date_diff = (row['FTA Scope Rejected'] - row['Rep Claim Collected']).days
      bc_estimate_date_diff = (row['BC Estimate Completed'] - datetime(2019, 7, 15)).days
      ob_scope_date_diff = (row['OB Scope Completed'] - row['FTA Scope Rejected']).days
      sup_pfynr_date_diff = (row['Sup Job Submitted'] - row['OB Scope Completed']).days
      
  # if the bc estimate was created after the 'blip'...
  else:
    # and if the record did NOT had the FTA Scope Rejected...
    if row['FTA Scope Rejected'] != row['FTA Scope Rejected']:
      # then compare the 'fta scope completed' date field
      fta_date_diff = (row['FTA Scope Completed'] - row['Rep Claim Collected']).days
      bc_estimate_date_diff = (row['BC Estimate Completed'] - row['FTA Scope Completed']).days
      ob_scope_date_diff = (row['OB Scope Completed'] - row['BC Estimate Completed']).days
      sup_pfynr_date_diff = (row['Sup Job Submitted'] - row['OB Scope Completed']).days
      
    else:
      # then compare the 'fta scope rejected' date field
      fta_date_diff = (row['FTA Scope Rejected'] - row['Rep Claim Collected']).days
      bc_estimate_date_diff = (row['BC Estimate Completed'] - row['FTA Scope Completed']).days
      ob_scope_date_diff = (row['OB Scope Completed'] - row['FTA Scope Rejected']).days
      sup_pfynr_date_diff = (row['Sup Job Submitted'] - row['OB Scope Completed']).days
  
####################################################################################################
  # due to manual OA 'Processed' and 'Invoice' date fields, PAs have been recording false dates...

  if row['PA OA Invoiced'] < row['PA OA Processed']:

    # ...which provide no advantage to the project.
    row['PA OA Invoiced'] = row['PA OA Processed'] + timedelta(days=1)
  
  # OAs can't be invoiced before they have been processed (approved)
  pa_invoice_date_diff = (row['PA OA Invoiced'] - row['PA OA Processed']).days
  
####################################################################################################
  # due to manual 'Approved for inspection' (R4F) date field, GMs have been recording false dates...

  if row['GM Approved for Inspection'] < row['Roof End']:

    # ...which provide no advantage to the project; those will be reset to the day after the build.
    row['GM Approved for Inspection'] = row['Roof End'] + timedelta(days=1)
  
  # roofs can't be approved for inspection prior to the roof being built
  gm_approval_date_diff = (row['GM Approved for Inspection'] - row['Roof End']).days

####################################################################################################
  # due to manual 'COC Collected' (COC Rcvd [A]) date field, SAs have been recording false dates...

  if row['Rep COC Collected'] < row['Roof End']:

    # ...these will be reset for after the build.
    row['Rep COC Collected'] = row['Roof End'] + timedelta(days=1)
  
  # coc's can't be collected until after the roof is built, not before
  rep_coc_collected_date_diff = (row['Rep COC Collected'] - row['Roof End']).days
  
####################################################################################################
  # these dates do not have any special circumstances and can be directly compared

  bc_approval_date_diff = (row['BC Approved for Production'] - row['Sup Job Submitted']).days
  ob_orderbuild_date_diff = (row['OB Order Built'] - row['BC Approved for Production']).days
  gm_create_order_date_diff = (row['GM Order Processed'] - row['OB Order Built']).days
  ra_requested_inspection_date_diff = (row['RA Inspection Requested'] - row['GM Approved for Inspection']).days
  sa_docs_uploaded_date_diff = (row['SA Job Docs Uploaded'] - row['Rep COC Collected']).days
  bc_project_invoiced_date_diff = (row['BC Project Invoiced'] - row['SA Job Docs Uploaded']).days
  bc_project_closed_date_diff = (row['BC Project Closed'] - row['BC Project Invoiced']).days
  
  # these dates are manual, outliers are due to incorrect / false data entry
  pa_oa_processed_date_diff = (row['PA OA Processed'] - row['GM Order Processed']).days

  # these provide the lead times of tasks not directly impacting the workflow.
  pa_permit_applied_date_diff = (row['PA Permit Applied'] - row['BC Approved for Production']).days
  pa_permit_processed_date_diff = (row['PA Permit Processed'] - row['PA Permit Applied']).days
  pa_notify_delivery_date_diff = (row['Delivery Date'] - row['PA Notify of Delivery']).days
  pa_notify_start_date_diff = (row['Roof Start'] - row['PA Notify of Start']).days
  
####################################################################################################
  # appending 'date diff' values to lists to create each df column
  
  claim_num.append(row["Claim #"])
  branch_list.append(row['Branch'])
  claim_status_list.append(row['Claim Status'])
  rep_claim_diff.append(rep_claim_date_diff)
  fta_scope_diff.append(fta_date_diff)
  ob_scope_diff.append(ob_scope_date_diff)
  bc_estimate_diff.append(bc_estimate_date_diff)
  sup_pfynr_diff.append(sup_pfynr_date_diff)
  bc_approval_diff.append(bc_approval_date_diff)
  ob_order_build_diff.append(ob_orderbuild_date_diff)
  gm_create_order_diff.append(gm_create_order_date_diff)
  pa_oa_processed_diff.append(pa_oa_processed_date_diff)
  pa_invoice_diff.append(pa_invoice_date_diff)
  gm_approval_diff.append(gm_approval_date_diff)
  rep_coc_collected_diff.append(rep_coc_collected_date_diff)
  sa_docs_uploaded_diff.append(sa_docs_uploaded_date_diff)
  bc_project_invoiced_diff.append(bc_project_invoiced_date_diff)
  bc_project_closed_diff.append(bc_project_closed_date_diff)
  
  # this data applies to leadtimes, not workflow 
  pa_permit_applied_diff.append(pa_permit_applied_date_diff)
  pa_permit_processed_diff.append(pa_permit_processed_date_diff)
  pa_notify_delivery_diff.append(pa_notify_delivery_date_diff)
  pa_notify_start_diff.append(pa_notify_start_date_diff)
  ra_request_inspection_diff.append(ra_requested_inspection_date_diff)
####################################################################################################

## Creating 'Workflow Days' df
days_df = pd.DataFrame({
  "Claim #": claim_num,
  "Rep Collecting Claim": rep_claim_diff,
  "FTA Completing Scope": fta_scope_diff,
  "BC Completing Estimate": bc_estimate_diff,
  "OB Completing Scope": ob_scope_diff,
  "Sup Submitting Job": sup_pfynr_diff,
  "BC Approving Job": bc_approval_diff,
  "OB Building Order": ob_order_build_diff,
  "GM Processing Order": gm_create_order_diff,
  "PA Processing OA": pa_oa_processed_diff,
  "PA Invoicing OA": pa_invoice_diff,
  'GM Approving for Inspection': gm_approval_diff,
  'RA Requesting Inspection': ra_request_inspection_diff,
  'Rep Collecting COC': rep_coc_collected_diff,
  'SA Uploading Docs': sa_docs_uploaded_diff,
  'BC Invoicing Project':bc_project_invoiced_diff,
  'BC Closed Project':bc_project_closed_diff
})

# creating a column holding the running tally across a row (project)
# can be done because not including 'date diffs' on non-workflow items
days_df['Days in Pipeline'] = days_df.sum(axis=1)

############################## EXPORT DATA #########################
all_project_df.to_csv("data/cleaned_data/project_table.csv", index=False)

days_df.to_csv("data/cleaned_data/workflow_table.csv", index=False)

info_df.to_csv("data/cleaned_data/project_info_table.csv", index=False)

