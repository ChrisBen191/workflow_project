########## Dependencies ##########
import pandas as pd
import datetime
from datetime import datetime
from datetime import timedelta

from sqlalchemy import create_engine
from config import username, password, hostname, port, database
engine = create_engine(f'mysql+pymysql://{username}:{password}@{hostname}/{database}')

############################## IMPORT DATA ##############################
# imports the '[TVA] Workflow Analysis' data
all_project_data = "./data/raw_datasets/[TVA] Workflow Analysis.csv"
# imports the '[TVA] Project Workflow Analysis' data
all_production_data = "./data/raw_datasets/[TVA] Project Workflow Analysis.csv"
# imports '[TVA] Project Info Analysis' data
info_data = "./data/raw_datasets/[TVA] Project Info Analysis.csv"
# imports '[TVA] FTA Scope Analysis' data
rejection_data = "./data/raw_datasets/[TVA] FTA Scope Analysis.csv"
# imports the 'oa processed' PA data currently being tracked
oa_processed_data = "./data/raw_datasets/Workflow Tracker - PA OA Processed.csv"
# imports the 'oa invoiced' PA data currently being tracked
oa_invoiced_data = "./data/raw_datasets/Workflow Tracker - PA OA Invoiced.csv"
# imports '[TVA] GM Change Order Analysis' data
change_order_data = "./data/raw_datasets/[TVA] GM Change Order Analysis.csv"
# imports '[TVA] GM Labor Order Adjustment Analysis' data
labor_adjustment_data = "./data/raw_datasets/[TVA] GM Labor Adjustment Analysis.csv"
# imports the 'approved for inspection' GM data currently being tracked
approved_for_inspection_data = "./data/raw_datasets/Workflow Tracker - GM Approved for Inspection.csv"
# imports the 'coc collected' SA data currently being tracked
coc_collected_data = "./data/raw_datasets/Workflow Tracker - Rep COC Collected.csv" 
# imports the '[TVA] Eagleview Analysis' data
eagleview_table_data = "./data/raw_datasets/[TVA] EagleView Analysis.csv"

######################################## DATA CLEANING FUNCTIONS ########################################

########## function to create the 'project_df'
def clean_sales_table():
    project_df = pd.read_csv(
        all_project_data, 
        dtype={'Claim #': str, 'Job #': str, 'Branch': str, 'Claim Status': str}, 
        parse_dates=[
            'Claim # Date',
            'FTA Scope. Req Date',
            'Submit for Estimate Date',
            '[OB] Created Scope Calc',
            '[B] Created Estimate Date',
            'Job Submittal Date',
            '[B] - Date Approved by BC',
            '[OB] Completed',
            'COC Rcvd Date [A]',
            'Job Docs Scanned', 
            '[B] Sent Invoice Packet to Ins Co',
            '[B] Settled with Insurance'])

    # having trouble recognizing 'coc' date as 'datetime', manually converted the dtype.
    project_df['COC Rcvd Date [A]'] = pd.to_datetime(project_df['COC Rcvd Date [A]'], errors='coerce')
    
    # removing all duplcates
    project_df = project_df.drop_duplicates(subset='Claim #', keep='first')

    # storing all floored timestamps in a list
    floored_ob_order_builds = []

    # iterating over the 'project_df' to 'floor' or zero out each timestamp
    for index, row in project_df.iterrows():

        # zeroing out the hours and minutes, appending value to 'floored' list
        ob_order_builds = row['[OB] Completed'].replace(hour=0, minute=0)
        floored_ob_order_builds.append(ob_order_builds)

    # adding the floored list to the 'project_df'
    project_df['[OB] Completed'] = floored_ob_order_builds

    return project_df

########## function to create the 'production_df'
def clean_production_table():
    production_df = pd.read_csv(
        all_production_data, 
        dtype={'Job #': str,'Supplier Name': str,'Building Department': str,'Permit Req?': str},
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
            'Final Inspection Date'])
    
    # removing all duplcates
    production_df = production_df.drop_duplicates(subset='Job #', keep='first')

    # storing all floored timestamps in a list
    floored_pa_oa_processed = []

    # iterating over the 'production_df' to 'floor' or zero out each timestamp
    for index, row in production_df.iterrows():

        # zeroing out the hours and minutes, appending value to 'floored' list
        pa_oa_processed = row['OA Date'].replace(hour=0, minute=0)
        floored_pa_oa_processed.append(pa_oa_processed)

    # adding the floored list to the 'production_df'
    production_df['OA Date'] = floored_pa_oa_processed

    return production_df

########## function to create the 'info_df'
def clean_info_table():
    info_df = pd.read_csv(info_data, dtype='str')
    
    # removing all duplcates
    info_df = info_df.drop_duplicates(subset='Claim #', keep='first')

    return info_df

########## function to create the 'rejection_table_df'
def clean_rejection_table():
    rejection_table_df = pd.read_csv(
        rejection_data, 
        dtype={'Claim #': str},
        parse_dates=['Created'])

    # removing all duplcates
    rejection_table_df = rejection_table_df.drop_duplicates(subset='Claim #', keep='first')

    # storing all floored timestamps in a list
    floored_fta_rejections = []

    # iterating over the 'rejection_table_df' to 'floor' or zero out each timestamp
    for index, row in rejection_table_df.iterrows():

        # zeroing out the hours and minutes, appending value to 'floored' list
        fta_rejections = row['Created'].replace(hour=0, minute=0)
        floored_fta_rejections.append(fta_rejections)

    # adding the floored list to the 'rejection_table_df'
    rejection_table_df['Created'] = floored_fta_rejections

    ### Latest Rejection
    # 'idmax()' of the 'Created' column provides the most current rejection date
    reject_df = rejection_table_df.loc[rejection_table_df.groupby('Claim #')['Created'].idxmax()]
    reject_df = reject_df.rename(columns={"Created": "Most Recent Rejection"})

    ###  Multi-Rejection Counts
    rejection_count_df = (rejection_table_df.groupby("Claim #").count())
    rejection_count_df.reset_index(inplace=True)
    rejection_count_df = rejection_count_df.rename(columns={"Created": "Scope Rejections"})

    #merging 'reject_df' with the scope rejection count(s)
    scope_rejection_df = reject_df.merge(rejection_count_df, on='Claim #')

    return scope_rejection_df

########## function to create the updated 'oa_processed_df'
def clean_oa_processed_table():
    oa_processed_df = pd.read_csv(
        oa_processed_data, 
        dtype={'Job #': str}, 
        parse_dates=['Updated'], 
        usecols=['Job #', 'Updated'])

    # storing all floored timestamps in a list
    floored_oa_processed = []

    # iterating over the 'oa_processed_df' to 'floor' or zero out each timestamp
    for index, row in oa_processed_df.iterrows():
        adjusted_oa_processed = row['Updated'].replace(hour=0, minute=0)
        floored_oa_processed.append(adjusted_oa_processed)

    # adding the floored list to the 'oa_processed_df'
    oa_processed_df['Updated'] = floored_oa_processed
    oa_processed_df = oa_processed_df.rename(columns={'Updated':'Updated OA Processed'})

    return oa_processed_df

########## function to create the updated 'oa_invoiced_df'
def clean_oa_invoiced_table():
    oa_invoiced_df = pd.read_csv(
        oa_invoiced_data, 
        dtype={'Job #': str},
        parse_dates=['Updated'],
        usecols=['Job #', 'Updated'])

    # storing all floored timestamps in a list
    floored_oa_invoiced = []

    # # iterating over the 'oa_invoiced_df' to 'floor' or zero out each timestamp
    for index, row in oa_invoiced_df.iterrows():
        adjusted_oa_invoiced = row['Updated'].replace(hour=0, minute=0)
        floored_oa_invoiced.append(adjusted_oa_invoiced)

    # adding the floored list to the 'oa_invoiced_df'
    oa_invoiced_df['Updated'] = floored_oa_invoiced
    oa_invoiced_df = oa_invoiced_df.rename(columns={'Updated':'Updated OA Invoiced'})

    return oa_invoiced_df

########## function to create the 'approve_for_inspection_df'
def clean_approve_for_inspection_table():
    approve_for_inspection_df = pd.read_csv(
        approved_for_inspection_data, 
        dtype={'Job #': str}, 
        parse_dates=['Updated'], 
        usecols=['Job #','Updated'])

    # storing all floored timestamps in a list
    floored_approve_for_inspection = []

    # # iterating over the 'approve_for_inspection_df' to 'floor' or zero out each timestamp
    for index, row in approve_for_inspection_df.iterrows():
        adjusted_approve_for_inspection = row['Updated'].replace(hour=0, minute=0)
        floored_approve_for_inspection.append(adjusted_approve_for_inspection)

    # adding the floored list to the 'approve_for_inspection_df'
    approve_for_inspection_df['Updated'] = floored_approve_for_inspection
    approve_for_inspection_df = approve_for_inspection_df.rename(columns={"Updated": "Updated R4F"})

    return approve_for_inspection_df

########## function to create the 'change_order_df'
def clean_change_order_table():
    change_order_df = pd.read_csv(
        change_order_data, 
        dtype={'Job #': str}, 
        parse_dates=['Created'])

    # storing all floored timestamps in a list
    floored_change_order = []

    # iterating over the 'change_order_df' to 'floor' or zero out each timestamp
    for index, row in change_order_df.iterrows():

        # zeroing out the hours and minutes, appending value to 'floored' list
        gm_change_order = row['Created'].replace(hour=0, minute=0)
        floored_change_order.append(gm_change_order)

    # adding the floored list to the 'change_order_df'
    change_order_df['Created'] = floored_change_order

    # Change Order Date
    co_date_df = change_order_df.loc[change_order_df.groupby('Job #')['Created'].idxmax()]
    co_date_df = co_date_df.rename(columns={"Created": "GM Change Order Date"})

    # Change Order Count
    co_count_df = (change_order_df.groupby("Job #").count())
    co_count_df.reset_index(inplace=True)
    co_count_df = co_count_df.rename(columns={"Created": "Change Orders"})

    #merging 'co_date_df' with the change order count(s)
    change_order_df = co_date_df.merge(co_count_df, on='Job #')

    return change_order_df

########## function to create the 'labor_adjustment_df'
def clean_labor_adjustment_table():
    # GM Labor Adjustment Analysis
    labor_adjustment_df = pd.read_csv(
        labor_adjustment_data, 
        dtype={'Order ID': str}, 
        parse_dates=['Created'])

    # storing all floored timestamps in a list
    floored_labor_adjustment = []

    # iterating over the 'labor_adjustment_df' to 'floor' or zero out each timestamp
    for index, row in labor_adjustment_df.iterrows():

        # zeroing out the hours and minutes, appending value to 'floored' list
        gm_labor_adjustment = row['Created'].replace(hour=0, minute=0)
        floored_labor_adjustment.append(gm_labor_adjustment)

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

    # Labor Adjustment Date
    la_date_df = labor_adjustment_df.loc[labor_adjustment_df.groupby('Job #')['Created'].idxmax()]
    la_date_df = la_date_df.rename(columns={"Created": "GM Labor Adjustment Date"})

    # Labor Adjustment Count
    la_count_df = (labor_adjustment_df.groupby("Job #").count())
    la_count_df.reset_index(inplace=True)
    la_count_df = la_count_df.rename(columns={"Created": "Labor Adjustments"})

    #merging 'la_date_df' with the labor adjustment count(s)
    labor_adjustment_df = la_count_df.merge(la_date_df, on='Job #')

    return labor_adjustment_df

########## function to create the updated 'coc_collected_df'
def clean_coc_table():
    coc_collected_df = pd.read_csv(coc_collected_data, dtype={'Job #': str}, parse_dates=['Updated'], usecols=['Claim #', 'Job #', 'Updated'])

    # removing all duplcates
    coc_collected_df = coc_collected_df.drop_duplicates(subset='Claim #', keep='first')

    # storing all floored timestamps in a list
    floored_coc_collected = []

    # iterating over the 'labor_adjustment_df' to 'floor' or zero out each timestamp
    for index, row in coc_collected_df.iterrows():

        # zeroing out the hours and minutes, appending value to 'floored' list
        adjusted_coc_collected = row['Updated'].replace(hour=0, minute=0)
        floored_coc_collected.append(adjusted_coc_collected)

    # adding the floored list to the 'labor_adjustment_df'
    coc_collected_df['Updated'] = floored_coc_collected
    coc_collected_df = coc_collected_df.rename(columns={'Updated':'Updated COC Collected'})
    
    return coc_collected_df

############################## Parsing Functions ##############################

############### function for preparing the 'sales workflow' table ###############
def cleanup_workflow_dates():
    # storing the table function dfs as variables
    project_table = clean_sales_table()
    updated_coc_table = clean_coc_table()

    # merging the project and 'updated coc' data
    cleanup_data = project_table.merge(updated_coc_table, how='left', on=['Claim #','Job #'])

    # list to store the correct coc dates
    parsed_coc_dates = []

    for index, row in cleanup_data.iterrows():

        # if there is an 'updated coc collected' date...
        if row['Updated COC Collected'] == row['Updated COC Collected']:
            # use the updated coc date
            real_coc_date = row['Updated COC Collected']
            parsed_coc_dates.append(real_coc_date)

        else:
            # if not use the trackvia coc date
            real_coc_date = row['COC Rcvd Date [A]']
            parsed_coc_dates.append(real_coc_date)

    # save the new coc list as the official 'coc' list
    cleanup_data['COC Rcvd Date [A]'] = parsed_coc_dates

    # delete the 'updated' coc column
    del cleanup_data['Updated COC Collected']

    return cleanup_data

############### function for preparing the 'production workflow' table ###############
def cleanup_project_workflow_dates():
    # storing the table function dfs as variables
    project_table = clean_production_table()
    updated_oa_processed_table = clean_oa_processed_table()
    updated_oa_invoiced_table = clean_oa_invoiced_table()
    updated_approved_for_inspection_table = clean_approve_for_inspection_table()

    # merging the 'oa_processed', 'oa_invoiced', and 'approved_for_inspection' tables
    project_table = project_table.merge(
        updated_oa_processed_table, how='left', on='Job #').merge(
        updated_oa_invoiced_table, how='left', on='Job #').merge(
        updated_approved_for_inspection_table, how='left', on='Job #')

    # lists to store the correct production dates
    parsed_oa_processed_dates = []
    parsed_oa_invoiced_dates = []
    parsed_approved_for_inspection_dates = []

    for index, row in project_table.iterrows():

        # if there is an 'updated oa processed' date...
        if row['Updated OA Processed'] == row['Updated OA Processed']:
            # use the updated oa processed date from googlesheet
            real_oa_processed_date = row['Updated OA Processed']
            parsed_oa_processed_dates.append(real_oa_processed_date)

        else:
            # if not use the trackvia oa processed date
            real_oa_processed_date = row['OA Date']
            parsed_oa_processed_dates.append(real_oa_processed_date)


        # if there is an 'updated oa invoiced' date...
        if row['Updated OA Invoiced'] == row['Updated OA Invoiced']:
            # use the updated oa invoiced date from googlesheet
            real_oa_invoiced_date = row['Updated OA Invoiced']
            parsed_oa_invoiced_dates.append(real_oa_invoiced_date)

        else:
            # if not use the trackvia oa invoiced date
            real_oa_invoiced_date = row['Invoice Date']
            parsed_oa_invoiced_dates.append(real_oa_invoiced_date)


        # if there is an 'updated approved for inspection' date...
        if row['Updated R4F'] == row['Updated R4F']:
            # use the updated approved for inspection date from googlesheet
            real_approved_for_inspection_date = row['Updated R4F']
            parsed_approved_for_inspection_dates.append(real_approved_for_inspection_date)

        else:
            # if not use the trackvia oa invoiced date
            real_approved_for_inspection_date = row['R4F']
            parsed_approved_for_inspection_dates.append(real_approved_for_inspection_date)

    # save the new lists as on the production table before iterating workflow days
    project_table['OA Date'] = parsed_oa_processed_dates
    project_table['Invoice Date'] = parsed_oa_invoiced_dates
    project_table['R4F'] = parsed_approved_for_inspection_dates

    # deleting columns not needed in the production datestamp info
    del project_table['Updated OA Processed'], project_table['Updated OA Invoiced'], project_table['Updated R4F'],project_table['Supplier Name'], project_table['Building Department'], project_table['Permit Req?'], project_table['On Hold?'], project_table['Branch']
    

    return project_table

################################### Database Functions ###################################

# creating the datestamp database
def create_datestamp_database():
    # assigning the functions to variables to be able to merge them
    production_table = cleanup_project_workflow_dates()
    sales_table = cleanup_workflow_dates()
    reject_table = clean_rejection_table()
    change_order_table = clean_change_order_table()
    labor_adjustment_table = clean_labor_adjustment_table()

    del reject_table['Scope Rejections']
    del labor_adjustment_table['Labor Adjustments']
    del change_order_table['Change Orders']

    datestamped_workflow_table = sales_table.merge(
        production_table, how='left', on=['Claim #', 'Job #']).merge(
        reject_table, how='left', on='Claim #').merge(
        change_order_table, how='left', on='Job #').merge(
        labor_adjustment_table, how='left', on='Job #')

    # removing all duplcates
    datestamped_workflow_table = datestamped_workflow_table.drop_duplicates(subset='Claim #', keep='first')

    # renaming the combined data
    datestamped_workflow_table = datestamped_workflow_table.rename(columns={
        'Claim # Date': 'Rep Agreement Signed Date',
        'FTA Scope. Req Date': 'Rep Claim Collected Date',
        'Most Recent Rejection': 'FTA Scope Rejected Date',
        'Submit for Estimate Date': 'FTA Scope Completed Date',
        '[B] Created Estimate Date': 'BC Estimate Completed Date',
        '[OB] Created Scope Calc': 'OB Scope Completed Date',
        'Job Submittal Date': 'Sup Job Submitted Date',
        '[B] - Date Approved by BC': 'BC Approved for Production Date',
        '[OB] Completed': 'OB Order Built Date',
        'Permit Applied [A]': 'PA Permit Applied Date',
        'Order Date': 'GM Order Processed Date',
        'Permit Received': 'PA Permit Processed Date',
        'OA Date': 'PA OA Processed Date',
        'Invoice Date': 'PA OA Invoiced Date',
        'Ntfd H.O. Dlvry': 'PA Notify of Delivery Date',
        'Dlvry Start': 'Delivery Date',
        'Ntfd H.O. Start': 'PA Notify of Start Date',
        'Roof Start': 'Roof Start Date',
        'Roof Complete Date': 'Roof End Date',
        'R4F': 'GM Approved for Inspection Date',
        'Requested Final Insp': 'RA Inspection Requested Date',
        'Final Inspection Date': 'RA Inspection Processed Date',
        'COC Rcvd Date [A]': 'Rep COC Collected Date',
        'Job Docs Scanned': 'SA Job Docs Uploaded Date',
        '[B] Sent Invoice Packet to Ins Co': 'BC Project Invoiced Date',
        '[B] Settled with Insurance': 'BC Project Closed Date'})

    # Organizing combined Data to follow Workflow
    all_project_df = datestamped_workflow_table[[
        'Claim #',
        'Job #',
        'Branch',
        'Claim Status',
        'Rep Agreement Signed Date',
        'Rep Claim Collected Date',
        'FTA Scope Completed Date',
        'FTA Scope Rejected Date',
        'BC Estimate Completed Date',
        'OB Scope Completed Date',
        'Sup Job Submitted Date',
        'BC Approved for Production Date',
        'OB Order Built Date',
        'GM Order Processed Date',
        'PA Permit Applied Date',
        'PA Permit Processed Date',
        'PA OA Processed Date',
        'PA OA Invoiced Date',
        'PA Notify of Delivery Date',
        'PA Notify of Start Date',
        'Delivery Date',
        'Roof Start Date',
        'Roof End Date',
        'GM Approved for Inspection Date',
        'GM Change Order Date',
        'GM Labor Adjustment Date',
        'RA Inspection Requested Date',
        'RA Inspection Processed Date',
        'Rep COC Collected Date',
        'SA Job Docs Uploaded Date',
        'BC Project Invoiced Date',
        'BC Project Closed Date']]

    return all_project_df

# creating the project info database
def create_info_database():

    # assigning the functions to variables to be able to merge them
    info_table = clean_info_table()
    production_table = clean_production_table()
    reject_table = clean_rejection_table()
    change_order_table = clean_change_order_table()
    labor_adjustment_table = clean_labor_adjustment_table()

    # creating smaller sets of data to merge to the 'info_table'
    production_info = production_table[['Job #', 'Supplier Name', 'Building Department', 'Permit Req?']]
    rejection_info = reject_table[['Claim #', 'Scope Rejections']]
    change_order_info = change_order_table[['Job #', 'Change Orders']]
    labor_adjustment_info = labor_adjustment_table[['Job #', 'Labor Adjustments']]

    # merging the relevant data to the 'info_table'
    info_table = info_table.merge(
        production_info, how='left', on='Job #').merge(
        rejection_info, how='left', on='Claim #').merge(
        change_order_info, how='left', on='Job #').merge(
        labor_adjustment_info, how='left', on='Job #')

    # Organizing Project Info Data
    info_table = info_table[[
        'Claim #',
        'Job #',
        'Branch',
        'City',
        'Building Department',
        'Permit Req?',
        'Supplier Name',
        'Crew',
        'Insurance Company',
        'Scope Rejections',
        'Change Orders',
        'Labor Adjustments']]

    return info_table

# creating the workflow days database
def create_workflow_database():

    # will store this project info
    claim_num = []
    job_num = []
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
    rep_coc_collected_diff = []
    sa_docs_uploaded_diff = []
    bc_project_invoiced_diff = []
    bc_project_closed_diff = []

    # this data applies to leadtimes, not workflow
    pa_permit_applied_diff = []
    pa_permit_processed_diff = []
    pa_notify_delivery_diff = []
    pa_notify_start_diff = []
    ra_request_inspection_diff = []
    ra_inspection_processed_diff = []

    # saving the 'datestamp database' to a variable to iterate over
    all_project_df = create_datestamp_database()

    # iterating over the df to create 'date diff' variables
    for index, row in all_project_df.iterrows():

        # creating 'date_diff' variables for each step in the workflow
        rep_claim_date_diff = float((row['Rep Claim Collected Date'] - row['Rep Agreement Signed Date']).days)

        # if the bc estimate was created prior to July 16th...
        if row['BC Estimate Completed Date'] <= datetime(2019, 7, 15):

            # and if the record did NOT had the FTA Scope Rejected...
            if row['FTA Scope Rejected Date'] != row['FTA Scope Rejected Date']:

                # then compare the date diffs using the 'FTA Scope Completed' date field
                fta_date_diff = (row['FTA Scope Completed Date'] - row['Rep Claim Collected Date']).days
                ob_scope_date_diff = (row['OB Scope Completed Date'] - row['FTA Scope Completed Date']).days
                bc_estimate_date_diff = (row['BC Estimate Completed Date'] - row['OB Scope Completed Date']).days
                sup_pfynr_date_diff = (row['Sup Job Submitted Date'] - row['BC Estimate Completed Date']).days

            # but if the record has had the FTA Scope Rejected...
            else:
                # then compare the 'FTA Scope rejected' date field
                fta_date_diff = (row['FTA Scope Rejected Date'] - row['Rep Claim Collected Date']).days
                ob_scope_date_diff = (row['OB Scope Completed Date'] - row['FTA Scope Rejected Date']).days
                bc_estimate_date_diff = (row['BC Estimate Completed Date'] - row['OB Scope Completed Date']).days
                sup_pfynr_date_diff = (row['Sup Job Submitted Date'] - row['OB Scope Completed Date']).days

        # if the bc estimate was addressed during the 'blip' on 7/16-7/17...
        elif row['BC Estimate Completed Date'] == datetime(2019, 7, 16) or row['BC Estimate Completed Date'] == datetime(2019, 7, 17):

            # and if the record did NOT had the FTA Scope Rejected...
            if row['FTA Scope Rejected Date'] != row['FTA Scope Rejected Date']:
                # then compare the 'bc estimate' to the 'blip' date, and the 'ob scope' date to the new 'bc date'
                fta_date_diff = (row['FTA Scope Completed Date'] - row['Rep Claim Collected Date']).days
                bc_estimate_date_diff = (row['BC Estimate Completed Date'] - datetime(2019, 7, 16)).days
                ob_scope_date_diff = (row['OB Scope Completed Date'] - row['BC Estimate Completed Date']).days
                sup_pfynr_date_diff = (row['Sup Job Submitted Date'] - row['OB Scope Completed Date']).days

            # but if the record has had the FTA Scope rejected...
            else:
                # then compare the 'fta scope rejected' date field
                fta_date_diff = (row['FTA Scope Rejected Date'] - row['Rep Claim Collected Date']).days
                bc_estimate_date_diff = (row['BC Estimate Completed Date'] - datetime(2019, 7, 16)).days
                ob_scope_date_diff = (row['OB Scope Completed Date'] - row['FTA Scope Rejected Date']).days
                sup_pfynr_date_diff = (row['Sup Job Submitted Date'] - row['OB Scope Completed Date']).days

        # if the bc estimate was created after the 'blip'...
        else:
            # and if the record did NOT had the FTA Scope Rejected...
            if row['FTA Scope Rejected Date'] != row['FTA Scope Rejected Date']:
                # then compare the 'fta scope completed' date field
                fta_date_diff = (row['FTA Scope Completed Date'] - row['Rep Claim Collected Date']).days
                bc_estimate_date_diff = (row['BC Estimate Completed Date'] - row['FTA Scope Completed Date']).days
                ob_scope_date_diff = (row['OB Scope Completed Date'] - row['BC Estimate Completed Date']).days
                sup_pfynr_date_diff = (row['Sup Job Submitted Date'] - row['OB Scope Completed Date']).days

            else:
                # then compare the 'fta scope rejected' date field
                fta_date_diff = (row['FTA Scope Rejected Date'] - row['Rep Claim Collected Date']).days
                bc_estimate_date_diff = (row['BC Estimate Completed Date'] - row['FTA Scope Completed Date']).days
                ob_scope_date_diff = (row['OB Scope Completed Date'] - row['FTA Scope Rejected Date']).days
                sup_pfynr_date_diff = (row['Sup Job Submitted Date'] - row['OB Scope Completed Date']).days

        # due to manual OA 'Processed' and 'Invoice' date fields, PAs have been recording false dates...
        if row['PA OA Invoiced Date'] < row['PA OA Processed Date']:

            # ...which provide no advantage to the project.
            row['PA OA Invoiced Date'] = row['PA OA Processed Date'] + timedelta(days=1)

        # OAs can't be invoiced before they have been processed (approved)
        pa_invoice_date_diff = (row['PA OA Invoiced Date'] - row['PA OA Processed Date']).days

        # due to manual 'Approved for inspection' (R4F) date field, GMs have been recording false dates...
        if row['GM Approved for Inspection Date'] < row['Roof End Date']:

            # ...which provide no advantage to the project; those will be reset to the day after the build.
            row['GM Approved for Inspection Date'] = row['Roof End Date'] + timedelta(days=1)

        # roofs can't be approved for inspection prior to the roof being built
        gm_approval_date_diff = (row['GM Approved for Inspection Date'] - row['Roof End Date']).days

        # due to manual 'COC Collected' (COC Rcvd [A]) date field, SAs have been recording false dates...
        if row['Rep COC Collected Date'] < row['Roof End Date']:

            # ...these will be reset for after the build.
            row['Rep COC Collected Date'] = row['Roof End Date'] + timedelta(days=1)

        # coc's can't be collected until after the roof is built, not before
        rep_coc_collected_date_diff = (row['Rep COC Collected Date'] - row['Roof End Date']).days

        # these dates do not have any special circumstances and can be directly compared
        bc_approval_date_diff = (row['BC Approved for Production Date'] - row['Sup Job Submitted Date']).days
        ob_orderbuild_date_diff = (row['OB Order Built Date'] - row['BC Approved for Production Date']).days
        gm_create_order_date_diff = (row['GM Order Processed Date'] - row['OB Order Built Date']).days
        ra_requested_inspection_date_diff = (row['RA Inspection Requested Date'] - row['GM Approved for Inspection Date']).days
        ra_inspection_processed_date_diff = (row['RA Inspection Processed Date'] - row['RA Inspection Requested Date']).days
        sa_docs_uploaded_date_diff = (row['SA Job Docs Uploaded Date'] - row['Rep COC Collected Date']).days
        bc_project_invoiced_date_diff = (row['BC Project Invoiced Date'] - row['SA Job Docs Uploaded Date']).days
        bc_project_closed_date_diff = (row['BC Project Closed Date'] - row['BC Project Invoiced Date']).days

        # these dates are manual, outliers are due to incorrect / false data entry
        pa_oa_processed_date_diff = (row['PA OA Processed Date'] - row['GM Order Processed Date']).days

        # these provide the lead times of tasks not directly impacting the workflow.
        pa_permit_applied_date_diff = (row['PA Permit Applied Date'] - row['BC Approved for Production Date']).days
        pa_permit_processed_date_diff = (row['PA Permit Processed Date'] - row['PA Permit Applied Date']).days
        pa_notify_delivery_date_diff = (row['Delivery Date'] - row['PA Notify of Delivery Date']).days
        pa_notify_start_date_diff = (row['Roof Start Date'] - row['PA Notify of Start Date']).days

    ####################################################################################################
        # appending 'date diff' values to lists to create each df column

        # project info
        claim_num.append(row["Claim #"])
        branch_list.append(row['Branch'])
        claim_status_list.append(row['Claim Status'])
        
        # workflow related info
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
        ra_inspection_processed_diff.append(ra_inspection_processed_date_diff)

    ####################################################################################################

    # Creating 'Workflow Days' df
    days_df = pd.DataFrame({

        # project info
        "Claim #": claim_num,

        # workflow
        "Rep Collected Claim": rep_claim_diff,
        "FTA Completed Scope": fta_scope_diff,
        "BC Completed Estimate": bc_estimate_diff,
        "OB Completed Scope": ob_scope_diff,
        "Sup Submitted Job": sup_pfynr_diff,
        "BC Approved Job": bc_approval_diff,
        "OB Built Order": ob_order_build_diff,
        "GM Processed Order": gm_create_order_diff,
        "PA Applied for Permit": pa_permit_applied_diff,
        "PA Processed Permit": pa_permit_processed_diff,
        "PA Processed OA": pa_oa_processed_diff,
        "PA Invoiced OA": pa_invoice_diff,
        "PA Notified of Delivery": pa_notify_delivery_diff,
        "PA Notified of Start": pa_notify_start_diff,
        'GM Approved for Inspection': gm_approval_diff,
        'RA Requested Inspection': ra_request_inspection_diff,
        'RA Processed Inspection': ra_inspection_processed_diff,
        'Rep Collected COC': rep_coc_collected_diff,
        'SA Uploaded Docs': sa_docs_uploaded_diff,
        'BC Invoiced Project': bc_project_invoiced_diff,
        'BC Closed Project': bc_project_closed_diff
    })

    # creating a list of the columns to use to calculate the 'total days'
    pipeline_list = [
        'Rep Collected Claim', 'FTA Completed Scope', 'BC Completed Estimate', 'OB Completed Scope',
        'Sup Submitted Job', 'BC Approved Job', 'OB Built Order', 'GM Processed Order',
        'PA Processed OA', 'PA Invoiced OA', 'GM Approved for Inspection', 'Rep Collected COC',
        'SA Uploaded Docs', 'BC Invoiced Project', 'BC Closed Project']

    # creating a column holding the running tally across a row (project)
    days_df['Days in Pipeline'] = days_df[pipeline_list].sum(axis=1)

    return days_df

def reporting_tool():
    
    print(f"---------------------------------------------")
    print(f"------------- TRACKVIA DATASETS -------------")
    print(f"---------------------------------------------")
    
    # these are data importing/creating reports 
    info_data = clean_info_table()
    print(f"Project Info Table: {len(info_data)}")
    
    sales_data = clean_sales_table()
    print(f"Project Sales Table: {len(sales_data)}")
    
    production_data = clean_production_table()
    print(f"Project Production Table: {len(production_data)}")
    
    rejection_data = clean_rejection_table()
    print(f"FTA Rejection Table: {len(rejection_data)}")
    
    oa_processed_data = clean_oa_processed_table()
    print(f"OA Processed Table: {len(oa_processed_data)}")
    
    oa_invoiced_data = clean_oa_invoiced_table()
    print(f"OA Invoiced Table: {len(oa_invoiced_data)}")
    
    gm_approved_data = clean_approve_for_inspection_table()
    print(f"GM Approved for Inspection Table: {len(gm_approved_data)}")
    
    change_order_data = clean_change_order_table()
    print(f"GM Change Order Table: {len(change_order_data)}")
    
    labor_adjustment_data = clean_labor_adjustment_table()
    print(f"GM Labor Adjustment Table: {len(labor_adjustment_data)}")
    
    coc_uploaded_data = clean_coc_table()
    print(f"SA COC Uploaded Table: {len(coc_uploaded_data)}")
    
    print(f"-------------------------------------------")
    print(f"-------- CLEANING/PARSING DATASETS --------")
    print(f"-------------------------------------------")
    
    # these are the parsing function reports 
    sales_workflow_data = cleanup_workflow_dates()
    print(f"Cleaning Sales Workflow Dates: {len(sales_workflow_data)}")
    
    production_workflow_data = cleanup_project_workflow_dates()
    print(f"Cleaning Production Workflow Dates: {len(production_workflow_data)}")
    
    print(f"------------------------------------------")
    print(f"-------- CREATING DATABASE TABLES --------")
    print(f"------------------------------------------")
    
    # these are the database table function reports 
    info_table = create_info_database()
    print(f"Creating Info Table: {len(info_table)}")
    
    datestamp_table = create_datestamp_database()
    print(f"Creating Datestamp Table: {len(datestamp_table)}")
    
    workflow_days_table = create_workflow_database()
    print(f"Creating Workflow Days Table: {len(workflow_days_table)}")
    print(f"------------------------------------------")
    return print(f'Report Complete')

reporting_tool()

print(f"------------------------------------------")
############### PROJECT TABLE DATA ####################
project_table = create_datestamp_database()

############### PROJECT INFORMATION TABLE DATA ###############
info_table = create_info_database()

# due to manual 'city' field, city is incorrectly spelled or off on capitalization
info_table['City'] = info_table['City'].str.upper()

#################### WORKFLOW TABLE DATA ####################
workflow_table = create_workflow_database()

######################### EAGLEVIEW DATA #########################
eagleview_table = pd.read_csv(eagleview_table_data)

# converting the squares measurement to roofing SQs
eagleview_table['Square Feet'] = eagleview_table['Square Feet'] / 100

# removing all duplcates
eagleview_table = eagleview_table.drop_duplicates(subset='Claim #', keep='first')

# #################################################### PIPELINE DICTIONARY ##################################################
pipeline_dict = {  
    'Rep Claims': {'start':'Rep Agreement Signed Date', 'end':'Rep Claim Collected Date', 'other':'none',
                   'project_table':['Claim #', 'Job #', 'Branch', 'Claim Status', 'Rep Agreement Signed Date', 'Rep Claim Collected Date'],
                   'workflow_table':['Claim #','Rep Collected Claim'],
                   'info_table':['Claim #','City'],
                   'eagleview_table':['Claim #', 'Square Feet'],
                   'clean_table':['Claim #',
                                  'Job #',
                                  'City',
                                  'Rep Agreement Signed Date',
                                  'Rep Claim Collected Date',
                                  'Rep Collected Claim',
                                  'Square Feet',
                                  'Branch',
                                  'Claim Status',]},
    'FTA Scopes': {'start':'Rep Claim Collected Date', 'end':'FTA Scope Completed Date', 'other':'none',
                   'project_table':['Claim #','Job #','Branch','Claim Status','Rep Claim Collected Date','FTA Scope Completed Date','FTA Scope Rejected Date',],
                   'workflow_table':['Claim #','FTA Completed Scope','Days in Pipeline',],
                   'info_table':['Claim #','City','Building Department','Scope Rejections',],
                   'eagleview_table':['Claim #','Square Feet',],
                   'clean_table':['Claim #',
                                  'Job #',
                                  'City',
                                  'Rep Claim Collected Date',
                                  'FTA Scope Completed Date',
                                  'FTA Scope Rejected Date',
                                  'FTA Completed Scope',
                                  'Days in Pipeline',
                                  'Scope Rejections',
                                  'Building Department',
                                  'Square Feet',
                                  'Branch',
                                  'Claim Status',]}, 
    'BC Estimates': {'start':'FTA Scope Completed Date', 'end':'BC Estimate Completed Date', 'other':'none',
                   'project_table':['Claim #','Job #','Branch','Claim Status','FTA Scope Completed Date','BC Estimate Completed Date',],
                   'workflow_table':['Claim #','BC Completed Estimate','Days in Pipeline',],
                   'info_table':['Claim #','City','Building Department',],
                   'eagleview_table':['Claim #','Square Feet',],
                   'clean_table':['Claim #',
                                  'Job #', 
                                  'City', 
                                  'FTA Scope Completed Date',
                                  'BC Estimate Completed Date',
                                  'BC Completed Estimate', 
                                  'Days in Pipeline',
                                  'Building Department',
                                  'Square Feet',
                                  'Branch', 
                                  'Claim Status',]},
    'OB Scopes': {'start':'BC Estimate Completed Date', 'end':'OB Scope Completed Date', 'other':'none',
                   'project_table':['Claim #','Job #','Branch','Claim Status','BC Estimate Completed Date','OB Scope Completed Date',],
                   'workflow_table':['Claim #','OB Completed Scope','Days in Pipeline',],
                   'info_table':['Claim #','City','Building Department','Scope Rejections',],
                   'eagleview_table':['Claim #','Square Feet',],
                   'clean_table':['Claim #',
                                  'Job #',
                                  'City',
                                  'BC Estimate Completed Date',
                                  'OB Scope Completed Date',
                                  'OB Completed Scope', 
                                  'Days in Pipeline',
                                  'Scope Rejections',
                                  'Building Department',
                                  'Square Feet',
                                  'Branch',
                                  'Claim Status',]},
    'Sup Submittals': {'start':'OB Scope Completed Date', 'end':'Sup Job Submitted Date', 'other':'Job #',
                   'project_table':['Claim #','Job #','Branch','Claim Status','OB Scope Completed Date','Sup Job Submitted Date',],
                   'workflow_table':['Claim #','Sup Submitted Job','Days in Pipeline',],
                   'info_table':['Claim #','City','Insurance Company',],
                   'eagleview_table':['Claim #','Square Feet',],
                   'clean_table':['Claim #',
                                  'Job #',
                                  'City',
                                  'OB Scope Completed Date',
                                  'Sup Job Submitted Date',
                                  'Sup Submitted Job',
                                  'Days in Pipeline',
                                  'Insurance Company', 
                                  'Square Feet',
                                  'Branch', 
                                  'Claim Status',]},
    'BC Approvals': {'start':'Sup Job Submitted Date', 'end':'BC Approved for Production Date','other':'none',
                   'project_table':['Claim #','Job #','Branch','Claim Status','Sup Job Submitted Date','BC Approved for Production Date',],
                   'workflow_table':['Claim #','BC Approved Job','Days in Pipeline',],
                   'info_table':['Claim #','City','Building Department','Insurance Company',],
                   'eagleview_table':['Claim #','Square Feet',],
                   'clean_table':['Claim #',
                                  'Job #',
                                  'City',
                                  'Sup Job Submitted Date',
                                  'BC Approved for Production Date',
                                  'BC Approved Job', 
                                  'Days in Pipeline',
                                  'Insurance Company',
                                  'Building Department',
                                  'Square Feet',
                                  'Branch', 
                                  'Claim Status',]},    
    'OB Orders Built': {'start':'BC Approved for Production Date', 'end':'OB Order Built Date','other':'none',
                   'project_table':['Claim #','Job #','Branch','Claim Status','BC Approved for Production Date','OB Order Built Date',],
                   'workflow_table':['Claim #','OB Built Order','Days in Pipeline',],
                   'info_table':['Claim #','City','Building Department','Scope Rejections','Change Orders',],
                   'eagleview_table':['Claim #','Square Feet',],
                   'clean_table':['Claim #',
                                  'Job #',
                                  'City',
                                  'BC Approved for Production Date',
                                  'OB Order Built Date',
                                  'OB Built Order',
                                  'Days in Pipeline',
                                  'Scope Rejections',
                                  'Change Orders', 
                                  'Building Department',
                                  'Square Feet',
                                  'Branch', 
                                  'Claim Status',]},
    'GM Orders Processed': {'start':'OB Order Built Date', 'end':'GM Order Processed Date','other':'none',
                   'project_table':['Claim #','Job #','Branch','Claim Status','OB Order Built Date','GM Order Processed Date',],
                   'workflow_table':['Claim #','GM Processed Order','Days in Pipeline',],
                   'info_table':['Claim #','City','Building Department','Permit Req?','Change Orders','Labor Adjustments',],
                   'eagleview_table':['Claim #','Square Feet',],
                   'clean_table':['Claim #',
                                  'Job #',
                                  'City',
                                  'OB Order Built Date',
                                  'GM Order Processed Date',
                                  'GM Processed Order',
                                  'Days in Pipeline', 
                                  'Change Orders',
                                  'Labor Adjustments',
                                  'Building Department',
                                  'Permit Req?',
                                  'Square Feet',
                                  'Branch', 
                                  'Claim Status']},
    'PA Permits Applied': {'start':'GM Order Processed Date', 'end':'PA Permit Applied Date','other':'Permit Req?',
                   'project_table':['Claim #','Job #','Branch','GM Order Processed Date','PA Permit Applied Date',],
                   'workflow_table':['Claim #','PA Applied for Permit','Days in Pipeline',],
                   'info_table':['Claim #','City','Building Department','Permit Req?',],
                   'eagleview_table':['Claim #','Square Feet',],
                   'clean_table':['Claim #', 
                                  'Job #',
                                  'City',
                                  'GM Order Processed Date',
                                  'PA Permit Applied Date',
                                  'PA Applied for Permit',
                                  'Days in Pipeline',
                                  'Building Department',
                                  'Permit Req?', 
                                  'Square Feet',
                                  'Branch']},
    'PA Permits Processed':{'start':'PA Permit Applied Date', 'end':'PA Permit Processed Date','other':'Permit Req?',
                   'project_table':['Claim #','Job #','Branch','PA Permit Applied Date','PA Permit Processed Date',],
                   'workflow_table':['Claim #','PA Processed Permit','Days in Pipeline',],
                   'info_table':['Claim #','City','Building Department','Permit Req?',],
                   'eagleview_table':['Claim #','Square Feet',],
                   'clean_table':['Claim #',
                                  'Job #',
                                  'City',
                                  'PA Permit Applied Date',
                                  'PA Permit Processed Date',
                                  'PA Processed Permit', 
                                  'Days in Pipeline',
                                  'Building Department',
                                  'Permit Req?',
                                  'Square Feet',
                                  'Branch',]},
    'PA OA Processed':{'start':'GM Order Processed Date', 'end':'PA OA Processed Date','other':'none',
                   'project_table':['Claim #','Job #','Branch','GM Order Processed Date','PA OA Processed Date',],
                   'workflow_table':['Claim #','PA Processed OA','Days in Pipeline',],
                   'info_table':['Claim #','City','Supplier Name','Change Orders',],
                   'eagleview_table':['Claim #','Square Feet',],
                   'clean_table':['Claim #',
                                  'Job #',
                                  'City',
                                  'GM Order Processed Date',
                                  'PA OA Processed Date',
                                  'PA Processed OA', 
                                  'Days in Pipeline',
                                  'Supplier Name',
                                  'Change Orders', 
                                  'Square Feet',
                                  'Branch',]},
    'PA OA Invoiced':{'start':'PA OA Processed Date', 'end':'PA OA Invoiced Date','other':'none',
                   'project_table':['Claim #','Job #','Branch','PA OA Processed Date','PA OA Invoiced Date',],
                   'workflow_table':['Claim #','PA Invoiced OA','Days in Pipeline',],
                   'info_table':['Claim #','City','Supplier Name','Change Orders',],
                   'eagleview_table':['Claim #','Square Feet',],
                   'clean_table':['Claim #',
                                  'Job #',
                                  'City',
                                  'PA OA Processed Date',
                                  'PA OA Invoiced Date',
                                  'PA Invoiced OA', 
                                  'Days in Pipeline', 
                                  'Supplier Name',
                                  'Change Orders',
                                  'Square Feet',
                                  'Branch',]},
    'PA Notify of Delivery':{'start':'Delivery Date', 'end':'PA Notify of Delivery Date','other':'none',
                   'project_table':['Claim #','Job #','Branch','PA Notify of Delivery Date','Delivery Date',],
                   'workflow_table':['Claim #','PA Notified of Delivery','Days in Pipeline',],
                   'info_table':['Claim #','City','Supplier Name','Change Orders',],
                   'eagleview_table':['Claim #','Square Feet',],
                   'clean_table':['Claim #',
                                  'Job #',
                                  'City',
                                  'PA Notify of Delivery Date',
                                  'Delivery Date',
                                  'PA Notified of Delivery',
                                  'Days in Pipeline',
                                  'Supplier Name',
                                  'Change Orders',
                                  'Square Feet',
                                  'Branch',]},
    'PA Notify of Start':{'start':'Roof Start Date', 'end':'PA Notify of Start Date','other':'none',
                   'project_table':['Claim #','Job #','Branch','PA Notify of Start Date','Roof Start Date',],
                   'workflow_table':['Claim #','PA Notified of Start','Days in Pipeline',],
                   'info_table':['Claim #','City','Crew','Labor Adjustments',],
                   'eagleview_table':['Claim #','Square Feet',],
                   'clean_table':['Claim #', 
                                  'Job #', 
                                  'City',
                                  'PA Notify of Start Date',
                                  'Roof Start Date',
                                  'PA Notified of Start',
                                  'Days in Pipeline',
                                  'Crew',
                                  'Labor Adjustments', 
                                  'Square Feet',
                                  'Branch',]}, 
    'GM Approved for Inspection':{'start':'Roof End Date', 'end':'GM Approved for Inspection Date','other':'Permit Req?',
                   'project_table':['Claim #','Job #','Branch','Roof End Date','GM Approved for Inspection Date',],
                   'workflow_table':['Claim #','GM Approved for Inspection','Days in Pipeline',],
                   'info_table':['Claim #','City','Building Department','Scope Rejections','Change Orders','Labor Adjustments','Permit Req?'],
                   'eagleview_table':['Claim #','Square Feet',],
                   'clean_table':['Claim #', 
                                  'Job #', 
                                  'City', 
                                  'Roof End Date',
                                  'GM Approved for Inspection Date',
                                  'GM Approved for Inspection',
                                  'Days in Pipeline',
                                  'Building Department', 
                                  'Scope Rejections',
                                  'Change Orders', 
                                  'Labor Adjustments',
                                  'Square Feet',
                                  'Branch',
                                  'Permit Req?',]},
    'RA Inspection Requested':{'start':'GM Approved for Inspection Date', 'end':'RA Inspection Requested Date','other':'Permit Req?',
                   'project_table':['Claim #','Job #','Branch','GM Approved for Inspection Date','RA Inspection Requested Date',],
                   'workflow_table':['Claim #','RA Requested Inspection','Days in Pipeline',],
                   'info_table':['Claim #','City','Building Department','Permit Req?',],
                   'eagleview_table':['Claim #','Square Feet',],
                   'clean_table':['Claim #', 
                                  'Job #',
                                  'City',
                                  'GM Approved for Inspection Date',
                                  'RA Inspection Requested Date',
                                  'RA Requested Inspection',
                                  'Days in Pipeline', 
                                  'Building Department',
                                  'Square Feet', 
                                  'Branch',
                                  'Permit Req?',]},
    'RA Inspection Processed':{'start':'RA Inspection Requested Date', 'end':'RA Inspection Processed Date','other':'Permit Req?',
                   'project_table':['Claim #','Job #','Branch','RA Inspection Requested Date','RA Inspection Processed Date',],
                   'workflow_table':['Claim #','RA Processed Inspection','Days in Pipeline',],
                   'info_table':['Claim #','City','Building Department','Permit Req?',],
                   'eagleview_table':['Claim #','Square Feet',],
                   'clean_table':['Claim #',
                                  'Job #',
                                  'City',
                                  'RA Inspection Requested Date',
                                  'RA Inspection Processed Date', 
                                  'RA Processed Inspection',
                                  'Days in Pipeline',  
                                  'Building Department',
                                  'Square Feet', 
                                  'Branch', 
                                  'Permit Req?',]},
    'Rep COC Collected':{'start':'Roof End Date', 'end':'Rep COC Collected Date','other':'none',
                   'project_table':['Claim #','Job #','Branch','Roof End Date','Rep COC Collected Date',],
                   'workflow_table':['Claim #','Rep Collected COC','Days in Pipeline',],
                   'info_table':['Claim #','City','Insurance Company','Change Orders','Labor Adjustments',],
                   'eagleview_table':['Claim #','Square Feet',],
                   'clean_table':['Claim #',
                                  'Job #',
                                  'City',
                                  'Roof End Date',
                                  'Rep COC Collected Date',
                                  'Rep Collected COC',
                                  'Days in Pipeline',
                                  'Insurance Company',
                                  'Change Orders',
                                  'Labor Adjustments', 
                                  'Square Feet',
                                  'Branch',]},
    'SA Job Docs Uploaded':{'start':'Rep COC Collected Date', 'end':'SA Job Docs Uploaded Date','other':'none',
                   'project_table':['Claim #','Job #','Branch','Rep COC Collected Date','SA Job Docs Uploaded Date',],
                   'workflow_table':['Claim #','SA Uploaded Docs','Days in Pipeline',],
                   'info_table':['Claim #','City','Insurance Company','Change Orders','Labor Adjustments',],
                   'eagleview_table':['Claim #','Square Feet',],
                   'clean_table':['Claim #', 
                                  'Job #',
                                  'City', 
                                  'Rep COC Collected Date',
                                  'SA Job Docs Uploaded Date',
                                  'SA Uploaded Docs', 
                                  'Days in Pipeline',
                                  'Insurance Company', 
                                  'Change Orders', 
                                  'Labor Adjustments',
                                  'Square Feet',
                                  'Branch',]},
    'BC Project Invoiced':{'start':'SA Job Docs Uploaded Date', 'end':'BC Project Invoiced Date','other':'none',
                   'project_table':['Claim #','Job #','Branch','SA Job Docs Uploaded Date','BC Project Invoiced Date',],
                   'workflow_table':['Claim #','BC Invoiced Project','Days in Pipeline',],
                   'info_table':['Claim #','City','Building Department','Insurance Company',],
                   'eagleview_table':['Claim #','Square Feet',],
                   'clean_table':['Claim #',
                                  'Job #',
                                  'City', 
                                  'SA Job Docs Uploaded Date',
                                  'BC Project Invoiced Date', 
                                  'BC Invoiced Project',
                                  'Days in Pipeline',
                                  'Building Department',
                                  'Insurance Company', 
                                  'Square Feet',
                                  'Branch',]},
    'BC Project Closed':{'start':'BC Project Invoiced Date', 'end':'BC Project Closed Date','other':'none',
                   'project_table':['Claim #','Job #','Branch','BC Project Invoiced Date','BC Project Closed Date',],
                   'workflow_table':['Claim #','BC Closed Project','Days in Pipeline',],
                   'info_table':['Claim #','City','Building Department','Insurance Company',],
                   'eagleview_table':['Claim #','Square Feet',],
                   'clean_table':['Claim #',
                                  'Job #',
                                  'City',
                                  'BC Project Invoiced Date',
                                  'BC Project Closed Date',
                                  'BC Closed Project', 
                                  'Days in Pipeline', 
                                  'Building Department', 
                                  'Insurance Company', 
                                  'Square Feet', 
                                  'Branch',]},
}

########################### WORKFLOW CLASS #########################
class Branch_Workflow:
    
    def __init__(self, branch, pipeline):
        self.branch = branch
        self.pipeline = pipeline

    # function that creates the specific branch/teammate workflow
    def create_teammate_workflow(self):
        
        # assigning the 'pipeline_dict' for the 'pipeline' to a variable to get dict values
        pipeline_info = pipeline_dict.get(self.pipeline)
        
        # prodviding only revelant information from project table
        project_list = pipeline_info['project_table']
        parsed_project_table = project_table[project_list]
        
        # providing only revelant information from workflow table
        workflow_list = pipeline_info['workflow_table']
        parsed_workflow_table = workflow_table[workflow_list]

        # providing only revelant information from info table
        info_list = pipeline_info['info_table']
        parsed_info_table = info_table[info_list]

        # providing only revelant information from eagleview table
        eagleview_list = pipeline_info['eagleview_table']        
        parsed_eagleview_table = eagleview_table[eagleview_list]

        # merging all parsed tables together
        merged_table = parsed_project_table.merge(
            parsed_workflow_table, on='Claim #', how='left').merge(
            parsed_info_table, on='Claim #', how='left').merge(
            parsed_eagleview_table, on='Claim #', how='left')
        
        # resorting each table created based on it's 'master_list'
        master_list = pipeline_info['clean_table']
        merged_table = merged_table[master_list]
        
        # filtering the merged_table df by the branch specified
        branch_data = merged_table.loc[(merged_table['Branch'] == self.branch), :]

        # assigning the start/end/other to dictate which formula to use
        start = pipeline_info['start']
        end = pipeline_info['end']
        other = pipeline_info['other']
        
        # filtering the workflows without an 'other' condition
        if other == 'none':

            # produce records for pipeline where 'start' and 'end' are not blank            
            workflow_data = branch_data.loc[(
                branch_data[start].isnull() == False )&(
                branch_data[end].isnull() == False),:]
            
        # filtering the workflows dependent on permit requirements
        elif other == 'Permit Req?':
            
            # produce records for pipeline where 'start' and 'end' are not blank, and 
            # where 'other' = 'Y' (for permit req) 
            workflow_data = branch_data.loc[(
                branch_data[start].isnull() == False)&(
                branch_data[end].isnull() == False)&(
                branch_data[other] == 'Y'),:]
            
        else:
            # pipelines with a specific 'other' value like 'other'='job #'
            workflow_data = branch_data.loc[(
                branch_data[start].isnull() == False )&(
                branch_data[end].isnull() == False)&(
                branch_data[other].isnull() == True),:]
        
        # removing records with incorrect data (negative day diffs) from pipeline
        clean_workflow_data = workflow_data.loc[ workflow_data[workflow_list[1] ] >= 0, :]
        
        # storing negatives for auditing later
        outlier_data = workflow_data.loc[ workflow_data[workflow_list[1] ] < 0, :]

        # creating a name for the outlier data according to 'pipeline' and 'branch' being looped
        outlier_name = f'{self.pipeline}_outliers_{self.branch}'
        
        # will not create a 'csv' of outlier data if there are no outlier records
        if len(outlier_data) > 0:
            outlier_data.to_csv(f"data/outlier_tables/{outlier_name}.csv", index=False)
        else:
            pass
        
        # returns the workflow df without negative/outlier records
        return clean_workflow_data
    
    # function that takes specific branch/teammate workflow and calculates aggregrates
    def workflow_analysis_data(self):
        
        # initializing a dict to store the aggregrate information
        workflow_analysis_dict = {}
        
        # want to include the branch and 'pipeline' used for each loop
        workflow_analysis_dict['branch'] = self.branch
        workflow_analysis_dict['workflow'] = self.pipeline
        
        workflow_data_df = self.create_teammate_workflow()
        
        # creating a list of the 'workflow day' calculations (mean, min, max, median, std)
        wf_days_analysis = workflow_data_df[(pipeline_dict[self.pipeline]['workflow_table'][1])]

        workflow_mean = wf_days_analysis.mean()
        workflow_analysis_dict['average_days'] = round(workflow_mean, 2)
        
        workflow_min = wf_days_analysis.min()
        workflow_analysis_dict['min_days'] = workflow_min
        
        workflow_max = wf_days_analysis.max()
        workflow_analysis_dict['max_days'] = workflow_max
        
        workflow_median = wf_days_analysis.median()
        workflow_analysis_dict['median_days'] = workflow_median
        
        workflow_std = wf_days_analysis.std()
        workflow_analysis_dict['std'] = round(workflow_std, 2)
        
        return workflow_analysis_dict

########################### LOOPING TO CREATE EACH WORKFLOW #########################
branch_list = ['DEN', 'COS', 'FCO', 'KCI', 'OMA']

workflow_list = ['Rep Claims', 'FTA Scopes','BC Estimates',
                 'OB Scopes', 'Sup Submittals','BC Approvals',
                 'OB Orders Built', 'GM Orders Processed', 'PA Permits Applied',
                 'PA Permits Processed','PA OA Processed', 'PA OA Invoiced',
                 'PA Notify of Delivery','PA Notify of Start', 'GM Approved for Inspection',
                 'RA Inspection Requested','RA Inspection Processed','Rep COC Collected',
                 'SA Job Docs Uploaded', 'BC Project Invoiced','BC Project Closed']

# initialized an empty list to store each workflow's aggreate from each branch
workflow_aggregrate_list = []

# looping over every workflow...
for workflow in workflow_list:
    
    # initalized an emplty df to store each workflow's project data (all branches)
    workflow_pipeline_df = pd.DataFrame()
    
    #...in every branch
    for branch in branch_list:    
            
        ##############################
        # creating an 'branch workflow' instance for each workflow, in each branch
        workflow_analysis = Branch_Workflow(branch, workflow)
        
        # running the 'workflow analysis' function for each workflow, in each branch
        aggegrates = workflow_analysis.workflow_analysis_data()
        
        # storing the 'aggregate' in the 'workflow_aggregrate_list'
        workflow_aggregrate_list.append(aggegrates)
        
        ##############################
        # running the 'create_teammate_workflow' function for each workflow
        workflow_data = workflow_analysis.create_teammate_workflow()
        
        # adding the pipeline data to the 'workflow_pipeline_df'
        workflow_pipeline_df = workflow_pipeline_df.append(workflow_data, ignore_index= True)
        
        # providing the name of the pipeline to use as a table name
        workflow_name = f'{workflow.lower()}'
        
    # sending the dfs created for each workflow/branch to it's own table in MySQL
    workflow_pipeline_df.to_sql(name=workflow_name,con=engine,if_exists='replace')
    print(f'{workflow_name.upper()} database added to MySQL')
        

########################### AGGREGRATE TABLE #########################
workflow_aggregrate_df = pd.DataFrame.from_dict(workflow_aggregrate_list)

# reorganizing the df
workflow_aggregrate_df = workflow_aggregrate_df[['branch', 'workflow', 'min_days', 'average_days', 'median_days', 'max_days', 'std']]

# sending the df to it's own table in MySQL
workflow_aggregrate_df.to_sql(name='pipeline aggregrates',con=engine,if_exists='replace')



