########## Dependencies ##########
import pandas as pd
import datetime
from datetime import datetime
from datetime import timedelta
from datetime import date


########## Import Data ##########
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

# imports the '[TVA] Eagleview Analysis' data
eagleview_data = "./data/raw_datasets/[TVA] EagleView Analysis.csv"

# imports the 'coc collected' SA data currently being tracked
<<<<<<< HEAD
coc_collected_data = "./data/raw_datasets/Workflow Tracker - Rep COC Collected.csv" 
=======
coc_collected_data = "./data/tracker_data/coc_collected.csv" 
>>>>>>> d6f959cdc6a79ff21248f9d1dd0c9895e592b623

########## Data Cleaning Functions ##########

########## function to create the 'info_df'
def create_project_info_table():
    info_df = pd.read_csv(info_data, dtype='str')
    
    # removing all duplcates
    info_df = info_df.drop_duplicates(subset='Claim #', keep='first')

    return info_df

########## function to create the 'project_df'
def create_project_sales_table():
    project_df = pd.read_csv(all_project_data, 
    dtype={'Claim #': str, 'Job #': str, 'Branch': str, 'Claim Status': str}, 
    parse_dates=['Claim # Date', 'FTA Scope. Req Date', 'Submit for Estimate Date',
          '[OB] Created Scope Calc', '[B] Created Estimate Date',
          'Job Submittal Date', '[B] - Date Approved by BC', '[OB] Completed',
          'COC Rcvd Date [A]', 'Job Docs Scanned', 
          '[B] Sent Invoice Packet to Ins Co','[B] Settled with Insurance'])
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
def create_project_production_table():
    production_df = pd.read_csv(
        all_production_data, 
        dtype={'Job #': str,'Supplier Name': str,'Building Department': str,'Permit Req?': str},
        parse_dates=['Permit Applied [A]','Order Date','Permit Received','OA Date','Invoice Date',
      'Ntfd H.O. Dlvry','Dlvry Start','Ntfd H.O. Start','Roof Start','Roof Complete Date',
      'R4F','Requested Final Insp','Final Inspection Date'])
    
        # removing all duplcates
    production_df = production_df.drop_duplicates(subset='Claim #', keep='first')

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

########## function to create the 'rejection_table_df'
def create_rejection_table():

    rejection_table_df = pd.read_csv(rejection_data, dtype={'Claim #': str},parse_dates=['Created'])

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
def create_oa_processed_updated_table():
    oa_processed_df = pd.read_csv(oa_processed_data, dtype={'Job #': str}, parse_dates=['Updated'], usecols=['Job #', 'Updated'])

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
def create_oa_invoiced_updated_table():
    oa_invoiced_df = pd.read_csv(oa_invoiced_data, dtype={'Job #': str}, parse_dates=['Updated'], usecols=['Job #', 'Updated'])

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
def create_approve_for_inspection_updated_table():
    approve_for_inspection_df = pd.read_csv(approved_for_inspection_data, dtype={'Job #': str}, parse_dates=['Updated'], usecols=['Job #','Updated'])

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
def create_change_order_table():
    change_order_df = pd.read_csv(change_order_data, dtype={'Job #': str}, parse_dates=['Created'])

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
def create_labor_adjustment_table():
    # GM Labor Adjustment Analysis
    labor_adjustment_df = pd.read_csv(labor_adjustment_data, dtype={'Order ID': str}, parse_dates=['Created'])

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
def create_coc_updated_table():
    coc_collected_df = pd.read_csv(coc_collected_data, dtype={'Job #': str}, parse_dates=['Updated'], usecols=['Claim #', 'Job #', 'Updated'])

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
    project_table = create_project_sales_table()
    updated_coc_table = create_coc_updated_table()

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
    project_table = create_project_production_table()
    updated_oa_processed_table = create_oa_processed_updated_table()
    updated_oa_invoiced_table = create_oa_invoiced_updated_table()
    updated_approved_for_inspection_table = create_approve_for_inspection_updated_table()

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
            # use the updated oa processed date
            real_oa_processed_date = row['Updated OA Processed']
            parsed_oa_processed_dates.append(real_oa_processed_date)
        else:
            # if not use the trackvia oa processed date
            real_oa_processed_date = row['OA Date']
            parsed_oa_processed_dates.append(real_oa_processed_date)


        # if there is an 'updated oa invoiced' date...
        if row['Updated OA Invoiced'] == row['Updated OA Invoiced']:
            # use the updated oa invoiced date
            real_oa_invoiced_date = row['Updated OA Invoiced']
            parsed_oa_invoiced_dates.append(real_oa_invoiced_date)
        else:
            # if not use the trackvia oa invoiced date
            real_oa_invoiced_date = row['Invoice Date']
            parsed_oa_invoiced_dates.append(real_oa_invoiced_date)


        # if there is an 'updated approved for inspection' date...
        if row['Updated R4F'] == row['Updated R4F']:
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
    reject_table = create_rejection_table()
    change_order_table = create_change_order_table()
    labor_adjustment_table = create_labor_adjustment_table()

    del reject_table['Scope Rejections']
    del labor_adjustment_table['Labor Adjustments']
    del change_order_table['Change Orders']

    datestamped_workflow_table = sales_table.merge(
        production_table, how='left', on=['Claim #', 'Job #']).merge(
        reject_table, how='left', on='Claim #').merge(
        change_order_table, how='left', on='Job #').merge(
        labor_adjustment_table, how='left', on='Job #')

    # renaming the combined data
    datestamped_workflow_table = datestamped_workflow_table.rename(columns={
        'Claim # Date': 'Rep Agreement Signed','FTA Scope. Req Date': 'Rep Claim Collected','Most Recent Rejection': 'FTA Scope Rejected',
        'Submit for Estimate Date': 'FTA Scope Completed','[B] Created Estimate Date': 'BC Estimate Completed','[OB] Created Scope Calc': 'OB Scope Completed',
        'Job Submittal Date': 'Sup Job Submitted','[B] - Date Approved by BC': 'BC Approved for Production','[OB] Completed': 'OB Order Built',
        'Permit Applied [A]': 'PA Permit Applied','Order Date': 'GM Order Processed','Permit Received': 'PA Permit Processed',
        'OA Date': 'PA OA Processed','Invoice Date': 'PA OA Invoiced','Ntfd H.O. Dlvry': 'PA Notify of Delivery',
        'Dlvry Start': 'Delivery Date','Ntfd H.O. Start': 'PA Notify of Start','Roof Complete Date': 'Roof End',
        'R4F': 'GM Approved for Inspection','Requested Final Insp': 'RA Inspection Requested','Final Inspection Date': 'RA Inspection Processed',
        'COC Rcvd Date [A]': 'Rep COC Collected','Job Docs Scanned': 'SA Job Docs Uploaded','[B] Sent Invoice Packet to Ins Co': 'BC Project Invoiced',
        '[B] Settled with Insurance': 'BC Project Closed'})

    # Organizing combined Data to follow Workflow
    all_project_df = datestamped_workflow_table[[
        'Claim #','Job #','Branch','Claim Status','Rep Agreement Signed',
        'Rep Claim Collected','FTA Scope Completed','FTA Scope Rejected','BC Estimate Completed','OB Scope Completed',
        'Sup Job Submitted','BC Approved for Production','OB Order Built','GM Order Processed','PA Permit Applied',
        'PA Permit Processed','PA OA Processed','PA OA Invoiced','PA Notify of Delivery','PA Notify of Start',
        'Delivery Date','Roof Start','Roof End','GM Approved for Inspection','GM Change Order Date',
        'GM Labor Adjustment Date','RA Inspection Requested','RA Inspection Processed','Rep COC Collected','SA Job Docs Uploaded',
        'BC Project Invoiced','BC Project Closed']]
    
    return all_project_df

# creating the project info database
def create_info_database():

    # assigning the functions to variables to be able to merge them
    info_table = create_project_info_table()
    production_table = create_project_production_table()
    reject_table = create_rejection_table()
    change_order_table = create_change_order_table()
    labor_adjustment_table = create_labor_adjustment_table()

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

    # saving the 'datestamp database' to a variable to iterate over
    all_project_df = create_datestamp_database()

    # iterating over the df to create 'date diff' variables
    for index, row in all_project_df.iterrows():

        # creating 'date_diff' variables for each step in the workflow
        rep_claim_date_diff = float((row['Rep Claim Collected'] - row['Rep Agreement Signed']).days)

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
                bc_estimate_date_diff = (row['BC Estimate Completed'] - datetime(2019, 7, 16)).days
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

        # due to manual OA 'Processed' and 'Invoice' date fields, PAs have been recording false dates...
        if row['PA OA Invoiced'] < row['PA OA Processed']:

            # ...which provide no advantage to the project.
            row['PA OA Invoiced'] = row['PA OA Processed'] + timedelta(days=1)

        # OAs can't be invoiced before they have been processed (approved)
        pa_invoice_date_diff = (row['PA OA Invoiced'] - row['PA OA Processed']).days

        # due to manual 'Approved for inspection' (R4F) date field, GMs have been recording false dates...
        if row['GM Approved for Inspection'] < row['Roof End']:

            # ...which provide no advantage to the project; those will be reset to the day after the build.
            row['GM Approved for Inspection'] = row['Roof End'] + timedelta(days=1)

        # roofs can't be approved for inspection prior to the roof being built
        gm_approval_date_diff = (row['GM Approved for Inspection'] - row['Roof End']).days

        # due to manual 'COC Collected' (COC Rcvd [A]) date field, SAs have been recording false dates...
        if row['Rep COC Collected'] < row['Roof End']:

            # ...these will be reset for after the build.
            row['Rep COC Collected'] = row['Roof End'] + timedelta(days=1)

        # coc's can't be collected until after the roof is built, not before
        rep_coc_collected_date_diff = (row['Rep COC Collected'] - row['Roof End']).days

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

    # Creating 'Workflow Days' df
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
        'BC Invoicing Project': bc_project_invoiced_diff,
        'BC Closed Project': bc_project_closed_diff
    })
    # creating a column holding the running tally across a row (project)
    # can be done because not including 'date diffs' on non-workflow items
    days_df['Days in Pipeline'] = days_df.sum(axis=1)
    
    return days_df

############### Output Function ###############
def create_database_tables():

    datestamp_table = create_datestamp_database()
    workflow_table = create_workflow_database()
    info_table = create_info_database()

<<<<<<< HEAD
    datestamp_table.to_csv("data/database_tables/project_table.csv", index=False)
    workflow_table.to_csv("data/database_tables/workflow_table.csv", index=False)
    info_table.to_csv("data/database_tables/project_info_table.csv", index=False)
    
    return (print(f"Database Tables Created"))



def reporting_tool():
    
    print(f"---------------------------------------------")
    print(f"------------- TRACKVIA DATASETS -------------")
    print(f"---------------------------------------------")
    
    # these are data importing/creating reports 
    info_data = create_project_info_table()
    print(f"Project Info Table: {len(info_data)}")
    
    sales_data = create_project_sales_table()
    print(f"Project Sales Table: {len(sales_data)}")
    
    production_data = create_project_production_table()
    print(f"Project Production Table: {len(production_data)}")
    
    rejection_data = create_rejection_table()
    print(f"FTA Rejection Table: {len(rejection_data)}")
    
    oa_processed_data = create_oa_processed_updated_table()
    print(f"OA Processed Table: {len(oa_processed_data)}")
    
    oa_invoiced_data = create_oa_invoiced_updated_table()
    print(f"OA Invoiced Table: {len(oa_invoiced_data)}")
    
    gm_approved_data = create_approve_for_inspection_updated_table()
    print(f"GM Approved for Inspection Table: {len(gm_approved_data)}")
    
    change_order_data = create_change_order_table()
    print(f"GM Change Order Table: {len(change_order_data)}")
    
    labor_adjustment_data = create_labor_adjustment_table()
    print(f"GM Labor Adjustment Table: {len(labor_adjustment_data)}")
    
    coc_uploaded_data = create_coc_updated_table()
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
    create_database_tables()
    print(f"------------------------------------------")
    return print(f'Report Complete')

reporting_tool()



=======
    datestamp_table.to_csv("data/cleaned_data/project_table.csv", index=False)
    workflow_table.to_csv("data/cleaned_data/workflow_table.csv", index=False)
    info_table.to_csv("data/cleaned_data/project_info_table.csv", index=False)
    
    return (print(f"Database Tables Created"))

create_database_tables()
>>>>>>> d6f959cdc6a79ff21248f9d1dd0c9895e592b623

def reporting_tool():
    
    print(f"---------------------------------------------")
    print(f"------------- TRACKVIA DATASETS -------------")
    print(f"---------------------------------------------")
    
    # these are data importing/creating reports 
    info_data = create_project_info_table()
    print(f"Project Info Table: {len(info_data)}")
    
    sales_data = create_project_sales_table()
    print(f"Project Sales Table: {len(sales_data)}")
    
    production_data = create_project_production_table()
    print(f"Project Production Table: {len(production_data)}")
    
    rejection_data = create_rejection_table()
    print(f"FTA Rejection Table: {len(rejection_data)}")
    
    oa_processed_data = create_oa_processed_updated_table()
    print(f"OA Processed Table: {len(oa_processed_data)}")
    
    oa_invoiced_data = create_oa_invoiced_updated_table()
    print(f"OA Invoiced Table: {len(oa_invoiced_data)}")
    
    gm_approved_data = create_approve_for_inspection_updated_table()
    print(f"GM Approved for Inspection Table: {len(gm_approved_data)}")
    
    change_order_data = create_change_order_table()
    print(f"GM Change Order Table: {len(change_order_data)}")
    
    labor_adjustment_data = create_labor_adjustment_table()
    print(f"GM Labor Adjustment Table: {len(labor_adjustment_data)}")
    
    coc_uploaded_data = create_coc_updated_table()
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
    
    return print(f'Report Complete')

reporting_tool()




