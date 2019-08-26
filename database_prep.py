import numpy as np
import pandas as pd

import datetime
from datetime import datetime
from datetime import timedelta

from sqlalchemy import create_engine
from config import username, password, hostname, port, database
engine = create_engine(f'mysql+pymysql://{username}:{password}@{hostname}/{database}')

# importing the 'data_prep.py' file
import data_prep

############### PROJECT TABLE DATA ####################
project_table_data = "./data/database_tables/project_table.csv"
project_table = pd.read_csv(
    project_table_data, 
    dtype={'Claim #': str,'Job #': str,'Branch':str,'Claim Status':str},
    parse_dates=[
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
        'BC Project Closed Date'])

############### PROJECT INFORMATION TABLE DATA ###############
project_info_data = "./data/database_tables/project_info_table.csv"

info_table = pd.read_csv(project_info_data, dtype={
    'Claim #':str, 'Job #':str, 'Branch':str, 'City':str, 'Building Department':str,
    'Permit Req?':str, 'Supplier Name':str, 'Crew':str, 'Insurance Company':str,
    'Multi-rejected':str, 'Scope Rejections':str, 'Change Orders':str,'Labor Adjustments':str})

# due to manual 'city' field, city is incorrectly spelled or off on capitalization
info_table['City'] = info_table['City'].str.upper()

#################### WORKFLOW TABLE DATA ####################
workflow_table_data = "./data/database_tables/workflow_table.csv"
workflow_table = pd.read_csv(workflow_table_data)

######################### EAGLEVIEW DATA #########################
eagleview_table_data = "./data/raw_datasets/[TVA] EagleView Analysis.csv"
eagleview_table = pd.read_csv(eagleview_table_data)

# converting the squares measurement to roofing SQs
eagleview_table['Square Feet'] = eagleview_table['Square Feet'] / 100

# removing all duplcates
eagleview_table = eagleview_table.drop_duplicates(subset='Claim #', keep='first')


# ########################### PIPELINE DICTIONARY #########################
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
        workflow_analysis_dict['mean'] = round(workflow_mean, 2)
        
        workflow_min = wf_days_analysis.min()
        workflow_analysis_dict['min'] = workflow_min
        
        workflow_max = wf_days_analysis.max()
        workflow_analysis_dict['max'] = workflow_max
        
        workflow_median = wf_days_analysis.median()
        workflow_analysis_dict['median'] = workflow_median
        
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
workflow_aggregrate_df = workflow_aggregrate_df[['branch', 'workflow', 'min', 'mean', 'median', 'max', 'std']]

# sending the df to it's own table in MySQL
workflow_aggregrate_df.to_sql(name='pipeline aggregrates',con=engine,if_exists='replace')


