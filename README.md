# Workflow Project
***
#### 

![Image of Workflow Chart](https://images.unsplash.com/photo-1543286386-713bdd548da4?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=1050&q=80)*Photo by Isaac Smith on Unsplash*

## Establish the process for a Project... ***from Claim Date to Close Date...***
Can the influence of each Teammate involved in the customer workflow be measured? What projects are being held up in the customer pipeline? What common factors exist for projects above the average turnaround time? 

### Required Datasets:
File Name | Dataset Information
|-----------|---------|
*workflow_analysis.csv* | Dataset that contains the datestamps recorded on a record as it progresses through the customer pipeline; this includes projects that have been marked 'Dead'.
*project_workfow_analysis.csv* | Dataset that contains the datestamps recorded on a record as it progresses through the production pipeline.
*info_table.csv* | Dataset that contains any additional information on the record that may be revelant in forecasting (branch, building department, supplier, install crew, etc)
*rejection_table.csv* | Dataset that contains all FTA 'scope rejection' records for the season; contains  all rejects for a project, if it was rejected multiple times.
*change_orders.csv* | Dataset that contains all production 'change order' records for the season. They contain all change orders for a project, with some projects having multiple change orders.
*labor_adjustments.csv* | Dataset that contains all 'FTA scope rejection' records for the season, even if the project has since been marked 'Dead'. They contain all labor adjustments on a project, with some project having multiple change orders
*eagleview_analysis.csv* | This dataset contains all 'EagleView' measurements on the season, even if the project has since been marked 'Dead'.

