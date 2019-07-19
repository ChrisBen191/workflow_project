# Workflow Project
***
#### 

![Image of Workflow Chart](https://images.unsplash.com/photo-1543286386-713bdd548da4?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=1050&q=80)*Photo by Isaac Smith on Unsplash*

## Establish the process for a Project... ***from Claim Date to Close Date...***

Can the influence of each Teammate involved in the customer workflow be measured? What projects are being held up in the customer pipeline? What common factors exist for projects above the average turnaround time? 

### Files in the 'data' folder:
- **[TVA] Workflow Analysis *(renamed 'workflow_analysis.csv')***

Dataset that contains the datestamps recorded on a record as it progresses through the customer pipeline; this includes projects that have been marked 'Dead'.

- **[TVA] Project Workflow Analysis *(renamed 'project_workfow_analysis.csv')*** 

This dataset contains projects in production that have 'notified the Homeowner' of their 'Roof Start' date. This is currently in progress.

---

- **[TVA] FTA Scope Analysis *(renamed 'improvement_table.csv')***

This dataset contains all 'scope rejection' records for the season, even if the project was marked 'Dead'. They contain all rejects for a project, if it was rejected multiple times.

*Currently need to create 'scope rejection' count field*

---

- **[TVA] Teammate Analysis *(renamed 'teammate_table.csv')***

This dataset contains all Teammates involved in jobs going into production. This dataset cannot be used for 'Dead' records because the BCs and OBs are unassigned from 'Dead' jobs.

