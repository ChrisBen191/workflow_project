# Workflow Project
***
#### 

![Image of Workflow Chart](https://images.unsplash.com/photo-1543286386-713bdd548da4?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=1050&q=80)*Photo by Isaac Smith on Unsplash*

## Establish the process for a Project... ***from Claim Date to Close Date...***

Can the influence of each Teammate involved in the customer workflow be measured? What projects are being held up in the customer pipeline? What common factors exist for projects above the average turnaround time? 

### Required Datasets:
- **[TVA] Workflow Analysis *(rename 'workflow_analysis.csv')***

Dataset that contains the datestamps recorded on a record as it progresses through the customer pipeline; this includes projects that have been marked 'Dead'.

---
- **[TVA] Project Workflow Analysis *(rename 'project_workfow_analysis.csv')*** 

This dataset contains the datestamps recorded on a record as it progresses through the production pipeline.

Due to database limitations, the 'supplier name' and 'building department' data is collected in this dataset to prevent loss of information.

---

- **[TVA] Project Info Analysis *(rename 'info_table.csv')***

This dataset contains all Teammates involved in jobs going into production. Due to limiatations of the database, this dataset cannot be used for any analysis including 'Dead' records because the BCs and OBs are unassigned from 'Dead' jobs in the current 'Audit Dead Jobs' process.

---

- **[TVA] FTA Scope Analysis *(rename 'rejection_table.csv')***

This dataset contains all 'FTA scope rejection' records for the season, even if the project has since been marked 'Dead'. They contain all rejects for a project, if it was rejected multiple times.

---

- **[TVA] GM Change Order Analysis *(rename 'change_orders.csv')***

This dataset contains all 'change order' records for the season. They contain all change orders for a project, with some projects having multiple change orders.

---

- **[TVA] GM Labor Order Adjustment Analysis *(rename 'labor_adjustments.csv')***

This dataset contains all 'FTA scope rejection' records for the season, even if the project has since been marked 'Dead'. They contain all rejects for a project, if it was rejected multiple times.

---



