\# Retail Customer Segmentation Pipeline (PySpark \& Python)



\## Overview

This project implements an end-to-end retail analytics pipeline to perform

customer segmentation using transactional data. The solution combines

PySpark for scalable data processing and Python (pandas, scikit-learn)

for analytics, clustering, and business interpretation.



The pipeline is fully automated, configurable, and designed for

real-world analytics workflows.



---



\## Tech Stack

\- Python

\- PySpark

\- pandas, NumPy

\- scikit-learn

\- matplotlib

\- YAML (configuration)

\- Bash (pipeline automation)



---



\## Pipeline Architecture

1\. \*\*Data Ingestion\*\*

&nbsp;  - Load raw retail transaction data

2\. \*\*Data Cleaning\*\*

&nbsp;  - Remove cancellations, invalid prices, missing customers

3\. \*\*Feature Engineering\*\*

&nbsp;  - Customer-level metrics (RFM-style features)

4\. \*\*EDA\*\*

&nbsp;  - Statistical and visual analysis of customer behavior

5\. \*\*Feature Scaling\*\*

&nbsp;  - RobustScaler to handle skewed monetary features

6\. \*\*Clustering\*\*

&nbsp;  - KMeans clustering

&nbsp;  - Optimal K selection using Elbow Method \& Silhouette Score

7\. \*\*Cluster Profiling\*\*

&nbsp;  - Business interpretation of customer segments

8\. \*\*Automation\*\*

&nbsp;  - Single-command pipeline execution using Bash



---



\## Customer Segments Identified

\- \*\*High-Value Power Customers\*\*

\- \*\*Frequent Loyal Customers\*\*

\- \*\*Occasional Buyers\*\*

\- \*\*Low-Engagement / At-Risk Customers\*\*



Each segment is profiled using real monetary and behavioral metrics.



---



\## How to Run the Pipeline



\### 1. Setup environment

```bash

python -m venv venv

source venv/Scripts/activate

pip install -r requirements.txt



