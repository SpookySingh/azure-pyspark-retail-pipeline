🚀 End-to-End Retail Customer Segmentation Pipeline (PySpark \& ML)

📌 Project Overview



Retail businesses generate large volumes of transactional data but often lack scalable pipelines to transform this data into actionable customer insights.



This project implements an end-to-end, production-style data pipeline that ingests raw retail transactions, performs scalable data cleaning and feature engineering, and applies unsupervised machine learning to segment customers. The final outputs are analytics-ready and BI-friendly, suitable for tools like Tableau or Power BI.



The pipeline emphasizes data engineering best practices while delivering business-interpretable customer segments.



🏗️ Architecture Overview

Raw Retail Transactions

&nbsp;       ↓

PySpark Data Cleaning

&nbsp;       ↓

Customer-Level Aggregation

&nbsp;       ↓

Feature Scaling

&nbsp;       ↓

KMeans Clustering

&nbsp;       ↓

Cluster Profiling

&nbsp;       ↓

BI / Analytics Consumption



🛠️ Tech Stack

Category	Tools

Language	Python

Big Data Processing	PySpark

Data Processing	Pandas

Machine Learning	scikit-learn

Configuration Management	YAML

Pipeline Automation	Bash

Version Control	Git \& GitHub

Visualization (Downstream)	Tableau / Power BI

📂 Project Structure

azure-pyspark-retail-pipeline/

│

├── data\_lake/

│   ├── raw/

│   ├── processed/

│

├── scripts/

│   ├── 01\_ingestion.py

│   ├── 02\_cleaning.py

│   ├── 03\_feature\_engineering\_spark.py

│   ├── 04\_eda\_customer\_features.py

│   ├── 05\_prepare\_clustering\_data.py

│   ├── 06\_kmeans\_clustering.py

│   ├── 07\_cluster\_profiling.py

│

├── notebooks/

├── logs/

│   ├── run.log

│   ├── error.log

│

├── config.yaml

├── run\_pipeline.sh

├── requirements.txt

└── README.md



⚙️ Key Pipeline Stages

1️⃣ Data Ingestion



Loads raw retail transaction data



Schema inference and validation



Basic sanity checks (row counts, missing files)



2️⃣ Data Cleaning (PySpark)



Removes cancelled transactions



Filters invalid quantities and prices



Handles missing customer identifiers



Deduplicates records



Normalizes textual fields



Designed to be scalable and fault-tolerant.



3️⃣ Feature Engineering



Customer-level features inspired by RFM analysis:



Feature	Description

total\_transactions	Number of purchases

total\_quantity	Total units purchased

total\_revenue	Total customer spend

avg\_order\_value	Revenue per transaction

recency\_days	Days since last purchase



Both Spark-based and Pandas-based implementations are included to demonstrate learning progression.



4️⃣ Feature Scaling



Uses RobustScaler



Handles heavy-tailed distributions and extreme outliers



Ensures clustering stability



5️⃣ Customer Segmentation



Algorithm: KMeans



Optimal K guided by:



Elbow Method (WCSS)



Silhouette Score



Final K selection balances:



Statistical validity



Business interpretability



6️⃣ Cluster Profiling



Aggregated metrics per cluster



Cluster sizes validated



Scaled values converted back to real-world units



Produces business-readable customer personas.



📊 Example Cluster Interpretation

Cluster	Description

High-Value	Very high spend, extremely frequent buyers

Loyal	Regular customers with stable revenue

Occasional	Infrequent buyers with moderate spend

Dormant	Long recency, low engagement

🤖 Pipeline Automation



Run the entire pipeline with a single command:



bash run\_pipeline.sh





Features:



Sequential execution of all stages



Centralized configuration via config.yaml



Execution logs (run.log) and error logs (error.log)



Reproducible and team-friendly



📤 Outputs



All final datasets are written to:



data\_lake/processed/





Key outputs:



Cleaned transaction data



Customer-level feature table



Clustered customer segments



Cluster profiling summary



All outputs are ready for BI dashboards and reporting.



💡 Business Value



Enables targeted marketing and personalization



Identifies high-value and at-risk customers



Converts raw data into explainable customer insights



Scales to large transactional datasets



▶️ How to Run

pip install -r requirements.txt

bash run\_pipeline.sh



👤 Author



Himanshu Singh

MSc Data Science

Focus: Data Engineering • Analytics • Machine Learning



⭐ Final Notes



This project demonstrates:



Production-style data pipeline design



Scalable data processing with Spark



Practical application of unsupervised ML



Strong separation of concerns (config, code, automation)

