#!/bin/bash

# ============================
# Pipeline Runner Script
# ============================

# Exit immediately if any command fails
set -e

# Create logs directory if not exists
mkdir -p logs

# Log files
RUN_LOG="logs/run.log"
ERROR_LOG="logs/error.log"

echo "===================================" >> $RUN_LOG
echo "Pipeline started at $(date)" >> $RUN_LOG
echo "===================================" >> $RUN_LOG

# Activate virtual environment
source venv/Scripts/activate >> $RUN_LOG 2>> $ERROR_LOG

# ----------------------------
# Phase 1: Ingestion
# ----------------------------
echo "Running ingestion..." >> $RUN_LOG
python scripts/01_ingestion.py >> $RUN_LOG 2>> $ERROR_LOG

# ----------------------------
# Phase 2: Cleaning
# ----------------------------
echo "Running cleaning..." >> $RUN_LOG
python scripts/02_cleaning.py >> $RUN_LOG 2>> $ERROR_LOG

# ----------------------------
# Phase 3: Feature Engineering (Spark)
# ----------------------------
echo "Running Spark feature engineering..." >> $RUN_LOG
python scripts/03_feature_engineering_spark.py >> $RUN_LOG 2>> $ERROR_LOG

# ----------------------------
# Phase 4: EDA
# ----------------------------
echo "Running EDA..." >> $RUN_LOG
python scripts/04_eda_customer_features.py >> $RUN_LOG 2>> $ERROR_LOG

# ----------------------------
# Phase 5: Prepare clustering data
# ----------------------------
echo "Preparing clustering data..." >> $RUN_LOG
python scripts/05_prepare_clustering_data.py >> $RUN_LOG 2>> $ERROR_LOG

# ----------------------------
# Phase 6: KMeans Clustering
# ----------------------------
echo "Running KMeans clustering..." >> $RUN_LOG
python scripts/06_kmeans_clustering.py >> $RUN_LOG 2>> $ERROR_LOG

# ----------------------------
# Phase 7: Cluster Profiling
# ----------------------------
echo "Running cluster profiling..." >> $RUN_LOG
python scripts/07_cluster_profiling.py >> $RUN_LOG 2>> $ERROR_LOG

echo "===================================" >> $RUN_LOG
echo "Pipeline completed successfully at $(date)" >> $RUN_LOG
echo "===================================" >> $RUN_LOG
