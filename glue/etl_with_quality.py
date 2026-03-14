"""
Glue ETL Job with Data Quality Evaluation and Anomaly Detection
"""
import sys
import json
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from pyspark.context import SparkContext

# Initialize
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_INPUT_PATH', 'S3_OUTPUT_PATH'])
job.init(args['JOB_NAME'], args)

# Read raw data
raw_data = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [args['S3_INPUT_PATH']]},
    format="parquet"
)

# Data Quality Ruleset with Anomaly Detection
RULESET = """
Rules = [
    # Static rules
    IsComplete "order_id",
    IsUnique "order_id",
    ColumnValues "order_total" >= 0,
    ColumnValues "order_total" <= 1000000,
    
    # Dynamic rules with anomaly detection
    ColumnValues "order_total" with threshold > 0.95,
    RowCount with threshold > 0.90,
    DataFreshness "order_date" <= 24 hours
],
Analyzers = [
    RowCount,
    Completeness "order_total",
    Mean "order_total",
    StandardDeviation "order_total",
    DistinctValuesCount "customer_id"
]
"""

# Evaluate data quality
dq_results = EvaluateDataQuality.apply(
    frame=raw_data,
    ruleset=RULESET,
    publishing_options={
        "dataQualityEvaluationContext": "orders_quality_check",
        "enableDataQualityCloudWatchMetrics": True,
        "enableDataQualityResultsPublishing": True
    }
)

# Extract results
quality_results = dq_results['EvaluateDataQuality.output']
results_df = quality_results.toDF()

# Check for anomalies
anomalies = results_df.filter(
    (results_df.Outcome == "Failed") | 
    (results_df.Outcome == "Anomaly")
).collect()

# Build quality summary
quality_summary = {
    "total_rules": results_df.count(),
    "passed_rules": results_df.filter(results_df.Outcome == "Passed").count(),
    "failed_rules": len([a for a in anomalies if a.Outcome == "Failed"]),
    "anomalies_detected": len([a for a in anomalies if a.Outcome == "Anomaly"]),
    "anomaly_details": [
        {
            "rule": a.Rule,
            "outcome": a.Outcome,
            "actual_value": str(a.ActualValue) if hasattr(a, 'ActualValue') else None,
        } for a in anomalies
    ]
}

# Write quality summary for Step Functions
summary_path = f"{args['S3_OUTPUT_PATH']}/quality_results/summary.json"
spark.sparkContext.parallelize([json.dumps(quality_summary)]).coalesce(1).saveAsTextFile(summary_path)

# Write curated data if no critical failures
if quality_summary['failed_rules'] == 0:
    glueContext.write_dynamic_frame.from_options(
        frame=raw_data,
        connection_type="s3",
        connection_options={"path": f"{args['S3_OUTPUT_PATH']}/curated/"},
        format="parquet"
    )

job.commit()
