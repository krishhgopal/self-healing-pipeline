"""
Execute auto-remediation based on Bedrock's diagnosis.
"""
import boto3
import json
import os

glue = boto3.client('glue')

DATA_BUCKET = os.environ.get('DATA_BUCKET')


def handler(event, context):
    """Execute auto-remediation based on diagnosis."""
    diagnosis = event.get('parsedDiagnosis', {})
    quality_results = event.get('qualityResults', {})
    input_path = event.get('inputPath', '')
    
    fix_instructions = diagnosis.get('fix_instructions', {})
    fix_type = fix_instructions.get('type', 'unknown')
    
    # Route to appropriate fix handler
    fix_handlers = {
        'fill_nulls': handle_fill_nulls,
        'remove_negatives': handle_remove_negatives,
        'deduplicate': handle_deduplicate,
        'filter_outliers': handle_filter_outliers,
    }
    
    handler_fn = fix_handlers.get(fix_type, handle_unknown_fix)
    result = handler_fn(input_path, fix_instructions)
    
    return result


def handle_fill_nulls(input_path, instructions):
    """Fill null values with specified strategy."""
    column = instructions.get('column')
    strategy = instructions.get('strategy', 'mean')
    
    spark_code = generate_fill_nulls_code(input_path, column, strategy)
    job_run = start_remediation_job(spark_code, 'fill_nulls')
    
    return {
        'success': True,
        'fix_type': 'fill_nulls',
        'fixedDataPath': input_path.replace('/raw/', '/remediated/'),
        'jobRunId': job_run.get('JobRunId'),
        'details': f"Filled nulls in {column} using {strategy} strategy"
    }


def handle_remove_negatives(input_path, instructions):
    """Remove or fix negative values."""
    column = instructions.get('column')
    action = instructions.get('action', 'remove')
    
    spark_code = generate_remove_negatives_code(input_path, column, action)
    job_run = start_remediation_job(spark_code, 'remove_negatives')
    
    return {
        'success': True,
        'fix_type': 'remove_negatives',
        'fixedDataPath': input_path.replace('/raw/', '/remediated/'),
        'jobRunId': job_run.get('JobRunId'),
        'details': f"Applied {action} to negative values in {column}"
    }


def handle_deduplicate(input_path, instructions):
    """Remove duplicate records."""
    key_columns = instructions.get('key_columns', [])
    keep = instructions.get('keep', 'first')
    
    spark_code = generate_deduplicate_code(input_path, key_columns, keep)
    job_run = start_remediation_job(spark_code, 'deduplicate')
    
    return {
        'success': True,
        'fix_type': 'deduplicate',
        'fixedDataPath': input_path.replace('/raw/', '/remediated/'),
        'jobRunId': job_run.get('JobRunId'),
        'details': f"Removed duplicates keeping {keep} occurrence"
    }


def handle_filter_outliers(input_path, instructions):
    """Filter statistical outliers."""
    column = instructions.get('column')
    method = instructions.get('method', 'iqr')
    threshold = instructions.get('threshold', 1.5 if method == 'iqr' else 3)
    
    spark_code = generate_filter_outliers_code(input_path, column, method, threshold)
    job_run = start_remediation_job(spark_code, 'filter_outliers')
    
    return {
        'success': True,
        'fix_type': 'filter_outliers',
        'fixedDataPath': input_path.replace('/raw/', '/remediated/'),
        'jobRunId': job_run.get('JobRunId'),
        'details': f"Filtered outliers in {column} using {method} method"
    }


def handle_unknown_fix(input_path, instructions):
    """Handle unknown fix type."""
    return {
        'success': False,
        'fix_type': 'unknown',
        'error': 'Unknown fix type specified'
    }


def start_remediation_job(spark_code: str, job_name: str) -> dict:
    """Start a Glue job with the generated remediation code."""
    # In production, this would start an actual Glue job
    # For demo, we return a mock response
    return {'JobRunId': f'jr_{job_name}_mock'}


def generate_fill_nulls_code(input_path, column, strategy):
    return f'''
df = spark.read.parquet("{input_path}")
if "{strategy}" == "mean":
    fill_value = df.select(mean("{column}")).collect()[0][0]
elif "{strategy}" == "median":
    fill_value = df.approxQuantile("{column}", [0.5], 0.01)[0]
df_fixed = df.fillna({{"{column}": fill_value}})
df_fixed.write.mode("overwrite").parquet("{input_path.replace('/raw/', '/remediated/')}")
'''


def generate_remove_negatives_code(input_path, column, action):
    return f'''
df = spark.read.parquet("{input_path}")
if "{action}" == "remove":
    df_fixed = df.filter(col("{column}") >= 0)
elif "{action}" == "absolute":
    df_fixed = df.withColumn("{column}", abs(col("{column}")))
df_fixed.write.mode("overwrite").parquet("{input_path.replace('/raw/', '/remediated/')}")
'''


def generate_deduplicate_code(input_path, key_columns, keep):
    return f'''
df = spark.read.parquet("{input_path}")
df_fixed = df.dropDuplicates({key_columns})
df_fixed.write.mode("overwrite").parquet("{input_path.replace('/raw/', '/remediated/')}")
'''


def generate_filter_outliers_code(input_path, column, method, threshold):
    return f'''
df = spark.read.parquet("{input_path}")
quantiles = df.approxQuantile("{column}", [0.25, 0.75], 0.01)
q1, q3 = quantiles[0], quantiles[1]
iqr = q3 - q1
df_fixed = df.filter((col("{column}") >= q1 - {threshold} * iqr) & (col("{column}") <= q3 + {threshold} * iqr))
df_fixed.write.mode("overwrite").parquet("{input_path.replace('/raw/', '/remediated/')}")
'''
