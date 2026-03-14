"""
Quarantine bad data by moving to isolated location.
"""
import boto3
import os
from datetime import datetime

s3 = boto3.client('s3')

DATA_BUCKET = os.environ.get('DATA_BUCKET')


def handler(event, context):
    """Quarantine data that failed quality checks."""
    input_path = event.get('inputPath', '')
    quality_results = event.get('qualityResults', {})
    diagnosis = event.get('parsedDiagnosis', {})
    
    # Parse S3 path
    bucket, key = parse_s3_uri(input_path)
    
    # Generate quarantine path
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    quarantine_key = f"quarantine/{timestamp}/{key.split('/')[-1]}"
    
    # Copy to quarantine location
    s3.copy_object(
        Bucket=bucket,
        CopySource={'Bucket': bucket, 'Key': key},
        Key=quarantine_key,
        Metadata={
            'quarantine_reason': diagnosis.get('root_cause', 'Unknown'),
            'quarantine_timestamp': timestamp,
            'failed_rules': str(quality_results.get('failed_rules', 0)),
            'anomalies_detected': str(quality_results.get('anomalies_detected', 0))
        },
        MetadataDirective='REPLACE'
    )
    
    # Write quarantine manifest
    manifest = {
        'original_path': input_path,
        'quarantine_path': f's3://{bucket}/{quarantine_key}',
        'quarantine_timestamp': timestamp,
        'reason': diagnosis.get('root_cause', 'Unknown'),
        'severity': diagnosis.get('severity', 'unknown'),
        'quality_results': quality_results
    }
    
    manifest_key = f"quarantine/{timestamp}/manifest.json"
    s3.put_object(
        Bucket=bucket,
        Key=manifest_key,
        Body=str(manifest),
        ContentType='application/json'
    )
    
    return {
        'quarantined': True,
        'quarantine_path': f's3://{bucket}/{quarantine_key}',
        'manifest_path': f's3://{bucket}/{manifest_key}',
        'reason': diagnosis.get('root_cause', 'Unknown')
    }


def parse_s3_uri(uri: str) -> tuple:
    """Parse S3 URI into bucket and key."""
    parts = uri.replace('s3://', '').split('/', 1)
    return parts[0], parts[1] if len(parts) > 1 else ''
