# Self-Healing Data Pipeline with AWS Step Functions, Bedrock, and Glue Data Quality

An intelligent data pipeline that detects anomalies, diagnoses root causes using generative AI, and automatically remediates issues.

> **Disclaimer**: This is **reference architecture code** intended to accompany the [Dev.to article](https://dev.to/gmarimuthu/self-healing-data-pipeline). The code demonstrates architectural patterns and AWS service integration but is **not production-ready**. You will need to customize resource names, ARNs, IAM policies, and test thoroughly in your environment before any production use.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         SELF-HEALING DATA PIPELINE                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  S3 Raw ──▶ Glue ETL ──▶ Glue Data Quality ──▶ S3 Curated                  │
│                                │                                            │
│                                │ Anomaly?                                   │
│                                ▼                                            │
│                         Step Functions                                      │
│                                │                                            │
│              ┌─────────────────┼─────────────────┐                         │
│              ▼                 ▼                 ▼                          │
│         Bedrock           Auto Fix          Quarantine                      │
│         Diagnosis         Lambda            & Alert                         │
│              │                 │                 │                          │
│              └─────────────────┴─────────────────┘                         │
│                                │                                            │
│                                ▼                                            │
│                         DynamoDB                                            │
│                      Incident History                                       │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Features

- **ML-Powered Anomaly Detection**: Glue Data Quality learns your data patterns
- **AI Diagnosis**: Amazon Bedrock analyzes anomalies and suggests fixes
- **Auto-Remediation**: Intelligent fixes for common data quality issues
- **Safety Guardrails**: Prevents dangerous auto-fixes on critical issues
- **Incident Learning**: DynamoDB stores history for pattern recognition

## Project Structure

```
├── cdk/                    # AWS CDK infrastructure
│   ├── lib/
│   │   └── pipeline-stack.ts
│   └── bin/app.ts
├── glue/                   # Glue ETL jobs
│   ├── etl_with_quality.py
│   └── data_quality_rules.py
├── lambda/                 # Lambda functions
│   ├── parse_diagnosis/
│   ├── execute_remediation/
│   ├── quarantine_data/
│   └── get_quality_results/
├── stepfunctions/          # State machine definition
│   └── pipeline.asl.json
└── tests/                  # Test data and scripts
    └── sample_data/
```

## Prerequisites

- AWS Account with Bedrock access (Claude 3 Sonnet)
- AWS CDK v2
- Python 3.11+
- Node.js 18+

## Deployment

```bash
# Install dependencies
cd cdk && npm install

# Deploy
cdk deploy

# Upload sample data to trigger pipeline
aws s3 cp tests/sample_data/ s3://your-bucket/raw/ --recursive
```

## Supported Auto-Remediation Actions

| Issue Type | Action | Description |
|------------|--------|-------------|
| Null values | `fill_nulls` | Fill with mean/median/mode/constant |
| Negative values | `remove_negatives` | Remove, absolute, or zero |
| Duplicates | `deduplicate` | Keep first/last occurrence |
| Outliers | `filter_outliers` | IQR or Z-score filtering |
| Type mismatch | `type_cast` | Cast to expected type |

## Safety Guardrails

- Critical severity issues always require human review
- Auto-fix requires confidence score ≥ 0.8
- If >10% of rules fail, data is quarantined
- All actions logged to DynamoDB for audit

## License

MIT

## Author

Gopalakrishnan Marimuthu
