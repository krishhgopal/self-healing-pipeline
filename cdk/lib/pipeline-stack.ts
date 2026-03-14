import * as cdk from 'aws-cdk-lib';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as glue from 'aws-cdk-lib/aws-glue';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as sns from 'aws-cdk-lib/aws-sns';
import * as sfn from 'aws-cdk-lib/aws-stepfunctions';
import { Construct } from 'constructs';

export class SelfHealingPipelineStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // S3 Buckets
    const dataBucket = new s3.Bucket(this, 'DataBucket', {
      bucketName: `self-healing-pipeline-${this.account}-${this.region}`,
      versioned: true,
    });

    // DynamoDB for incident history
    const incidentTable = new dynamodb.Table(this, 'IncidentTable', {
      tableName: 'pipeline-incidents',
      partitionKey: { name: 'incident_id', type: dynamodb.AttributeType.STRING },
      sortKey: { name: 'timestamp', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
    });

    // SNS Topics for alerts
    const alertTopic = new sns.Topic(this, 'AlertTopic', {
      topicName: 'data-quality-alerts',
    });

    const criticalTopic = new sns.Topic(this, 'CriticalTopic', {
      topicName: 'data-quality-critical',
    });

    // Glue Database
    const glueDatabase = new glue.CfnDatabase(this, 'GlueDatabase', {
      catalogId: this.account,
      databaseInput: { name: 'ecommerce_db' },
    });

    // IAM Role for Glue
    const glueRole = new iam.Role(this, 'GlueRole', {
      assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole'),
      ],
    });
    dataBucket.grantReadWrite(glueRole);

    // Glue ETL Job
    new glue.CfnJob(this, 'ETLJob', {
      name: 'orders-etl-with-quality',
      role: glueRole.roleArn,
      command: {
        name: 'glueetl',
        pythonVersion: '3',
        scriptLocation: `s3://${dataBucket.bucketName}/scripts/etl_with_quality.py`,
      },
      defaultArguments: {
        '--enable-metrics': 'true',
        '--enable-continuous-cloudwatch-log': 'true',
        '--enable-glue-datacatalog': 'true',
      },
      glueVersion: '4.0',
      workerType: 'G.1X',
      numberOfWorkers: 2,
    });

    // Lambda Functions
    const lambdaRole = new iam.Role(this, 'LambdaRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
      ],
    });
    
    lambdaRole.addToPolicy(new iam.PolicyStatement({
      actions: ['bedrock:InvokeModel'],
      resources: ['*'],
    }));
    dataBucket.grantReadWrite(lambdaRole);
    incidentTable.grantReadWriteData(lambdaRole);

    const parseDiagnosisFn = new lambda.Function(this, 'ParseDiagnosisFunction', {
      runtime: lambda.Runtime.PYTHON_3_11,
      handler: 'index.handler',
      code: lambda.Code.fromAsset('../lambda/parse_diagnosis'),
      timeout: cdk.Duration.minutes(1),
      role: lambdaRole,
    });

    const executeRemediationFn = new lambda.Function(this, 'ExecuteRemediationFunction', {
      runtime: lambda.Runtime.PYTHON_3_11,
      handler: 'index.handler',
      code: lambda.Code.fromAsset('../lambda/execute_remediation'),
      timeout: cdk.Duration.minutes(10),
      role: lambdaRole,
      environment: {
        DATA_BUCKET: dataBucket.bucketName,
      },
    });

    const quarantineDataFn = new lambda.Function(this, 'QuarantineDataFunction', {
      runtime: lambda.Runtime.PYTHON_3_11,
      handler: 'index.handler',
      code: lambda.Code.fromAsset('../lambda/quarantine_data'),
      timeout: cdk.Duration.minutes(5),
      role: lambdaRole,
      environment: {
        DATA_BUCKET: dataBucket.bucketName,
      },
    });

    // Step Functions State Machine (from ASL file)
    const stateMachine = new sfn.StateMachine(this, 'SelfHealingPipeline', {
      stateMachineName: 'self-healing-data-pipeline',
      definitionBody: sfn.DefinitionBody.fromFile('../stepfunctions/pipeline.asl.json'),
      timeout: cdk.Duration.hours(2),
    });

    // Outputs
    new cdk.CfnOutput(this, 'BucketName', { value: dataBucket.bucketName });
    new cdk.CfnOutput(this, 'StateMachineArn', { value: stateMachine.stateMachineArn });
    new cdk.CfnOutput(this, 'IncidentTableName', { value: incidentTable.tableName });
  }
}
