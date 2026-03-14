#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { SelfHealingPipelineStack } from '../lib/pipeline-stack';

const app = new cdk.App();

new SelfHealingPipelineStack(app, 'SelfHealingPipelineStack', {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION || 'us-east-1',
  },
  description: 'Self-Healing Data Pipeline with Bedrock and Glue Data Quality',
});
