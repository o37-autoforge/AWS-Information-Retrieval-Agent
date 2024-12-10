import boto3
import json
import os
import subprocess
from botocore.exceptions import ClientError

def create_iam_roles():
    iam = boto3.client('iam')

def create_cloudformation_template():
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Description": "AWS Config to Redshift Pipeline with Enhanced Data Collection",
        "Resources": {
            "AWSConfigBucket": {
                "Type": "AWS::S3::Bucket",
                "Properties": {
                    "BucketName": "aws-config-bucket-${AWS::AccountId}"
                }
            },
            "FirehoseDeliveryStream": {
                "Type": "AWS::KinesisFirehose::DeliveryStream",
                "Properties": {
                    "DeliveryStreamName": "AWSConfigDeliveryStream",
                    "RedshiftDestinationConfiguration": {
                        "ClusterJDBCURL": {"Ref": "RedshiftClusterJDBCURL"},
                        "CopyCommand": {
                            "DataTableName": "aws_config_resources",
                            "CopyOptions": "JSON 'auto'"
                        },
                        "Username": {"Ref": "RedshiftUsername"},
                        "Password": {"Ref": "RedshiftPassword"},
                        "RoleARN": {"Fn::GetAtt": ["FirehoseDeliveryRole", "Arn"]},
                        "S3Configuration": {
                            "BucketARN": {"Fn::GetAtt": ["AWSConfigBucket", "Arn"]},
                            "BufferingHints": {
                                "IntervalInSeconds": 300,
                                "SizeInMBs": 5
                            },
                            "CompressionFormat": "UNCOMPRESSED",
                            "Prefix": "firehose/"
                        }
                    }
                }
            },
            "CloudWatchLogsSubscriptionFilter": {
                "Type": "AWS::Logs::SubscriptionFilter",
                "Properties": {
                    "DestinationArn": {"Fn::GetAtt": ["FirehoseDeliveryStream", "Arn"]},
                    "FilterPattern": "",
                    "LogGroupName": "/aws/lambda/example-function"
                }
            },
            "AMICollectorLambda": {
                "Type": "AWS::Lambda::Function",
                "Properties": {
                    "FunctionName": "AMICollector",
                    "Handler": "index.handler",
                    "Role": {"Fn::GetAtt": ["LambdaExecutionRole", "Arn"]},
                    "Code": {
                        "ZipFile": {
                            "Fn::Join": ["\n", [
                                "import boto3",
                                "import json",
                                "def handler(event, context):",
                                "    ec2 = boto3.client('ec2')",
                                "    response = ec2.describe_images(Owners=['self'])",
                                "    return json.dumps(response['Images'])"
                            ]]
                        }
                    },
                    "Runtime": "python3.8",
                    "Timeout": 30
                }
            },
            "ServiceQuotasCollectorLambda": {
                "Type": "AWS::Lambda::Function",
                "Properties": {
                    "FunctionName": "ServiceQuotasCollector",
                    "Handler": "index.handler",
                    "Role": {"Fn::GetAtt": ["LambdaExecutionRole", "Arn"]},
                    "Code": {
                        "ZipFile": {
                            "Fn::Join": ["\n", [
                                "import boto3",
                                "import json",
                                "def handler(event, context):",
                                "    quotas = boto3.client('service-quotas')",
                                "    response = quotas.list_service_quotas(ServiceCode='ec2')",
                                "    return json.dumps(response['Quotas'])"
                            ]]
                        }
                    },
                    "Runtime": "python3.8",
                    "Timeout": 30
                }
            },
            "LambdaExecutionRole": {
                "Type": "AWS::IAM::Role",
                "Properties": {
                    "AssumeRolePolicyDocument": {
                        "Version": "2012-10-17",
                        "Statement": [{
                            "Effect": "Allow",
                            "Principal": {"Service": ["lambda.amazonaws.com"]},
                            "Action": ["sts:AssumeRole"]
                        }]
                    },
                    "ManagedPolicyArns": [
                        "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
                        "arn:aws:iam::aws:policy/AmazonEC2ReadOnlyAccess",
                        "arn:aws:iam::aws:policy/ServiceQuotasReadOnlyAccess"
                    ]
                }
            }
        },
        "Parameters": {
            "RedshiftClusterJDBCURL": {
                "Type": "String",
                "Description": "JDBC URL for the Redshift cluster"
            },
            "RedshiftUsername": {
                "Type": "String",
                "Description": "Username for Redshift database"
            },
            "RedshiftPassword": {
                "Type": "String",
                "Description": "Password for Redshift database",
                "NoEcho": True
            }
        }
    }
    
    with open('aws_config_pipeline_template.json', 'w') as f:
        json.dump(template, f, indent=2)
    
    print("CloudFormation template created: aws_config_pipeline_template.json")

def deploy_cloudformation_stack(stack_name, template_file, parameters):
    cloudformation = boto3.client('cloudformation')
    
    with open(template_file, 'r') as f:
        template_body = f.read()
    
    try:
        cloudformation.create_stack(
            StackName=stack_name,
            TemplateBody=template_body,
            Parameters=parameters,
            Capabilities=['CAPABILITY_NAMED_IAM']
        )
        print(f"CloudFormation stack {stack_name} creation initiated")
    except ClientError as e:
        print(f"Error creating CloudFormation stack: {e}")

def create_redshift_tables():
    redshift = boto3.client('redshift-data')
    cluster_identifier = 'your-redshift-cluster-identifier'
    database = 'your-redshift-database'
    db_user = 'your-redshift-username'

    sql_commands = """
    CREATE TABLE IF NOT EXISTS cloudwatch_logs (
        log_group VARCHAR(255),
        log_stream VARCHAR(255),
        timestamp TIMESTAMP,
        message TEXT
    );

    CREATE TABLE IF NOT EXISTS ami_details (
        ami_id VARCHAR(255),
        name VARCHAR(255),
        description TEXT,
        creation_date TIMESTAMP,
        owner_id VARCHAR(255)
    );

    CREATE TABLE IF NOT EXISTS quota_details (
        service VARCHAR(255),
        quota_name VARCHAR(255),
        quota_value FLOAT,
        used FLOAT,
        unit VARCHAR(50)
    );

    CREATE TABLE IF NOT EXISTS service_limits (
        service VARCHAR(255),
        limit_name VARCHAR(255),
        limit_value FLOAT,
        unit VARCHAR(50)
    );

    CREATE TABLE IF NOT EXISTS cost_usage_reports (
        time_period VARCHAR(50),
        service VARCHAR(255),
        cost FLOAT,
        usage FLOAT,
        unit VARCHAR(50)
    );
    """

    try:
        response = redshift.execute_statement(
            ClusterIdentifier=cluster_identifier,
            Database=database,
            DbUser=db_user,
            Sql=sql_commands
        )
        print("Redshift tables created successfully")
    except ClientError as e:
        print(f"Error creating Redshift tables: {e}")

def main():
    # Create IAM roles
    create_iam_roles()
    
    # Create CloudFormation template
    create_cloudformation_template()
    
    # Deploy CloudFormation stack
    stack_name = 'AWSConfigPipeline'
    template_file = 'aws_config_pipeline_template.json'
    parameters = [
        {'ParameterKey': 'RedshiftClusterJDBCURL', 'ParameterValue': 'your_redshift_jdbc_url'},
        {'ParameterKey': 'RedshiftUsername', 'ParameterValue': 'your_redshift_username'},
        {'ParameterKey': 'RedshiftPassword', 'ParameterValue': 'your_redshift_password'}
    ]
    deploy_cloudformation_stack(stack_name, template_file, parameters)
    
    # Create expanded set of tables in Redshift
    create_redshift_tables()
    
    print("AWS Config to database pipeline setup completed with enhanced data collection")

if __name__ == "__main__":
    main()