import boto3
import json
import os
import subprocess
from botocore.exceptions import ClientError

def create_iam_roles():
    iam = boto3.client('iam')
    
    # Create role for AWS Config
    config_role_name = 'AWSConfigRole'
    config_trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": "config.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }
    
    try:
        iam.create_role(
            RoleName=config_role_name,
            AssumeRolePolicyDocument=json.dumps(config_trust_policy)
        )
        iam.attach_role_policy(
            RoleName=config_role_name,
            PolicyArn='arn:aws:iam::aws:policy/service-role/AWSConfigRole'
        )
        print(f"Created IAM role: {config_role_name}")
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityAlreadyExists':
            print(f"IAM role {config_role_name} already exists")
        else:
            raise

    # Create role for Firehose
    firehose_role_name = 'FirehoseDeliveryRole'
    firehose_trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": "firehose.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }
    
    try:
        iam.create_role(
            RoleName=firehose_role_name,
            AssumeRolePolicyDocument=json.dumps(firehose_trust_policy)
        )
        iam.attach_role_policy(
            RoleName=firehose_role_name,
            PolicyArn='arn:aws:iam::aws:policy/service-role/AWSConfigRoleForOrganizations'
        )
        print(f"Created IAM role: {firehose_role_name}")
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityAlreadyExists':
            print(f"IAM role {firehose_role_name} already exists")
        else:
            raise

def create_cloudformation_template():
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Description": "AWS Config to Redshift Pipeline",
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
    
    # Run the schema creation script
    subprocess.run(['python', 'aws_config_schema_design.py'])
    
    print("AWS Config to database pipeline setup completed")

if __name__ == "__main__":
    main()