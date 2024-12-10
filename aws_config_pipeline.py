import boto3
import json
from botocore.exceptions import ClientError
import time

def enable_aws_config(session, region):
    """Enable AWS Config in the specified region."""
    config = session.client('config', region_name=region)
    try:
        config.put_configuration_recorder(
            ConfigurationRecorder={
                'name': 'default',
                'roleARN': f'arn:aws:iam::{session.client("sts").get_caller_identity()["Account"]}:role/aws-service-role/config.amazonaws.com/AWSServiceRoleForConfig',
                'recordingGroup': {
                    'allSupported': True,
                    'includeGlobalResourceTypes': True
                }
            }
        )
        config.start_configuration_recorder(ConfigurationRecorderName='default')
        print(f"AWS Config enabled in region {region}")
    except ClientError as e:
        print(f"Error enabling AWS Config in region {region}: {e}")

def create_streaming_delivery_channel(session, region, bucket_name, firehose_name):
    """Create a streaming delivery channel for AWS Config."""
    config = session.client('config', region_name=region)
    try:
        config.put_delivery_channel(
            DeliveryChannel={
                'name': 'default',
                's3BucketName': bucket_name,
                'configSnapshotDeliveryProperties': {
                    'deliveryFrequency': 'One_Hour'
                },
                'streamingDeliveryProperties': {
                    'streamArn': f'arn:aws:kinesis::{session.client("sts").get_caller_identity()["Account"]}:stream/{firehose_name}'
                }
            }
        )
        print(f"Streaming delivery channel created in region {region}")
    except ClientError as e:
        print(f"Error creating streaming delivery channel in region {region}: {e}")

def create_firehose_delivery_stream(session, region, firehose_name, redshift_cluster_jdbc_url, redshift_table_name, redshift_username, redshift_password):
    """Create a Kinesis Data Firehose delivery stream."""
    firehose = session.client('firehose', region_name=region)
    try:
        response = firehose.create_delivery_stream(
            DeliveryStreamName=firehose_name,
            DeliveryStreamType='DirectPut',
            RedshiftDestinationConfiguration={
                'RoleARN': f'arn:aws:iam::{session.client("sts").get_caller_identity()["Account"]}:role/firehose_delivery_role',
                'ClusterJDBCURL': redshift_cluster_jdbc_url,
                'CopyCommand': {
                    'DataTableName': redshift_table_name,
                    'CopyOptions': "JSON 'auto'"
                },
                'Username': redshift_username,
                'Password': redshift_password
            }
        )
        print(f"Kinesis Data Firehose delivery stream created: {firehose_name}")
        return response['DeliveryStreamARN']
    except ClientError as e:
        print(f"Error creating Kinesis Data Firehose delivery stream: {e}")
        return None

def get_database_schema():
    return """
    CREATE TABLE aws_config_resources (
        resource_id VARCHAR(255),
        resource_type VARCHAR(50),
        region VARCHAR(20),
        configuration JSON,
        tags JSON,
        capture_time TIMESTAMP
    );

    CREATE TABLE cloudwatch_logs (
        log_group VARCHAR(255),
        log_stream VARCHAR(255),
        timestamp TIMESTAMP,
        message TEXT
    );

    CREATE TABLE ami_details (
        ami_id VARCHAR(255),
        name VARCHAR(255),
        description TEXT,
        creation_date TIMESTAMP,
        owner_id VARCHAR(255)
    );

    CREATE TABLE quota_details (
        service VARCHAR(255),
        quota_name VARCHAR(255),
        quota_value FLOAT,
        used FLOAT,
        unit VARCHAR(50)
    );

    CREATE TABLE service_limits (
        service VARCHAR(255),
        limit_name VARCHAR(255),
        limit_value FLOAT,
        unit VARCHAR(50)
    );

    CREATE TABLE cost_usage_reports (
        time_period VARCHAR(50),
        service VARCHAR(255),
        cost FLOAT,
        usage FLOAT,
        unit VARCHAR(50)
    );
    """

def setup_cloudwatch_logs_subscription(session, region, log_group_name, firehose_name):
    """Set up CloudWatch Logs subscription filter to stream logs to Kinesis Data Firehose."""
    logs = session.client('logs', region_name=region)
    try:
        logs.put_subscription_filter(
            logGroupName=log_group_name,
            filterName='FirehoseSubscription',
            filterPattern='',  # Empty string means all log events
            destinationArn=f"arn:aws:firehose:{region}:{session.client('sts').get_caller_identity()['Account']}:deliverystream/{firehose_name}"
        )
        print(f"CloudWatch Logs subscription filter created for {log_group_name}")
    except ClientError as e:
        print(f"Error creating CloudWatch Logs subscription filter: {e}")

def collect_ami_details(session, region):
    """Collect AMI details using AWS Systems Manager."""
    ssm = session.client('ssm', region_name=region)
    try:
        response = ssm.get_parameter(Name='/aws/service/ami-amazon-linux-latest/amzn2-ami-hvm-x86_64-gp2')
        ami_id = response['Parameter']['Value']
        ec2 = session.client('ec2', region_name=region)
        ami_details = ec2.describe_images(ImageIds=[ami_id])
        print(f"Collected AMI details for {ami_id}")
        return ami_details
    except ClientError as e:
        print(f"Error collecting AMI details: {e}")
        return None

def gather_service_quotas(session, region):
    """Gather service quota information."""
    quotas = session.client('service-quotas', region_name=region)
    try:
        response = quotas.list_service_quotas(ServiceCode='ec2')
        print("Gathered service quota information")
        return response['Quotas']
    except ClientError as e:
        print(f"Error gathering service quota information: {e}")
        return None

def setup_cost_usage_reports(session, bucket_name):
    """Set up AWS Cost and Usage Reports to deliver data to S3."""
    cur = session.client('cur')
    try:
        cur.put_report_definition(
            ReportDefinition={
                'ReportName': 'CostUsageReport',
                'TimeUnit': 'HOURLY',
                'Format': 'Parquet',
                'Compression': 'Parquet',
                'AdditionalSchemaElements': ['RESOURCE_ID'],
                'S3Bucket': bucket_name,
                'S3Prefix': 'cost-usage-reports/',
                'S3Region': session.region_name,
                'AdditionalArtifacts': ['REDSHIFT'],
                'RefreshClosedReports': True,
                'ReportVersioning': 'OVERWRITE_REPORT'
            }
        )
        print("Set up AWS Cost and Usage Reports")
    except ClientError as e:
        print(f"Error setting up AWS Cost and Usage Reports: {e}")

def create_redshift_copy_command(session, bucket_name, redshift_table_name):
    """Create a Redshift COPY command to load CUR data from S3."""
    account_id = session.client('sts').get_caller_identity()['Account']
    region = session.region_name
    copy_command = f"""
    COPY {redshift_table_name}
    FROM 's3://{bucket_name}/cost-usage-reports'
    IAM_ROLE 'arn:aws:iam::{account_id}:role/RedshiftCopyRole'
    FORMAT AS PARQUET;
    """
    return copy_command

def setup_aws_config_pipeline(regions, bucket_name, firehose_name, redshift_cluster_jdbc_url, redshift_table_name, redshift_username, redshift_password):
    """Set up the complete AWS Config to Database pipeline."""
    session = boto3.Session()
    schema = get_database_schema()
    execute_sql_query(schema)

    for region in regions:
        enable_aws_config(session, region)
        create_streaming_delivery_channel(session, region, bucket_name, firehose_name)
        
        # Set up CloudWatch Logs subscription
        setup_cloudwatch_logs_subscription(session, region, '/aws/lambda/example-function', firehose_name)
        
        # Collect AMI details
        ami_details = collect_ami_details(session, region)
        
        # Gather service quotas
        service_quotas = gather_service_quotas(session, region)

    # Create Firehose delivery stream in a single region (e.g., the first region in the list)
    firehose_arn = create_firehose_delivery_stream(session, regions[0], firehose_name, redshift_cluster_jdbc_url, redshift_table_name, redshift_username, redshift_password)

    # Set up Cost and Usage Reports
    setup_cost_usage_reports(session, bucket_name)

    # Create Redshift COPY command for CUR data
    copy_command = create_redshift_copy_command(session, bucket_name, 'cost_usage_reports')
    print("Redshift COPY command for CUR data:")
    print(copy_command)

    if firehose_arn:
        print("AWS Config to Database pipeline setup completed successfully.")
    else:
        print("Failed to set up AWS Config to Database pipeline.")

if __name__ == "__main__":
    # Replace these with your actual values
    regions = ['us-west-2', 'us-east-1']  # Add all regions you want to enable
    bucket_name = 'your-s3-bucket-name'
    firehose_name = 'your-firehose-delivery-stream-name'
    redshift_cluster_jdbc_url = 'jdbc:redshift://your-cluster.redshift.amazonaws.com:5439/dev'
    redshift_table_name = 'aws_config_data'
    redshift_username = 'your_redshift_username'
    redshift_password = 'your_redshift_password'

    setup_aws_config_pipeline(regions, bucket_name, firehose_name, redshift_cluster_jdbc_url, redshift_table_name, redshift_username, redshift_password)