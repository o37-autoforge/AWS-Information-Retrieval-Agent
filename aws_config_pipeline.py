import boto3
import json
from botocore.exceptions import ClientError

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

def setup_aws_config_pipeline(regions, bucket_name, firehose_name, redshift_cluster_jdbc_url, redshift_table_name, redshift_username, redshift_password):
    """Set up the complete AWS Config to Database pipeline."""
    session = boto3.Session()

    for region in regions:
        enable_aws_config(session, region)
        create_streaming_delivery_channel(session, region, bucket_name, firehose_name)

    # Create Firehose delivery stream in a single region (e.g., the first region in the list)
    firehose_arn = create_firehose_delivery_stream(session, regions[0], firehose_name, redshift_cluster_jdbc_url, redshift_table_name, redshift_username, redshift_password)

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