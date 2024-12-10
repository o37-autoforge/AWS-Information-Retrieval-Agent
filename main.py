from aws_config_pipeline import setup_aws_config_pipeline
from nl_query_agent import process_user_query

def main():
    # Set up the AWS Config pipeline
    regions = ['us-west-2', 'us-east-1']
    bucket_name = 'your-s3-bucket-name'
    firehose_name = 'your-firehose-delivery-stream-name'
    redshift_cluster_jdbc_url = 'jdbc:redshift://your-cluster.redshift.amazonaws.com:5439/dev'
    redshift_table_name = 'aws_config_data'
    redshift_username = 'your_redshift_username'
    redshift_password = 'your_redshift_password'

    setup_aws_config_pipeline(regions, bucket_name, firehose_name, redshift_cluster_jdbc_url, redshift_table_name, redshift_username, redshift_password)

    # Process user queries
    while True:
        user_query = input("Enter your query about AWS resources (or 'quit' to exit): ")
        if user_query.lower() == 'quit':
            break
        result = process_user_query(user_query)
        print("\nAnswer:")
        print(result)

if __name__ == "__main__":
    main()