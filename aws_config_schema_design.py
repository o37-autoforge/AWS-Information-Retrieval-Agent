def define_extended_schema():
    schemas = {
        "aws_config_resources": [
            {"name": "resource_id", "type": "VARCHAR(255)", "description": "Unique identifier for the resource."},
            {"name": "resource_type", "type": "VARCHAR(50)", "description": "Type of AWS resource (e.g., EC2, S3)."},
            {"name": "region", "type": "VARCHAR(20)", "description": "AWS region where the resource is located."},
            {"name": "configuration", "type": "JSON", "description": "AWS Config resource configuration."},
            {"name": "tags", "type": "JSON", "description": "Tags applied to the resource."},
            {"name": "capture_time", "type": "TIMESTAMP", "description": "Time when the configuration was captured."}
        ],
        "cloudwatch_logs": [
            {"name": "log_group", "type": "VARCHAR(255)", "description": "Name of the CloudWatch log group."},
            {"name": "log_stream", "type": "VARCHAR(255)", "description": "Name of the CloudWatch log stream."},
            {"name": "timestamp", "type": "TIMESTAMP", "description": "Timestamp of the log event."},
            {"name": "message", "type": "TEXT", "description": "Log message content."}
        ],
        "ami_details": [
            {"name": "ami_id", "type": "VARCHAR(255)", "description": "Unique identifier for the AMI."},
            {"name": "name", "type": "VARCHAR(255)", "description": "Name of the AMI."},
            {"name": "description", "type": "TEXT", "description": "Description of the AMI."},
            {"name": "creation_date", "type": "TIMESTAMP", "description": "Creation date of the AMI."},
            {"name": "owner_id", "type": "VARCHAR(255)", "description": "Owner ID of the AMI."}
        ],
        "quota_details": [
            {"name": "service", "type": "VARCHAR(255)", "description": "Name of the AWS service."},
            {"name": "quota_name", "type": "VARCHAR(255)", "description": "Name of the quota."},
            {"name": "quota_value", "type": "FLOAT", "description": "Value of the quota."},
            {"name": "used", "type": "FLOAT", "description": "Used value of the quota."},
            {"name": "unit", "type": "VARCHAR(50)", "description": "Unit of the quota."}
        ],
        "service_limits": [
            {"name": "service", "type": "VARCHAR(255)", "description": "Name of the AWS service."},
            {"name": "limit_name", "type": "VARCHAR(255)", "description": "Name of the service limit."},
            {"name": "limit_value", "type": "FLOAT", "description": "Value of the service limit."},
            {"name": "unit", "type": "VARCHAR(50)", "description": "Unit of the service limit."}
        ],
        "cost_usage_reports": [
            {"name": "time_period", "type": "VARCHAR(50)", "description": "Time period for the cost report."},
            {"name": "service", "type": "VARCHAR(255)", "description": "Name of the AWS service."},
            {"name": "cost", "type": "FLOAT", "description": "Cost incurred for the service."},
            {"name": "usage", "type": "FLOAT", "description": "Usage amount for the service."},
            {"name": "unit", "type": "VARCHAR(50)", "description": "Unit of the usage."}
        ]
    }
    return schemas

def generate_create_table_sql(schemas):
    sql_statements = {}
    for table_name, columns in schemas.items():
        column_definitions = ",\n    ".join([f"{col['name']} {col['type']}" for col in columns])
        create_table_sql = f"""
    CREATE TABLE {table_name} (
        {column_definitions}
    );
    """
        sql_statements[table_name] = create_table_sql
    return sql_statements

def create_index_sql(schemas):
    index_statements = {}
    for table_name, columns in schemas.items():
        indexes = []
        for col in columns:
            if col['type'].startswith('VARCHAR') or col['type'] == 'TIMESTAMP':
                indexes.append(f"CREATE INDEX idx_{table_name}_{col['name']} ON {table_name} ({col['name']});")
        index_statements[table_name] = "\n    ".join(indexes)
    return index_statements

def main():
    schemas = define_extended_schema()
    create_table_sql = generate_create_table_sql(schemas)
    index_sql = create_index_sql(schemas)
    
    print("SQL to create the tables:")
    for table_name, sql in create_table_sql.items():
        print(f"\n{table_name}:")
        print(sql)
    
    print("\nSQL to create indexes:")
    for table_name, sql in index_sql.items():
        print(f"\n{table_name}:")
        print(sql)

if __name__ == "__main__":
    main()