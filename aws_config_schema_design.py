def define_aws_config_schema():
    schema = {
        "table_name": "aws_config_resources",
        "columns": [
            {"name": "resource_id", "type": "VARCHAR(255)", "description": "Unique identifier for the resource."},
            {"name": "resource_type", "type": "VARCHAR(50)", "description": "Type of AWS resource (e.g., EC2, S3)."},
            {"name": "region", "type": "VARCHAR(20)", "description": "AWS region where the resource is located."},
            {"name": "configuration", "type": "JSON", "description": "AWS Config resource configuration."},
            {"name": "tags", "type": "JSON", "description": "Tags applied to the resource."},
            {"name": "capture_time", "type": "TIMESTAMP", "description": "Time when the configuration was captured."}
        ]
    }
    return schema

def generate_create_table_sql(schema):
    table_name = schema["table_name"]
    columns = schema["columns"]
    
    column_definitions = ",\n    ".join([f"{col['name']} {col['type']}" for col in columns])
    
    create_table_sql = f"""
    CREATE TABLE {table_name} (
        {column_definitions}
    );
    """
    
    return create_table_sql

def create_index_sql(schema):
    table_name = schema["table_name"]
    return f"""
    CREATE INDEX idx_resource_id ON {table_name} (resource_id);
    CREATE INDEX idx_resource_type ON {table_name} (resource_type);
    CREATE INDEX idx_region ON {table_name} (region);
    CREATE INDEX idx_capture_time ON {table_name} (capture_time);
    """

def main():
    schema = define_aws_config_schema()
    create_table_sql = generate_create_table_sql(schema)
    index_sql = create_index_sql(schema)
    
    print("SQL to create the table:")
    print(create_table_sql)
    print("\nSQL to create indexes:")
    print(index_sql)

if __name__ == "__main__":
    main()