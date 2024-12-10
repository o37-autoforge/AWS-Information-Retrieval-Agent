import openai
import psycopg2
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up OpenAI API key
openai.api_key = os.getenv("OPENAI_API_KEY")

def get_database_schema():
    """Retrieve the database schema."""
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

def generate_sql_query(user_query, schema):
    """Generate SQL query from natural language using GPT-3.5."""
    prompt = f"""
    You are an AI assistant that translates natural language queries about AWS resources into SQL queries.
    The database schema is as follows:
    {schema}

    User query: {user_query}

    Translate the above query into a SQL query that can be executed on the given schema.
    """

    response = openai.Completion.create(
        engine="text-davinci-002",
        prompt=prompt,
        max_tokens=150,
        n=1,
        stop=None,
        temperature=0.7,
    )

    return response.choices[0].text.strip()

def execute_query(sql_query):
    """Execute the SQL query on the Redshift database."""
    conn = psycopg2.connect(
        dbname=os.getenv("REDSHIFT_DB"),
        user=os.getenv("REDSHIFT_USER"),
        password=os.getenv("REDSHIFT_PASSWORD"),
        host=os.getenv("REDSHIFT_HOST"),
        port=os.getenv("REDSHIFT_PORT")
    )
    
    with conn.cursor() as cur:
        cur.execute(sql_query)
        results = cur.fetchall()
    
    conn.close()
    return results

def format_data_for_gemini(raw_data):
    """Format the raw data for Gemini input."""
    return "\n".join([str(row) for row in raw_data])

def generate_gemini_prompt(user_query, formatted_data):
    """Generate a prompt for Gemini based on the user query and formatted data."""
    return f"""
    User Query: {user_query}

    Retrieved Data:
    {formatted_data}

    Based on the user's query and the retrieved data, provide a comprehensive and accurate answer.
    """

def query_gemini(gemini_prompt):
    """Send a prompt to Gemini and get the response."""
    response = openai.Completion.create(
        engine="text-davinci-002",  # Replace with actual Gemini model when available
        prompt=gemini_prompt,
        max_tokens=300,
        n=1,
        stop=None,
        temperature=0.7,
    )
    return response.choices[0].text.strip()

def process_user_query(user_query):
    """Process a user query using SQL generation, data retrieval, and Gemini for final answer."""
    schema = get_database_schema()
    sql_query = generate_sql_query(user_query, schema)
    raw_data = execute_query(sql_query)
    formatted_data = format_data_for_gemini(raw_data)
    gemini_prompt = generate_gemini_prompt(user_query, formatted_data)
    final_answer = query_gemini(gemini_prompt)
    return final_answer

if __name__ == "__main__":
    user_query = input("Enter your query about AWS resources: ")
    result = process_user_query(user_query)
    print("\nAnswer:")
    print(result)