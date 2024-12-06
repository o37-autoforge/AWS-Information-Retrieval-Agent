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

def format_results(results):
    """Format the query results for user-friendly presentation."""
    # This is a simple formatting. You may want to enhance this based on your needs.
    return "\n".join([str(row) for row in results])

def natural_language_query(user_query):
    """Process a natural language query and return formatted results."""
    schema = get_database_schema()
    sql_query = generate_sql_query(user_query, schema)
    raw_results = execute_query(sql_query)
    formatted_results = format_results(raw_results)
    return formatted_results

if __name__ == "__main__":
    user_query = input("Enter your query about AWS resources: ")
    results = natural_language_query(user_query)
    print("\nQuery Results:")
    print(results)