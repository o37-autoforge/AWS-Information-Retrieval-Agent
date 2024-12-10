import openai
import psycopg2
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up OpenAI API key
openai.api_key = os.getenv("OPENAI_API_KEY")

def translate_user_query_to_sql(user_query, schema):
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
        max_tokens=150
    )

    return response.choices[0].text.strip()

def execute_sql_query(sql_query):
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
    # Convert raw data to a string format that Gemini can understand
    return "\n".join([str(row) for row in raw_data])

def generate_gemini_prompt(user_query, formatted_data):
    return f"""
    User Query: {user_query}

    Retrieved Data:
    {formatted_data}

    Based on the user's query and the retrieved data, provide a comprehensive and accurate answer.
    """

def query_gemini(gemini_prompt):
    response = openai.Completion.create(
        engine="text-davinci-002",
        prompt=gemini_prompt,
        max_tokens=300
    )
    return response.choices[0].text.strip()

def process_user_query(user_query, schema):
    # Step 1: Translate user query to SQL
    sql_query = translate_user_query_to_sql(user_query, schema)
    
    # Step 2: Execute SQL query and retrieve data
    raw_data = execute_sql_query(sql_query)
    
    # Step 3: Format retrieved data
    formatted_data = format_data_for_gemini(raw_data)
    
    # Step 4: Generate Gemini prompt
    gemini_prompt = generate_gemini_prompt(user_query, formatted_data)
    
    # Step 5: Send prompt to Gemini and get response
    final_answer = query_gemini(gemini_prompt)
    
    return final_answer

# Example usage
if __name__ == "__main__":
    schema = """
    CREATE TABLE aws_config_resources (
        resource_id VARCHAR(255),
        resource_type VARCHAR(50),
        region VARCHAR(20),
        configuration JSON,
        tags JSON,
        capture_time TIMESTAMP
    );
    """
    user_query = "Show me all EC2 instances in the us-west-2 region"
    result = process_user_query(user_query, schema)
    print(result)