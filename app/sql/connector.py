import os
from databricks import sql
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    return sql.connect(
        server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
        http_path=os.getenv("DATABRICKS_HTTP_PATH"),
        access_token=os.getenv("DATABRICKS_TOKEN")
    )

def run_query(sql_string: str) -> list[dict]:
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql_string)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]
