from openai import OpenAI
import os
 
 
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN")
 
MODEL_NAME = "databricks-claude-sonnet-4-5"
BASE_URL = "https://dbc-cde67c9e-0ad1.cloud.databricks.com/serving-endpoints"
 
INPUT_SQL_FILE = "input.sql"
OUTPUT_SQL_FILE = "converted_databricks.sql"
 
CHUNK_SIZE = 8000   # size per chunk
 
 
 
client = OpenAI(
    api_key=DATABRICKS_TOKEN,
    base_url=BASE_URL
)
 
 
 
def read_sql():
    with open(INPUT_SQL_FILE, "r", encoding="utf-8") as f:
        return f.read()
 
 
def split_into_chunks(text, chunk_size):
    chunks = []
    start = 0
    while start < len(text):
        end = min(start + chunk_size, len(text))
        chunks.append(text[start:end])
        start = end
    return chunks
 
 
 
def call_llm(chunk_text):
    response = client.chat.completions.create(
        model=MODEL_NAME,
        messages=[
            {
                "role": "user",
                "content": f"""
Convert the following SQL Server script into Databricks SQL.
Return only the converted SQL, no explanation.
 
--- INPUT CHUNK START ---
{chunk_text}
--- INPUT CHUNK END ---
"""
            }
        ],
        max_tokens=5000
    )
 
    return response.choices[0].message.content
 
 
def process_and_write(chunks):
    # clear output file first
    with open(OUTPUT_SQL_FILE, "w", encoding="utf-8") as f:
        f.write("-- Converted Databricks SQL\n\n")
 
    for idx, chunk in enumerate(chunks):
        print(f"Processing chunk {idx+1}/{len(chunks)}...")
 
        converted_sql = call_llm(chunk)
 
        # append output chunk to file
        with open(OUTPUT_SQL_FILE, "a", encoding="utf-8") as f:
            f.write(converted_sql)
            f.write("\n\n-- END OF CHUNK {}\n\n".format(idx+1))
 
 
 
if __name__ == "__main__":
    print("Reading input SQL...")
    sql_text = read_sql()
 
    print("Splitting into chunks...")
    chunks = split_into_chunks(sql_text, CHUNK_SIZE)
    print(f"Total chunks: {len(chunks)}")
 
    print("Converting chunks via LLM and writing output...")
    process_and_write(chunks)
 
    print(f"✅ DONE! Output written to: {OUTPUT_SQL_FILE}")
 
 
