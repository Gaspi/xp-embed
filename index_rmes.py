import psycopg
from pgvector.psycopg import register_vector
from ollama import Client
import polars as pl
from datetime import datetime

client = Client(host='http://ollama.ollama.svc.cluster.local:11434')

conn = psycopg.connect(dbname="vector", autocommit=True)
register_vector(conn)

df = pl.read_parquet(
    "s3://projet-llm-insee-open-data/data/processed_data/rmes_sources_content.parquet",
    columns=["id", "content"]
)

print(f"Indexing {len(df)} rmes articles...")
with conn.cursor() as cur:
    start_time = datetime.now()
    for (i, (pageid, content)) in enumerate(df.iter_rows()):
        response = client.embed(model='bge-m3:latest', input=content)
        embedding = response.embeddings[0]
        cur.execute("""
            INSERT INTO rmes_emb(pageid, embedding)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING
            """,
            (pageid, embedding)
        )
        remain_time = str((datetime.now() - start_time) * (len(df)-i-1) / (i+1)).split('.')[0]
        print(
            f"{i+1} / {len(df)} ({100*(i+1)/len(df):.1f}%) Remains: {remain_time}  ->  {pageid}",
            " " * 20,
            end='\r'
        )
conn.close()
print("Done.")
