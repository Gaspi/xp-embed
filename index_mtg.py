import duckdb
import psycopg
from pgvector.psycopg import register_vector
from ollama import Client
from itertools import batched

from utils import ProgressPrinter

client = Client(host='http://ollama.ollama.svc.cluster.local:11434')

conn = psycopg.connect(dbname="vector", autocommit=True)
register_vector(conn)

print("Loading data from s3 file...")
data = duckdb.query("""
    SELECT
      name,
      first(uuid ORDER BY length(text) DESC, uuid) as uuid,
      first(text ORDER BY length(text) DESC, uuid) as text
    FROM 's3://gferey/mtg/cards.parquet'
    WHERE language = 'English'
      AND text IS NOT NULL
      AND length(text) > 0
    GROUP BY name;
      """).fetchall()
print(f"Done. Loaded {len(data)} cards.")

pp = ProgressPrinter(size=len(data))
for batch in batched(enumerate(data), 2000):
    with conn.cursor() as cur:
        for (name, uuid, text) in data:
            response = client.embed(model='bge-m3:latest', input=f"[{name}] {text}")
            embedding = response.embeddings[0]
            cur.execute("""
                INSERT INTO mtg_emb(uuid, name, embedding)
                VALUES (%s, %s, %s)
                """, (uuid, name, embedding)
            )
            pp.step(name)
