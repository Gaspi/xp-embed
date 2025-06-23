from ollama import Client
import numpy as np
import sys
import duckdb
from duckdb.typing import VARCHAR

client = Client(host='http://ollama.ollama.svc.cluster.local:11434')
def embed(prompt: str) -> np.ndarray:
    return np.array(client.embed(model='bge-m3:latest', input=prompt).embeddings[0])

conn = duckdb.connect()
conn.create_function("embed", embed, [VARCHAR], 'FLOAT[1024]')
conn.execute("ATTACH 'dbname=vector' AS vector(TYPE postgres);")

for name, txt, sim in conn.execute("""
            FROM 's3://gferey/mtg/cards.parquet' AS cards
            JOIN vector.mtg_emb AS emb ON emb.uuid = cards.uuid
            SELECT
              cards.name,
              cards.text,
              CAST(emb.embedding as FLOAT[1024]) <-> embed($q) AS dst
            ORDER BY dst LIMIT 5
        """,
        {'q': sys.argv[1]}).fetchall():
    print(f"  - {name} ({sim}): {txt}")
    print()
