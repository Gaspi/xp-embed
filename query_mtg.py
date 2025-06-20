import psycopg
from pgvector.psycopg import register_vector
from ollama import Client
import numpy as np
import sys
import duckdb
from duckdb.typing import VARCHAR


client = Client(host='http://ollama.ollama.svc.cluster.local:11434')
prompt = sys.argv[1]
response = client.embed(model='bge-m3:latest', input=prompt)
embedding = np.array(response.embeddings[0])

conn = psycopg.connect(dbname="vector", autocommit=True)
register_vector(conn)
query = "SELECT uuid, embedding <-> %s as dst FROM mtg_emb ORDER BY dst LIMIT 5"
cards = conn.execute(query, (embedding,)).fetchall()
conn.close()


def embed(sentence: str) -> np.ndarray:
    return model.encode(sentence)['dense_vecs']

conn.create_function("embed", embed, [VARCHAR], 'FLOAT[1024]')

print(duckdb.query(f"""
        ATTACH 'dbname=vector' AS vector(TYPE postgres);
        SELECT cards.name, cards.text, embbeddings.dst
        FROM 's3://gferey/mtg/cards.parquet' AS cards
        JOIN (
            SELECT uuid, embedding <-> '{embedding}' as dst
            FROM vector.mtg_emb
            ORDER BY dst
            LIMIT 5
        ) as embbeddings ON embbeddings.uuid = cards.uuid""").fetchall())

