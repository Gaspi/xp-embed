import psycopg
from pgvector.psycopg import register_vector
from ollama import Client
import numpy as np

client = Client(host='http://ollama.ollama.svc.cluster.local:11434')

conn = psycopg.connect(dbname="vector", autocommit=True)
register_vector(conn)


def embedding_query(query: str, prompt: str):
    response = client.embed(model='bge-m3:latest', input=prompt)
    embedding = np.array(response.embeddings[0])
    return conn.execute(query, (embedding,)).fetchall()


def table_query(table: str, prompt: str):
    if table == "rmes":
        query = "SELECT pageid, embedding <-> %s as dst FROM rmes_emb ORDER BY dst LIMIT 5"
    else:
        query = "SELECT word, embedding <-> %s as dst FROM dictionnary_emb ORDER BY dst LIMIT 5"
    return embedding_query(query, prompt)


if __name__ == '__main__':
    import sys
    print(f"Table: {sys.argv[1]}")
    print(f"Query: {sys.argv[2]}")
    print(table_query(sys.argv[1], sys.argv[2]))
conn.close()
