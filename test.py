import psycopg
from pgvector.psycopg import register_vector
from ollama import Client
import numpy as np
import sys

client = Client(host='http://ollama.ollama.svc.cluster.local:11434')

conn = psycopg.connect(dbname="vector", autocommit=True)
register_vector(conn)


def embedding_query(query: str, prompt: str):
    response = client.embed(model='bge-m3:latest', input=prompt)
    embedding = np.array(response.embeddings[0])
    return conn.execute(query, (embedding,)).fetchall()


prompt = sys.argv[1]

print("Closest article:")
articles = embedding_query("SELECT pageid, embedding FROM rmes_emb ORDER BY embedding <-> %s LIMIT 1", prompt)
(article, emb) = articles[0]
print(f"- https://www.insee.fr/fr/metadonnees/source/serie/{article}")

print("Words closest to that article:")
words = conn.execute("SELECT word, language, embedding <-> %s as dst FROM word_emb ORDER BY dst LIMIT 10", (emb,)).fetchall()
for word, language, dst in words:
    print(f"- [{language}] ({dst:.3f}) {word}")

print("Definitions closest to that article:")
defs = conn.execute("SELECT word, language, embedding <-> %s as dst FROM definition_emb ORDER BY dst LIMIT 10", (emb,)).fetchall()
for word, language, dst in defs:
    print(f"- [{language}] ({dst:.3f}) {word}")

conn.close()
