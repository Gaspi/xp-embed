import sys
import csv
import psycopg
from pgvector.psycopg import register_vector
from ollama import Client
from itertools import batched
from datetime import datetime

csv.field_size_limit(sys.maxsize)

client = Client(host='http://ollama.ollama.svc.cluster.local:11434')

conn = psycopg.connect(dbname="vector", autocommit=True)
register_vector(conn)


# https://raw.githubusercontent.com/benjihillard/English-Dictionary-Database/refs/heads/main/english%20Dictionary.csv
with open("data/english_dictionary.csv") as csvfile:
    all_nouns = [
        (w[0], w[2])
        for w in csv.reader(csvfile)
        if w[1] == 'n.'
    ]

nb_nouns = len(all_nouns)
print(f"Indexing {len(all_nouns)} nouns...")
start_time = datetime.now()
for batch in batched(enumerate(all_nouns), 1000):
    with conn.cursor() as cur:
        for (i, (word, definition)) in batch:
            response = client.embed(model='bge-m3:latest', input=f"{word}: {definition}")
            embedding = response.embeddings[0]
            cur.execute("""
                INSERT INTO definition_emb(word, language, embedding)
                VALUES (%s, 'en', %s)
                ON CONFLICT DO NOTHING
                """, (word, embedding)
            )
            if i % 10 == 0:
                remain_time = str((datetime.now() - start_time) * (nb_nouns-i-1) / (i+1)).split('.')[0]
                print(
                    f"{i+1} / {nb_nouns} ({100*(i+1)/nb_nouns:.1f}%) Remains: {remain_time}  ->  {word}",
                    " " * 20,
                    end='\r'
                )
conn.close()
print("Done.")
