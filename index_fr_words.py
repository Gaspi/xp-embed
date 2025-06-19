import requests
import zipfile
import io
import fireducks.pandas as pd
import psycopg
from pgvector.psycopg import register_vector
from ollama import Client
from itertools import batched
from datetime import datetime


url = "http://www.lexique.org/databases/Lexique383/Lexique383.zip"
response = requests.get(url)
with zipfile.ZipFile(io.BytesIO(response.content)) as z:
    with z.open("Lexique383.tsv") as f:
        df = pd.read_csv(f, sep="\t")
all_nouns = df[(df.cgram == "NOM") & (df.nombre == 's')].lemme

client = Client(host='http://ollama.ollama.svc.cluster.local:11434')

conn = psycopg.connect(dbname="vector", autocommit=True)
register_vector(conn)


nb_nouns = len(all_nouns)
print(f"Indexing {len(all_nouns)} fr nouns...")
start_time = datetime.now()
for batch in batched(enumerate(all_nouns), 1000):
    with conn.cursor() as cur:
        for (i, word) in batch:
            response = client.embed(model='bge-m3:latest', input=f"{word}")
            embedding = response.embeddings[0]
            cur.execute("""
                INSERT INTO word_emb(word, language, embedding)
                VALUES (%s, 'fr', %s)
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
