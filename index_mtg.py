import duckdb
import psycopg
from pgvector.psycopg import register_vector
from ollama import Client
from itertools import batched

from utils import ProgressPrinter

client = Client(host='http://ollama.ollama.svc.cluster.local:11434')

conn = psycopg.connect(dbname="vector", autocommit=True)
register_vector(conn)

ddb = duckdb.connect()
ddb.execute("ATTACH 'dbname=vector' AS vector(TYPE postgres);")

print("Loading data from s3 file...")
data = ddb.query("""
      FROM 's3://gferey/mtg/cards.parquet' AS cards
      ANTI JOIN vector.mtg_emb AS emb ON emb.name = cards.name
      SELECT name,
        first(type      ORDER BY length(text) DESC, uuid) as type,
        first(manaCost  ORDER BY length(text) DESC, uuid) as manaCost,
        first(manaValue ORDER BY length(text) DESC, uuid) as manaValue,
        first(uuid      ORDER BY length(text) DESC, uuid) as uuid,
        first(text      ORDER BY length(text) DESC, uuid) as text
      WHERE language = 'English'
        AND text IS NOT NULL
        AND length(text) > 0
      GROUP BY name""").fetchall()
print(f"Done. Loaded {len(data)} cards.")

pp = ProgressPrinter(size=len(data))
for batch in batched(enumerate(data), 2000):
    with conn.cursor() as cur:
        for (name, cardType, manaCost, manaValue, uuid, text) in data:
            base_prompt = f"[{name}] {text}"
            full_prompt = f"""
                The tabletop card game Magic: The Gathering by Wizards of the Coast has a card with the name \"{name}\".
                This card has type \"{cardType}\" and costs {manaCost} to play (total mana cost: {manaValue}).
                It has the following description: {text}"""
            base_embedding = client.embed(model='bge-m3:latest', input=base_prompt).embeddings[0]
            full_embedding = client.embed(model='bge-m3:latest', input=full_prompt).embeddings[0]
            cur.execute("""
                INSERT INTO mtg_emb(uuid, name, base_embedding, full_embedding)
                VALUES (%s, %s, %s, %s)
                """, (uuid, name, base_embedding, full_embedding)
            )
            pp.step(name)

