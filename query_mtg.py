from ollama import Client
import numpy as np
import sys
import duckdb
from duckdb.typing import VARCHAR

"""
PROMPT="destroy a creature and draw cards"
EMB=`curl -s 'http://ollama.ollama:11434/api/embed' -X POST -d "{\"model\":\"bge-m3:latest\",\"input\":\"${PROMPT}\"}" | jq .embeddings[0]`
duckdb -c ".mode column" -c " \
  ATTACH 'host=postgresql-475245.user-gferey dbname=vector user=mtg password=mtg' AS vector(TYPE postgres); \
  FROM 's3://gferey/mtg/cards.parquet' AS cards \
  JOIN vector.mtg_emb AS emb ON emb.uuid = cards.uuid \
  SELECT \
    cards.name, \
    cards.text, \
    CAST(emb.embedding as FLOAT[1024]) <-> $EMB as dst \
  ORDER BY dst \
  LIMIT 10"
"""

client = Client(host='http://ollama.ollama.svc.cluster.local:11434')

conn = duckdb.connect()
conn.create_function(
    "embed",
    lambda prompt: np.array(client.embed(model='bge-m3:latest', input=prompt).embeddings[0]),
    [VARCHAR],
    'FLOAT[1024]'
)
conn.execute("ATTACH 'dbname=vector' AS vector(TYPE postgres);")

for name, txt, sim in conn.execute("""
    FROM 's3://gferey/mtg/cards.parquet' AS cards
    JOIN vector.mtg_emb AS emb ON emb.uuid = cards.uuid
    CROSS JOIN (SELECT embed($q) AS emb_vec) AS q_emb
    SELECT cards.name, cards.text,
        CAST(emb.embedding as FLOAT[1024]) <-> q_emb.emb_vec AS dst
    ORDER BY dst LIMIT $limit
        """, {
          'q': sys.argv[1],
          'limit': sys.argv[2] if len(sys.argv) > 2 else 5
        }).fetchall():
    print(f"  - [{sim:.3f}] {name}: {txt}")
