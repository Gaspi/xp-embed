from ollama import Client
import numpy as np
import sys
import os
import duckdb
from duckdb.typing import VARCHAR

"""
LLM_TOKEN="get token from OpenWebUI service"
PROMPT="destroy a creature and draw cards"
EMB=`curl -s 'https://llm.lab.sspcloud.fr/ollama/api/embed' -X POST -H "Authorization: Bearer ${LLM_TOKEN}" -H "Content-Type: application/json" -d "{\"model\":\"bge-m3:latest\",\"input\":\"${PROMPT}\"}" | jq .embeddings[0]`
duckdb -c ".mode column" -c " \
  ATTACH 'host=postgresql-475245.user-gferey dbname=vector user=mtg password=mtg' AS vector(TYPE postgres); \
  FROM 's3://gferey/mtg/cards.parquet' AS cards \
  JOIN vector.mtg_emb AS emb ON emb.uuid = cards.uuid \
  SELECT cards.name, cards.text \
  ORDER BY CAST(emb.base_embedding as FLOAT[1024]) <-> $EMB LIMIT 10"
"""

client = Client(
  host='https://llm.lab.sspcloud.fr/ollama',
  headers={
    "Authorization": f"Bearer {os.getenv('LLM_TOKEN')}"
  }
)

conn = duckdb.connect()
conn.create_function(
    "embed",
    lambda prompt: np.array(client.embed(model='bge-m3:latest', input=prompt).embeddings[0]),
    [VARCHAR],
    'FLOAT[1024]'
)
conn.execute("ATTACH 'dbname=vector' AS vector(TYPE postgres);")

print("Description search")
for name, txt, sim in conn.execute("""
    FROM 's3://gferey/mtg/cards.parquet' AS cards
    JOIN vector.mtg_emb AS emb ON emb.uuid = cards.uuid
    CROSS JOIN (SELECT embed($q) AS emb_vec) AS q_emb
    SELECT cards.name, cards.text,
        CAST(emb.base_embedding as FLOAT[1024]) <-> q_emb.emb_vec AS dst
    ORDER BY dst
    LIMIT $limit
        """, {
          'q': sys.argv[1],
          'limit': sys.argv[2] if len(sys.argv) > 2 else 5
        }).fetchall():
    print(f"  - [{sim:.3f}] {name}: {txt}")


print()
print("Extended description search")
for name, txt, sim in conn.execute("""
    FROM 's3://gferey/mtg/cards.parquet' AS cards
    JOIN vector.mtg_emb AS emb ON emb.uuid = cards.uuid
    CROSS JOIN (SELECT embed($q) AS emb_vec) AS q_emb
    SELECT cards.name, cards.text,
        CAST(emb.full_embedding as FLOAT[1024]) <-> q_emb.emb_vec AS dst
    ORDER BY dst LIMIT $limit
        """, {
          'q': sys.argv[1],
          'limit': sys.argv[2] if len(sys.argv) > 2 else 5
        }).fetchall():
    print(f"  - [{sim:.3f}] {name}: {txt}")


    # emb = np.array(emb)
    # print("Words closest to that article:")
    # words = conn.execute("""
    #     SELECT word, language, CAST(emb.embedding as FLOAT[1024]) <-> $emb as dst
    #     FROM vector.word_emb as emb
    #     ORDER BY dst
    #     LIMIT 10""",
    #     {
    #       "emb": emb,
    #     }).fetchall()
    # for word, language, dst in words:
    #     print(f"- [{language}] ({dst:.3f}) {word}")

    # print("Definitions closest to that article:")
    # defs = conn.execute("""
    #     SELECT word, language, CAST(emb.embedding as FLOAT[1024]) <-> $emb as dst
    #     FROM vector.definition_emb as emb
    #     ORDER BY dst
    #     LIMIT 10""",
    #     {
    #       "emb": emb,
    #     }).fetchall()
    # for word, language, dst in defs:
    #     print(f"- [{language}] ({dst:.3f}) {word}")


