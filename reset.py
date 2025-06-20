
import psycopg
import os
import sys

DATABASE_NAME: str = 'vector'
EMBEDDING_LENGTH: int = 1024
GRANTEE: str = os.getenv('PGUSER')

if len(sys.argv) > 1 and sys.argv[1] == 'database':
    reset_conn = psycopg.connect(user="postgres", autocommit=True)
    with reset_conn.cursor() as reset_cur:
        reset_cur.execute(f"""
            SELECT pg_terminate_backend(pid)
            FROM pg_stat_activity
            WHERE datname = '{DATABASE_NAME}'
            AND pid <> pg_backend_pid();
        """)
        reset_cur.execute(f"DROP DATABASE IF EXISTS \"{DATABASE_NAME}\";")
        reset_cur.execute(f"CREATE DATABASE \"{DATABASE_NAME}\";")
        reset_cur.execute(f"GRANT CONNECT ON DATABASE \"{DATABASE_NAME}\" TO \"{GRANTEE}\";")
    reset_conn.commit()
    reset_conn.close()

conn = psycopg.connect(user="postgres", dbname=DATABASE_NAME, autocommit=True)
with conn.cursor() as cur:
    if len(sys.argv) > 2 and sys.argv[1] == 'table':
        cur.execute(f'DROP TABLE "{sys.argv[2]}";')
    cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
    # Create word_emb table
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS word_emb (
            word varchar(30) unique,
            language varchar(2),
            embedding vector({EMBEDDING_LENGTH})
        );
        """)
    # Create definition_emb table
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS definition_emb (
            word varchar(30) unique,
            language varchar(2),
            embedding vector({EMBEDDING_LENGTH})
        );
        """)
    # Create dictionnary_emb table
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS rmes_emb (
            pageid varchar(5) unique,
            embedding vector({EMBEDDING_LENGTH})
        );
        """)
    # Create dictionnary_emb table
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS mtg_emb (
            uuid UUID PRIMARY KEY,
            name varchar(800),
            embedding vector({EMBEDDING_LENGTH})
        );
        """)
    # Grant rights
    cur.execute(f"GRANT USAGE ON SCHEMA public TO \"{GRANTEE}\";")
    cur.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO \"{GRANTEE}\";")
    cur.execute(f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO \"{GRANTEE}\";")
conn.close()
