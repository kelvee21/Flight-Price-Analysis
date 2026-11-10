SELECT format(
    'CREATE ROLE %I LOGIN PASSWORD %L',
    :'postgres_user',
    :'postgres_password'
)
WHERE NOT EXISTS (
    SELECT FROM pg_roles WHERE rolname = :'postgres_user'
)\gexec

SELECT format(
    'ALTER ROLE %I WITH LOGIN PASSWORD %L',
    :'postgres_user',
    :'postgres_password'
)\gexec

SELECT format('CREATE DATABASE %I OWNER %I', :'postgres_db', :'postgres_user')
WHERE NOT EXISTS (
    SELECT FROM pg_database WHERE datname = :'postgres_db'
)\gexec

ALTER DATABASE :"postgres_db" OWNER TO :"postgres_user";
\connect :postgres_db
GRANT USAGE, CREATE ON SCHEMA public TO :"postgres_user";