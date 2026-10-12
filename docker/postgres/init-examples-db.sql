-- Superset's metadata DB (superset) and its "examples" DB (superset_examples) live on
-- the same Postgres instance but as separate logical databases, keeping example/demo
-- data cleanly out of Superset's own operational tables while staying entirely inside
-- Superset's own infrastructure (the access-log pipeline's output lives in Iceberg, not here).
CREATE DATABASE superset_examples;
