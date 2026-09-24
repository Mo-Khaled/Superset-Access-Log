-- Superset's metadata DB (superset) and its "examples" DB (superset_examples) live on
-- the same Postgres instance but as separate logical databases, keeping example/demo
-- data cleanly out of Superset's own operational tables while staying entirely inside
-- Superset's own infrastructure (never touching MySQL, which is reserved for the
-- access-log pipeline's output).
CREATE DATABASE superset_examples;
