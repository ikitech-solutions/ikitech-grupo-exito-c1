-- Create Marquez database and user
CREATE DATABASE marquez;
CREATE USER marquez WITH ENCRYPTED PASSWORD 'marquez';
GRANT ALL PRIVILEGES ON DATABASE marquez TO marquez;

-- Connect to marquez database
\c marquez;

-- Grant schema permissions
GRANT ALL ON SCHEMA public TO marquez;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO marquez;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO marquez;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO marquez;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO marquez;
