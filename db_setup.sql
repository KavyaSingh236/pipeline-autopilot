-- Run this ONE TIME in pgAdmin as the postgres superuser
-- Step 1: create user + db
CREATE USER pipeline WITH PASSWORD 'pipeline';
CREATE DATABASE pipeline OWNER pipeline;

-- Step 2: connect to `pipeline` db in pgAdmin, then run the rest
-- (In pgAdmin: right-click pipeline db → Query Tool → paste below → Run)

-- The app auto-creates all tables on first start via asyncpg.
-- This file is just to create the user and database.
-- Nothing else needed!
