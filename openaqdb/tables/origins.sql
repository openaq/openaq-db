CREATE TABLE origins(
    origin text primary key,
    metadata jsonb default '{}'::jsonb
);