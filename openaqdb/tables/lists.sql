
CREATE SEQUENCE IF NOT EXISTS lists_sq START 1;

CREATE TYPE visibility_type AS ENUM (
    'private'
    , 'public unlisted'
);


-- users_id is the single lists owner
CREATE TABLE IF NOT EXISTS lists (
    lists_id int PRIMARY KEY DEFAULT nextval('lists_sq')
    , users_id int NOT NULL REFERENCES users
    , label text NOT NULL
    , description text
    , visibility visibility_type NOT NULL DEFAULT 'private'
);

CREATE SEQUENCE IF NOT EXISTS sensor_nodes_lists_sq START 1;

-- multiple sensor nodes can be added to a single list
CREATE TABLE IF NOT EXISTS sensor_nodes_list (
    sensor_nodes_lists_id int PRIMARY KEY DEFAULT nextval('sensor_nodes_lists_sq')
    , sensor_nodes_id int NOT NULL REFERENCES sensor_nodes
    , lists_id int NOT NULL REFERENCES lists
    , description text NOT NULL
    , UNIQUE(sensor_nodes_id, lists_id)
);


CREATE TYPE role_type AS ENUM (
    , 'editor'
    , 'viewer'
);

CREATE SEQUENCE IF NOT EXISTS users_lists_sq START 1;

-- allows non-owners to be added to lists 
CREATE TABLE IF NOT EXISTS users_lists (
    users_lists_id int PRIMARY KEY DEFAULT nextval('users_lists_sq')
    , users_id int NOT NULL REFERENCES users
    , lists_id int NOT NULL REFERENCES lists
    , role role_type NOT NULL default 'viewer'
    , UNIQUE(users_id, lists_id)
);