CREATE ROLE views_user LOGIN PASSWORD 'password';
CREATE DATABASE views_test OWNER views_user;
\c postgresql://localhost/views_test;
CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL, created_on DATE NOT NULL);
CREATE TABLE posts (id SERIAL PRIMARY KEY,
                    title TEXT NOT NULL,
                    body TEXT NOT NULL,
                    created_on DATE NOT NULL,
                    user_id INTEGER NOT NULL,
                    FOREIGN KEY (user_id) REFERENCES users(id));
ALTER TABLE users OWNER TO views_user;
ALTER TABLE posts OWNER TO views_user;
