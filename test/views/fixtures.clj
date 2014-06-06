(ns views.fixtures
  (:require
   [environ.core :as e]
   [clojure.java.jdbc :as j]
   [honeysql.core :as hsql]
   [clojure.data.generators :as dg]))

;; CREATE ROLE views_user LOGIN PASSWORD 'password';
;; CREATE DATABASE views_test OWNER views_user;

(defn sql-ts
  ([ts] (java.sql.Timestamp. ts))
  ([] (java.sql.Timestamp. (.getTime (java.util.Date.)))))

(def db {:classname   "org.postgresql.Driver"
         :subprotocol "postgresql"
         :subname     (get :views-test-db e/env "//localhost/views_test")
         :user        (get :views-test-user e/env "views_user")
         :password    (get :views-test-ppassword e/env "password")})

(defn users-table-fixture!
  []
  (j/execute! db ["CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL, created_on DATE NOT NULL)"]))

(defn posts-table-fixture!
  []
  (j/execute! db ["CREATE TABLE posts (id SERIAL PRIMARY KEY,
                                       title TEXT NOT NULL,
                                       body TEXT NOT NULL,
                                       created_on DATE NOT NULL,
                                       user_id INTEGER NOT NULL,
                                       FOREIGN KEY (user_id) REFERENCES users(id))"]))

(defn drop-tables!
  [tables]
  (doseq [t tables]
    (j/execute! db [(str "DROP TABLE " (name t))])))

(defn database-fixtures!
  [f]
  (users-table-fixture!)
  (posts-table-fixture!)
  (f)
  (drop-tables! [:posts :users]))

(defn user-fixture!
  [name]
  (j/execute! db (hsql/format (hsql/build :insert-into :users :values [{:name name :created_on (sql-ts)}]))))

(defn gen-n-users!
  [n]
  (dotimes [n n]
    (user-fixture! (dg/string #(rand-nth (seq "abcdefghijklmnopqrstuwvxyz")))))
  (j/query db ["SELECT * FROM users"]))
