(ns views.fixtures
  (:require
   [environ.core :as e]
   [clojure.java.jdbc :as j]
   [honeysql.core :as hsql]
   [clojure.data.generators :as dg]))

(defn sql-ts
  ([ts] (java.sql.Timestamp. ts))
  ([] (java.sql.Timestamp. (.getTime (java.util.Date.)))))

(def db {:classname   "org.postgresql.Driver"
         :subprotocol "postgresql"
         :subname     (get :views-test-db e/env "//localhost/views_test")
         :user        (get :views-test-user e/env "views_user")
         :password    (get :views-test-ppassword e/env "password")})

(defn clean-tables!
  [tables]
  (doseq [t (map name tables)]
    (j/execute! db [(str "DELETE FROM " t)])))

(defn database-fixtures!
  [f]
  (clean-tables! [:posts :users])
  (f))

(defn user-fixture!
  [name]
  (j/execute! db (hsql/format (hsql/build :insert-into :users :values [{:name name :created_on (sql-ts)}]))))

(defn gen-n-users!
  [n]
  (dotimes [n n]
    (user-fixture! (dg/string #(rand-nth (seq "abcdefghijklmnopqrstuwvxyz")))))
  (j/query db ["SELECT * FROM users"]))

(defn users-tmpl
  []
  (hsql/build :select [:id :name :created_on] :from :users))

(defn user-posts-tmpl
  [user_id]
  (hsql/build :select [:u.id :u.name :p.title :p.body :p.created_on]
              :from {:posts :p}
              :join [[:users :u][:= :u.id :p.user_id]]
              :where [:= :p.user_id user_id]))

(def templates
  {:users      {:fn #'users-tmpl}
   :user-posts {:fn #'user-posts-tmpl}})
