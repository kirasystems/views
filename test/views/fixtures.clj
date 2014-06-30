(ns views.fixtures
  (:require
   [environ.core :as e]
   [clojure.java.jdbc :as j]
   [honeysql.core :as hsql]
   [edl.core :refer [defschema]]
   [clojure.data.generators :as dg]))

(defn sql-ts
  ([ts] (java.sql.Timestamp. ts))
  ([] (java.sql.Timestamp. (.getTime (java.util.Date.)))))

(def db {:classname   "org.postgresql.Driver"
         :subprotocol "postgresql"
         :subname     (get :views-test-db e/env "//localhost/views_test")
         :user        (get :views-test-user e/env "views_user")
         :password    (get :views-test-ppassword e/env "password")})

(defschema vschema db "public")

(defn clean-tables!
  [tables]
  (doseq [t (map name tables)]
    (j/execute! db [(str "DELETE FROM " t)])))

(defn database-fixtures!
  [f]
  (clean-tables! [:posts :users :comments])
  (f))

(defn rand-str
  [l]
  (dg/string #(rand-nth (seq "abcdefghijklmnopqrstuwvxyz ")) l))

(defn view-query
  [view]
  (j/query db (hsql/format view)))

(defn view-action!
  [action]
  (j/execute! db (hsql/format action)))

(defn user-fixture!
  [name]
  (view-action! (hsql/build :insert-into :users :values [{:name name :created_on (sql-ts)}])))

(defn gen-n-users!
  [n]
  (dotimes [n n] (user-fixture! (rand-str 10)))
  (j/query db ["SELECT * FROM users"]))

(defn insert-post-tmpl
  [uid title body]
  (hsql/build :insert-into :posts :values [{:user_id uid :title title :body body :created_on (sql-ts)}]))

(defn post-fixture!
  [uid title body]
  (view-action! (insert-post-tmpl uid title body)))

(defn gen-n-posts-for-user!
  [n uid]
  (dotimes [n n] (post-fixture! uid (rand-str 20) (rand-str 100))))

(defn users-tmpl
  []
  (hsql/build :select [:id :name :created_on] :from :users))

(defn user-posts-tmpl
  [user_id]
  (hsql/build :select [:u.id :u.name :p.title :p.body :p.created_on]
              :from {:posts :p}
              :join [[:users :u][:= :u.id :p.user_id]]
              :where [:= :p.user_id user_id]))

(defn users-posts-tmpl
  []
  (hsql/build :select [[:u.id :user_id] :u.name :p.id :p.title :p.body :p.created_on]
              :from {:users :u}
              :left-join [[:posts :p][:= :u.id :p.user_id]]))

(defn all-comments-tmpl
  []
  (hsql/build :select [:id :body :created_on] :from {:comments :c}))

(def templates
  {:users        {:fn #'users-tmpl}
   :user-posts   {:fn #'user-posts-tmpl}
   :all-comments {:fn #'all-comments-tmpl}})
