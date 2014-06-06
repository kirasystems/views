(ns views.db.core-test
  (:require
   [clojure.test :refer [use-fixtures deftest is]]
   [honeysql.core :as hsql]
   [views.fixtures :as vf :refer [gen-n-users! database-fixtures!]]
   [views.db.core :as vdb]
   [clojure.string :refer [upper-case]]))

(use-fixtures :each database-fixtures!)

(defn users-tmpl
  []
  (hsql/build :select [:id :name :created_on] :from :users))

(defn user-posts-tmpl
  [user_id]
  (hsql/build :select [:u.user_id :u.name :p.title :p.body :p.created_on]
              :from {:posts :p}
              :join [[:users :u][:= :user_id user_id]]))

(def templates
  {:users      {:fn #'users-tmpl}
   :user-posts {:fn #'user-posts-tmpl}})

(defn subscribed-views
  []
  {[:users] {:view-map ((get-in templates [:users :fn]))}})

(deftest initializes-views
  (let [users (gen-n-users! 2)]
    (is (= (vdb/initial-views vf/db [[:users]] templates (subscribed-views))
           {[:users] users}))))

(deftest post-processes-views
  (let [users       (gen-n-users! 1)
        with-postfn (assoc-in templates [:users :post-fn] #(update-in % [:name] upper-case))
        views-rs    (vdb/initial-views vf/db [[:users]] with-postfn (subscribed-views))]
    (is (= (-> (get views-rs [:users]) first :name)
           (-> users first :name upper-case)))))
