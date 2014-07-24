(ns views.db.deltas-test
  (:require
   [clojure.test :refer [use-fixtures deftest is]]
   [honeysql.core :as hsql]
   [honeysql.helpers :as hh]
   [views.fixtures :as vf :refer [vschema sql-ts]]
   [views.db.core :as vdb]
   [views.db.deltas :as vd]))

(defn dvt-helper
  ([all-views action] (dvt-helper all-views action vf/templates))
  ([all-views action templates]
     (vd/do-view-transaction vschema vf/db all-views action templates)))

(use-fixtures :each (vf/database-fixtures!))

(deftest builds-view-map
  (let [{:keys [view-sig view refresh-only?]} (vd/view-map vf/users-tmpl [:users])]
    (is (= view-sig [:users]))
    (is (= view {:from [:users], :select [:id :name :created_on]}))
    (is (nil? refresh-only?))))

(defn non-nil-values-for-keys?
  [hm keys]
  (every? #(% hm) keys))

(deftest calculates-insert-deltas
  (let [views     [(vd/view-map vf/users-tmpl [:users])]
        user-args {:name "Test user" :created_on (sql-ts)}
        insert    (hsql/build :insert-into :users :values [user-args])
        {:keys [new-deltas result-set]} (dvt-helper views insert)
        insert-delta (first (:insert-deltas (first (get new-deltas [:users]))))]

    ;; Result set
    (is (not (nil? (:id (first result-set)))))
    (is (= user-args (dissoc (first result-set) :id)))

    ;; Deltas
    (is (= (:name user-args) (:name insert-delta)))
    (is (= (:created_on user-args) (:created_on insert-delta)))
    (is (non-nil-values-for-keys? insert-delta (-> views first :view :select)))))

(deftest calculates-delete-deltas
  (let [views     [(vd/view-map vf/users-tmpl [:users])]
        user-args {:name "Test user" :created_on (sql-ts)}
        user      (vf/view-action! (hsql/build :insert-into :users :values [user-args]))
        delete    (hsql/build :delete-from :users :where [:= :name (:name user-args)])
        {:keys [new-deltas result-set]} (dvt-helper views delete)
        delete-delta (first (:delete-deltas (first (get new-deltas [:users]))))]

    ;; Deltas
    (is (= (:name user-args) (:name delete-delta)))
    (is (= (:created_on user-args) (:created_on delete-delta)))
    (is (non-nil-values-for-keys? delete-delta (-> views first :view :select)))))

(deftest calculates-update-deltas
  (let [views     [(vd/view-map vf/users-tmpl [:users])]
        user-args {:name "Test user" :created_on (sql-ts)}
        user      (vf/view-action! (hsql/build :insert-into :users :values [user-args]))
        new-name  "new name!"
        update    (hsql/build :update :users :set {:name new-name} :where [:= :name (:name user-args)])
        {:keys [new-deltas result-set]} (dvt-helper views update)
        {:keys [insert-deltas delete-deltas]} (first (get new-deltas [:users]))]

    ;; Deltas
    (is (= (:name user-args) (:name (first delete-deltas))))
    (is (= new-name (:name (first insert-deltas))))))

(deftest does-not-calculate-deltas-for-unrelated-views
  (let [views     [(vd/view-map vf/users-tmpl [:users])
                   (vd/view-map vf/all-comments-tmpl [:all-comments])]
        user-args {:name "Test user" :created_on (sql-ts)}
        insert    (hsql/build :insert-into :users :values [user-args])
        {:keys [new-deltas result-set]} (dvt-helper views insert)]

;;    (is (= (count (insert-deltas new-deltas) 1))
    (is (nil? (get new-deltas [:all-comments])))))
