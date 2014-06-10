(ns views.db.load
  (:require
   [clojure.java.jdbc :as j]
   [honeysql.core :as hsql]))

(defn view-query
  "Takes db and query-fn (compiled HoneySQL hash-map)
   and runs the query, returning results."
  [db query-map]
  (j/query db (hsql/format query-map)))

(defn post-process-result-set
  [nv templates result-set]
  (if-let [post-fn (get-in templates [(first nv) :post-fn])]
    (mapv post-fn result-set)
    result-set))

(defn initial-views
  "Takes a db spec, the new views sigs (new-views) we want to produce result-sets for,
   the template config map, and the subscribed-views themselves (with compiled view-maps)
   and returns a result-set for the new-views with post-fn functions applied to the data."
  [db new-views templates subscribed-views]
  (reduce
   (fn [results nv]
     (->> (get subscribed-views nv)
          :view
          (view-query db)
          (into [])
          (post-process-result-set nv templates)
          (assoc results nv)))
   {}
   new-views))
