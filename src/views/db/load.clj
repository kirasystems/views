(ns views.db.load
  (:import
   [java.sql SQLException])
  (:require
   [clojure.tools.logging :refer [debug info warn error]]
   [clojure.java.jdbc :as j]
   [honeysql.core :as hsql]
   [views.db.util :refer [safe-map log-exception serialization-error?]]))

(defn view-query
  "Takes db and query-fn (compiled HoneySQL hash-map)
   and runs the query, returning results."
  [db query-map]
  (j/query db (hsql/format query-map)))

(defn post-process-result-set
  [view-sig templates result-set]
  (if-let [post-fn (get-in templates [(first view-sig) :post-fn])]
    (mapv post-fn result-set)
    result-set))

(defn initial-view
  "Takes a db spec, the new views sigs (new-views) we want to produce result-sets for,
   the template config map, and the view-map itself.
   and returns a result-set for the new-views with post-fn functions applied to the data."
  [db new-view templates view-map]
  (try
    (->> view-map
         (view-query db)
         (into [])
         (post-process-result-set new-view templates)
         (hash-map new-view))
    (catch Exception e
      (error "when computing initial-view for" new-view)
      (log-exception e)
      (throw e))))
