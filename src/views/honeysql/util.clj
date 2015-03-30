(ns views.honeysql.util
  (:require
    [honeysql.core :as hsql]
    [honeysql.helpers :as hh]
    [clojure.string :refer [split]]))
;; The following is used for full refresh views where we can have CTEs and
;; subselects in play.

(declare query-tables)

(defn- first-leaf
  "Retrieves the first leaf in a collection of collections

  (first-leaf :table)                -> :table
  (first-leaf [[:table] [& values]]) -> :table"
  [v]
  (if (coll? v) (recur (first v)) v))

(defn cte-tables
  [query]
  (mapcat #(query-tables (second %)) (:with query)))

(defn isolate-tables
  "Isolates tables from table definitions in from and join clauses."
  [c]
  (if (keyword? c) [c] (let [v (first c)] (if (map? v) (query-tables v) [v]))))

(defn from-tables
  [query]
  (mapcat isolate-tables (:from query)))

(defn every-second
  [coll]
  (map first (partition 2 coll)))

(defn join-tables
  [query k]
  (mapcat isolate-tables (every-second (k query))))

(defn collect-maps
  [wc]
  (cond
    (coll? wc) (let [maps  (filterv map? wc)
                     colls (filter #(and (coll? %) (not (map? %))) wc)]
                 (into maps (mapcat collect-maps colls)))
    (map? wc)  [wc]
    :else      []))

(defn where-tables
  "This search for subqueries in the where clause."
  [query]
  (mapcat query-tables (collect-maps (:where query))))

(defn insert-tables
  [query]
  (some->> query :insert-into first-leaf vector))

(defn update-tables
  [query]
  (if-let [v (:update query)] [v] []))

(defn delete-tables
  [query]
  (if-let [v (:delete-from query)] [v] []))

(defn query-tables
  "Return all the tables in an sql statement."
  [query]
  (set (concat
         (cte-tables query)
         (from-tables query)
         (join-tables query :join)
         (join-tables query :left-join)
         (join-tables query :right-join)
         (where-tables query)
         (insert-tables query)
         (update-tables query)
         (delete-tables query))))
