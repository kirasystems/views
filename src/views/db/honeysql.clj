(ns views.db.honeysql
  (:require
    [honeysql.core :as hsql]
    [honeysql.helpers :as hh]
    [clojure.string :refer [split]]))

(def table-clauses
  [:from :insert-into :update :delete-from :join :left-join :right-join])

;; This list doesn't support custom operators. Is there something else we can do?
(def pred-ops
  #{:= :< :> :<> :>= :<= :in :between :match :ltree-match :and :or :not= :like :xor :regexp :not-in :not-like
    :!= :is :is-not})

(defn process-complex-clause
  [tables clause]
  (reduce
    #(if (coll? %2)
      (if (some pred-ops [(first %2)])
        %1
        (conj %1 %2))
      (conj %1 [%2]))
    tables
    clause))

(defn extract-tables*
  [tables clause]
  (if clause
    (if (coll? clause)
      (process-complex-clause tables clause)
      (conj tables [clause]))
    tables))

(defn extract-tables
  "Extracts a set of table vector from a HoneySQL spec hash-map.
   Each vector either contains a single table keyword, or the
   table keyword and an alias keyword."
  ([hh-spec] (extract-tables hh-spec table-clauses))
  ([hh-spec clauses] (reduce #(extract-tables* %1 (%2 hh-spec)) #{} clauses)))

;; The following is used for full refresh views where we can have CTEs and
;; subselects in play.

(declare query-tables)

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
  (if-let [v (:insert-into query)] [v] []))

(defn update-tables
  [query]
  (if-let [v (:update query)] [v] []))

(defn delete-tables
  [query]
  (if-let [v (:delete-from query)] [v] []))

(defn query-tables
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

(defn with-op
  "Takes a collection of things and returns either an nary op of them, or
  the item in the collection if there is only one."
  [op coll]
  (if (> (count coll) 1) (into [op] coll) (first coll)))

(defn find-table-aliases
  "Returns the table alias for the supplied table."
  [action-table tables]
  (filter #(= (first action-table) (first %)) tables))

(defn outer-join-table?
  "Return true if table is used in an outer join in the honeysql expression."
  [hh-spec table]
  (let [tables (map first (extract-tables hh-spec [:left-join :right-join]))]
    (boolean (some #(= table %) tables))))

(defn prefix-column
  "Prefixes a column with an alias."
  [column alias]
  (keyword (str (name alias) \. (name column))))

(defn create-null-constraints
  "Create 'is null' constraints for all the columns of a table."
  [schema table table-alias]
  (let [columns (map #(prefix-column % table-alias) (keys (:columns (get schema (name table)))))]
    (into [:and] (for [c columns] [:= c nil]))))

(defn table-alias
  "Returns the name of a table or its alias. E.g. for [:table :t] returns :t."
  [table]
  (if (keyword? table) table (last table)))

(defn table-name
  "Returns the name of a table . E.g. for [:table :t] returns :table."
  [table]
  (if (keyword? table) table (first table)))

(defn table-column
  "Assumes that table columns are keywords of the form :table.column. Returns
  the column as a keyword or nil if the supplied keyword doesn't match the pattern."
  [table item]
  (let [s (name item), t (str (name table) \.)]
    (if (.startsWith s t) (keyword (subs s (count t))))))

(defn modified-outer-join-predicate
  "Returns an outer join predicate with the join tables columns subistituted
   with values from a record."
  [table predicate record]
  (if-let [column (and (keyword? predicate) (table-column table predicate))]
    (or (get record column)
        (throw (Exception. (str "No value for column " column " in " record))))
    (if (vector? predicate)
      (apply vector (map #(modified-outer-join-predicate table % record) predicate))
      predicate)))

(defn find-outer-joins
  "Find and return all the outer joins on a given table."
  [hh-spec table]
  (->> (concat (:left-join hh-spec) (:right-join hh-spec))
    (partition 2 2)
    (filter #(= table (table-name (first %))))))

(defn- create-outer-join-predicates
  "Create outer join predicate from a record and joins."
  [schema table record joins]
  (->> joins
    (map (fn [[table-spec join-pred]]
           [:and
            (modified-outer-join-predicate (table-alias table-spec) join-pred record)
            (create-null-constraints schema table (table-alias table-spec))]))
    (with-op :or)))

(defn outer-join-delta-query
  "Create an outer join delta query given a honeysql template and record"
  [schema hh-spec table record]
  (let [join-tables (find-outer-joins hh-spec table)
        join-pred   (create-outer-join-predicates schema table record join-tables)]
    (assert (not (nil? join-pred)))
    (update-in hh-spec [:where] #(vector :and % join-pred))))

(defn merge-where-clauses
  "Takes two where clauses from two different HoneySQL maps and joins then with an and.
   If one is nil, returns only the non-nil where clause."
  [wc1 wc2]
  (if (and wc1 wc2)
    (hh/where wc1 wc2)
    (hh/where (or wc1 wc2))))

(defn replace-table
  "Replace all instances of table name t1 pred with t2."
  [pred t1 t2]
  (if-let [column (and (keyword? pred) (table-column t1 pred))]
    (keyword (str (name t2) \. (name column)))
    (if (coll? pred)
      (map #(replace-table % t1 t2) pred)
      pred)))

(defn unprefixed-column?
  [c]
  (and (keyword? c) (not (pred-ops c)) (neg? (.indexOf (name c) (int \.)))))

(defn prefix-columns
  "Prefix all unprefixed columns with table."
  [pred table]
  (if (unprefixed-column? pred)
    (keyword (str (name table) \. (name pred)))
    (if (coll? pred)
      (map #(prefix-columns % table) pred)
      pred)))
