(ns views.db.deltas
  (:require
   [clojure.string :refer [split]]
   [clojure.java.jdbc :as j]
   [honeysql.core :as hsql]
   [honeysql.helpers :as hh]
   [views.db.honeysql :as vh]))

(defn get-primary-key
  "Get a primary key for a table."
  [schema table]
  (or
    (keyword (get-in schema [(name table) :primary-key :column_name]))
    (throw (Exception. (str "Cannot find primary key for table: " table)))))

(defn swap-out-dummy-for-pos
  "Replaces dummy arg like \"?0\" for integer value (0) so we can sort args."
  [dummy-arg]
  (Integer. (subs dummy-arg 1)))

;; Helper for determine-filter-clauses (which is a helper
;; for view-check-template). Extracts constituent parts from
;; where clause.
(defn set-filter-clauses
  [dummy-args fc w]
  (if (= w :and)
    fc
    (if (contains? (set dummy-args) (last w))
      (update-in fc [:s] assoc (swap-out-dummy-for-pos (last w)) (second w))
      (update-in fc [:w] (fnil conj []) w))))

;; Helper for generating the view-check HoneySQL template.
;; Builds the where and select clauses up from constituent
;; where-clauses. Placeholder identifies the parameters
;; to pull out into the select clause.
(defn determine-filter-clauses
  [wc dummy-args]
  (let [fc {:s {} :w nil}
        fc (if (and (not= :and (first wc)) (not (coll? (first wc))))
             (set-filter-clauses dummy-args fc wc)
             (reduce #(set-filter-clauses dummy-args %1 %2) fc wc))]
    (-> fc
        (update-in [:s] #(into [] (vals (sort-by key %))))
        (update-in [:w] #(vh/with-op :and %)))))

(defn append-arg-map
  "Removes table/alias namespacing from select fields and creates a hash-map
   of field to arguments for checking this view against checked-results later on.
   Note that this assumes our select-fields are in the same order as they
   are present in the :args view-map field (which they should be)."
  [view-map select-fields]
  (let [select-fields (map #(-> % name (split #"\.") last keyword) select-fields)]
    (assoc view-map :arg-compare (zipmap select-fields (into [] (:args view-map))))))

(defn- create-view-delta-where-clauses
  [view-map action]
  (let [action-table (first (vh/extract-tables action))]
    (for [view-table (vh/find-table-aliases action-table (:tables view-map))]
      (-> (:where action)
          (vh/prefix-columns (vh/table-alias view-table))
          (vh/replace-table (vh/table-alias action-table) (vh/table-alias view-table))))))

(defn format-action-wc-for-view
  "Takes view-map and action (HoneySQL hash-map for insert/update/delete),
   extracts where clause from action, and formats it with the proper
   alias (or no alias) so that it will work when applied to the view SQL."
  [view-map action]
  (if (:where action)
    (let [preds (create-view-delta-where-clauses view-map action)]
      (if (> (count preds) 1)
        (into [:or] preds)
        (first preds)))))

(defn- update-where-clause
  [hh-spec where]
  (if-let [w (:where where)]
    (assoc hh-spec :where w)
    (dissoc hh-spec :where)))

(defn view-check-template
  "Receives a view-map and an action (insert/update/delete HoneySQL hash-map).
   Returns a HoneySQL hash-map which will can be formatted as SQL to check if a
   view needs to receive deltas for the action SQL."
  [view-map action]
  (let [{:keys [dummy-view dummy-args]} view-map
        fc        (determine-filter-clauses (:where dummy-view) dummy-args)
        action-wc (format-action-wc-for-view view-map action)
        view-map  (append-arg-map view-map (:s fc))] ; we need this to compare *after* the check is run
    (->> (-> dummy-view
             (update-where-clause (vh/merge-where-clauses action-wc (:w fc)))
             (merge (apply hh/select (:s fc))))
         (hash-map :view-map view-map :view-check))))

(defn prepare-checks-for-view-deltas
  "Checks to see if an action has tables related to a view, and
   if so builds the HoneySQL hash-map for the SQL needed.
   Uses this hash-map as a key and conj's the view-map to the key's
   value so as to avoid redundant delta-check querying."
  [action confirmed-views view-map]
  ;; Confirm if any of the tables in view-map are present in the action template:
  (if (some (set (map first (vh/extract-tables action)))
            (map first (:tables view-map)))

    ;; Then construct the check-template for this particular view.
    (if-let [{:keys [view-check view-map]} (view-check-template view-map action)]
      ;; We then use the view-check as an index and conj the
      ;; view-map to it so as to avoid redundant checks.
      (update-in confirmed-views [view-check] #(conj % view-map))
      confirmed-views)
    confirmed-views))

(defn prepare-view-checks
  "Prepares checks for a collection of views (view-maps) against a HoneySQL action
   (insert/update/delete) hash-map.

   Returns a structure like so:
     {{<computed HoneySQL hash-map for the check SQL}
      [<collection of all views this check hash-map key applies to]}
  "
  [view-maps action]
  (reduce #(prepare-checks-for-view-deltas action %1 %2) {} view-maps))

(defn- do-check
  [db check-template]
  (j/query db (hsql/format check-template)))

(defn- check-view-args
  [checked-results view-map]
  (let [view-args (:arg-compare view-map)]
    (reduce
     (fn [hit cr]
       (if (seq (filter #(= (% cr) (% view-args)) (keys view-args)))
         (reduced view-map) ; don't care which args, just whether or not the view-map hit
         hit))
     nil
     checked-results)))

(defn- check-all-view-args
  [checked-results views]
  (->> views
       (map #(check-view-args checked-results %))
       (remove nil?)
       distinct))

(defn- do-view-pre-check
  [db needs-deltas view-check]
  ;;
  ;; We have empty-select? if we have a view with no where predicate clauses--
  ;; so it will always require deltas if there are matching tables.
  ;;
  ;; empty-where comes about if we are inserting--we don't have any where predicate
  ;; in the insert, of course, so we can't perform pre-checks reliably.
  ;; When we do an insert we have to simply do the delta query regardless, for now.
  ;;
  (let [empty-select? (seq (remove nil? (:select (first view-check))))
        empty-where?  (seq (remove #(or (nil? %) (= :and %)) (:where (first view-check))))]
    (if (or (not empty-select?) (not empty-where?))
      (apply conj needs-deltas (last view-check)) ;; put them all in if we can't do pre-check.
      (let [checked-results (do-check db (first view-check))
            ;; checks view args against checked result set
            checked-views   (check-all-view-args checked-results (last view-check))]
        (if (seq checked-views)
          (apply conj needs-deltas checked-views)
          needs-deltas)))))

(defn do-view-pre-checks
  "Takes db, all views (view-maps) and the HoneySQL action (insert/update/delete)
   hash-map.  Returns view-maps for all the views which need to receive
   delta updates after the action is performed.

   *This function should be called within a transaction before performing the
   insert/update/delete action.*"
  [db all-views action]
  (let [view-checks (prepare-view-checks all-views action)]
    (reduce #(do-view-pre-check db %1 %2) [] view-checks)))

(defn- calculate-delete-deltas
  [db view-map]
  (->> (:delete-deltas-map view-map)
       hsql/format
       (j/query db)
       (assoc view-map :delete-deltas)))

;; -------------------------------------------------------------------------------
;; Handle inserts
;; -------------------------------------------------------------------------------

(defn compute-delete-deltas-for-insert
  "Computes and returns a sequence of delete deltas for a single view and insert."
  [schema db view-map table record]
  (if (vh/outer-join-table? (:view view-map) table)
    (let [delta-q (vh/outer-join-delta-query schema (:view view-map) table record)]
      (j/query db (hsql/format delta-q)))
    []))

(defn primary-key-predicate
  "Return a predicate for a where clause that constrains to the primary key of
  the record."
  [schema table record]
  (let [pkey (get-primary-key schema table)]
    [:= pkey (pkey record)]))

(defn compute-insert-deltas-for-insert
  [schema db view-map table record]
  (let [pkey-pred        (primary-key-predicate schema table record)
        action           (hsql/build :insert-into table :values [record] :where pkey-pred)
        insert-delta-wc  (format-action-wc-for-view view-map action)
        view             (:view view-map)
        insert-delta-map (update-in view [:where] #(:where (vh/merge-where-clauses insert-delta-wc %)))]
    (j/query db (hsql/format insert-delta-map))))

(defn compute-insert-delete-deltas-for-views
  [schema db views table record]
  (doall (map #(compute-delete-deltas-for-insert schema db % table record) views)))

(defn compute-insert-insert-deltas-for-views
  [schema db views table record]
  (doall (map #(compute-insert-deltas-for-insert schema db % table record) views)))

(defn compute-deltas-for-insert
  "This takes a *single* insert and a view, applies the insert and computes
  the view deltas."
  [schema db views table record]
  (let [deletes (compute-insert-delete-deltas-for-views schema db views table record)
        record* (first (j/insert! db table record))
        inserts (compute-insert-insert-deltas-for-views schema db views table record*)]
    {:views-with-deltas (doall (map #(assoc %1 :delete-deltas %2 :insert-deltas %3) views deletes inserts))
     :result record*}))

;; Handles insert and calculation of insert (after insert) delta.
(defn- insert-and-append-deltas!
  [schema db views action table pkey]
  (let [table (:insert-into action)]
    (reduce
     #(-> %1
          (update-in [:views-with-deltas] into (:views-with-deltas %2))
          (update-in [:result-set] conj (:result %2)))
     {:views-with-deltas [] :result-set []}
     (map #(compute-deltas-for-insert schema db views table %) (:values action)))))

;; -------------------------------------------------------------------------------

;; This is for insert deltas for non-insert updates.

;;; Takes the HoneySQL map (at key :view) from the view-map and appends
;;; the appropriately-table-namespaced where clause which limits the
;;; view query to the previously inserted or updated records.
(defn- calculate-insert-deltas
  [db action pkey-wc view-map]
  (let [action           (assoc action :where pkey-wc)
        insert-delta-wc  (format-action-wc-for-view view-map action)
        view             (:view view-map)
        insert-delta-map (update-in view [:where] #(:where (vh/merge-where-clauses insert-delta-wc %)))
        deltas           (j/query db (hsql/format insert-delta-map))]
    (if (seq deltas)
      (update-in view-map [:insert-deltas] #(apply conj % deltas))
      view-map)))

;; Helper to query the action's table for primary key and pull it out.
(defn- get-action-row-key
  [db pkey table action]
  (->> (:where action)
       (hsql/build :select pkey :from table :where)
       hsql/format
       (j/query db)
       first pkey))

;; Handles update and calculation of delete (before update) and insert (after update) deltas.
(defn- update-and-append-deltas!
  [db views action table pkey]
  (let [views-pre (doall (map #(calculate-delete-deltas db %) views))
        pkey-val  (get-action-row-key db pkey table action)
        update    (j/execute! db (hsql/format action))]
    {:views-with-deltas (doall (map #(calculate-insert-deltas db action [:= pkey pkey-val] %) views-pre))
     :result-set        update}))

;; Handles deletion and calculation of delete (before update) delta.
(defn- delete-and-append-deltas!
  [db views action table pkey]
  (let [views-pre (doall (map #(calculate-delete-deltas db %) views))]
    {:views-with-deltas views-pre
     :result-set        (j/execute! db (hsql/format action))}))

;; Identifies which action--insert, update or delete--we are performing and dispatches appropriately.
;; Returns view-map with appropriate deltas appended.
(defn perform-action-and-return-deltas
  [schema db views action table pkey]
  (cond
   (:insert-into action) (insert-and-append-deltas! schema db views action table pkey)
   (:update action)      (update-and-append-deltas! db views action table pkey)
   (:delete-from action) (delete-and-append-deltas! db views action table pkey)
   :else (throw (Exception. "Received malformed action: " action))))

(defn generate-view-delta-map
  "Adds a HoneySQL hash-map for the delta-calculation specific to the view + action.
   Takes a view-map and the action HoneySQL hash-map, and appends the action's
   where clause to the view's where clause, and adds in new field :insert-deltas-map."
  [view-map action]
  (let [action-wc (format-action-wc-for-view view-map action)
        view      (:view view-map)]
    (->> (update-in view [:where] #(:where (vh/merge-where-clauses action-wc %)))
         (assoc view-map :delete-deltas-map))))
