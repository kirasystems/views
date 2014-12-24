(ns views.db.deltas
  (:import (java.sql SQLException))
  (:require
   [clojure.string :refer [split]]
   [clojure.java.jdbc :as j]
   [clojure.tools.logging :refer [debug error]]
   [honeysql.core :as hsql]
   [honeysql.helpers :as hh]
   [views.db.load :as vdbl]
   [views.db.checks :as vc]
   [views.db.honeysql :as vh]
   [views.db.util :refer [log-exception serialization-error?]]))

;;
;; Terminology and data structures used throughout this code
;;
;; <name>-template - refers to a function which receives parameters
;;                   and returns a HoneySQL hash-map with params interpolated.
;;
;; action          - describes the HoneySQL hash-map for the action to be performed
;;                   --the template function has already been called and returned this
;;                   with the appropriate parameter arguments.
;;
;; view-map        - contains a set of computed information for each view itself.
;;                   Refer to the view-map doc-string for more information.
;;

(defn view-map
  "Constructs a view map from a HoneySQL view function and its arguments.
   Contains four fields:
    :view          - the hash-map with interpolated parameters
    :view-sig      - the \"signature\" for the view, i.e. [:matter 1]
    :tables        - the tables present in all :from, :insert-into,
                 :update, :delete-from, :join, :left-join :right-join clauses

   Input is a view template function and a view signature. The template
   function must take the same number of paramters as the signature and
   return a honeysql data structure "
  [view-template view-sig]
  (let [compiled-view (if (> (count view-sig) 1)
                        (apply view-template (rest view-sig))
                        (view-template))]
    {:view-sig      view-sig
     :view          compiled-view
     :refresh-only? (:refresh-only (meta view-template))}))

(defn view-sig->view-map
  "Takes a map of sig keys to view template function vars (templates)
   and a view signature (view-sig the key for the template map and its args)
   and returns a view-map for that view-sig."
  [templates view-sig]
  (let [lookup (first view-sig)]
    (view-map (get-in templates [lookup :fn]) view-sig)))

;; Helpers

(defn get-primary-key
  "Get a primary key for a table."
  [schema table]
  (or
    (keyword (get-in schema [(name table) :primary-key :column_name]))
    (throw (Exception. (str "Cannot find primary key for table: " table)))))

(defn- create-view-delta-where-clauses
  [view-map action]
  (let [action-table (first (vh/extract-tables action))
        view-tables  (vh/extract-tables (:view view-map))]
    (for [view-table (vh/find-table-aliases action-table view-tables)]
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

;; DELTA CALCULATIONS

(defn- calculate-delete-deltas
  [db view-map]
  (try
    (->> (:delete-deltas-map view-map)
         hsql/format
         (j/query db)
         (assoc view-map :delete-deltas))
    (catch Exception e
      (error "computing delete deltas for" view-map)
      (log-exception e)
      (throw e))))

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
  (mapv #(compute-delete-deltas-for-insert schema db % table record) views))

(defn compute-insert-insert-deltas-for-views
  [schema db views table record]
  (mapv #(compute-insert-deltas-for-insert schema db % table record) views))

(defn compute-deltas-for-insert
  "This takes a *single* insert and a view, applies the insert and computes
  the view deltas."
  [schema db views table record]
  (let [deletes (compute-insert-delete-deltas-for-views schema db views table record)
        record* (first (j/insert! db table record))
        inserts (compute-insert-insert-deltas-for-views schema db views table record*)]
    {:views-with-deltas (doall (map #(assoc %1 :delete-deltas %2 :insert-deltas %3) views deletes inserts))
     :result record*}))

(defn- insert-and-append-deltas!
  "Handles insert and calculation of insert (after insert) delta."
  [schema db views action table pkey]
  (let [table (:insert-into action)]
    (reduce
     #(-> %1
          (update-in [:views-with-deltas] into (:views-with-deltas %2))
          (update-in [:result-set] conj (:result %2)))
     {:views-with-deltas [] :result-set []}
     (map #(compute-deltas-for-insert schema db views table %) (:values action)))))

(defn- calculate-insert-deltas
  "This is for insert deltas for non-insert updates.

  Takes the HoneySQL map (at key :view) from the view-map and appends
  the appropriately-table-namespaced where clause which limits the
  view query to the previously inserted or updated records."
  [db action pkey-wc view-map]
  (let [action           (assoc action :where pkey-wc)
        insert-delta-wc  (format-action-wc-for-view view-map action)
        view             (:view view-map)
        insert-delta-map (update-in view [:where] #(:where (vh/merge-where-clauses insert-delta-wc %)))
        deltas           (j/query db (hsql/format insert-delta-map))]
    (if (seq deltas)
      (update-in view-map [:insert-deltas] #(apply conj % deltas))
      view-map)))

(defn- get-action-row-key
  "Helper to query the action's table for primary key and pull it out."
  [db pkey table action]
  (->> (:where action)
       (hsql/build :select pkey :from table :where)
       hsql/format
       (j/query db)
       first pkey))

(defn- update-and-append-deltas!
  "Handles update and calculation of delete (before update) and insert (after update) deltas."
  [db views action table pkey]
  (let [views-pre (mapv #(calculate-delete-deltas db %) views)
        pkey-val  (get-action-row-key db pkey table action)
        update    (j/execute! db (hsql/format action))]
    {:views-with-deltas (mapv #(calculate-insert-deltas db action [:= pkey pkey-val] %) views-pre)
     :result-set        update}))

(defn- delete-and-append-deltas!
  "Handles deletion and calculation of delete (before update) delta."
  [db views action table pkey]
  (let [views-pre (mapv #(calculate-delete-deltas db %) views)]
    {:views-with-deltas views-pre
     :result-set        (j/execute! db (hsql/format action))}))

(defn perform-action-and-return-deltas
  "Identifies which action--insert, update or delete--we are performing and dispatches appropriately.
  Returns view-map with appropriate deltas appended."
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

(defn update-deltas-with-refresh-set
  [refresh-set]
  (fn [view-deltas]
    (if (coll? view-deltas)
      (map #(assoc % :refresh-set refresh-set) view-deltas)
      [{:refresh-set refresh-set}])))

(defn calculate-refresh-sets
  "For refresh-only views, calculates the refresh-set and adds it to the view's delta update collection."
  [deltas db templates refresh-only-views]
  (reduce
     (fn [d {:keys [view-sig view] :as rov}]
       (try
         (let [refresh-set (get (vdbl/initial-view db view-sig templates view) view-sig)]
           (update-in d [view-sig] (update-deltas-with-refresh-set refresh-set)))
         ;; report bad view-sig on error
         (catch Exception e
           (error "error refreshing view" view-sig)
           (log-exception e)
           (throw e))))
     deltas
     refresh-only-views))

(defn format-deltas
  "Removes extraneous data from view delta response collections.
  TODO: Is there only one delta pack per view-sig here?"
  [views-with-deltas]
  (reduce #(update-in %1 [(:view-sig %2)] (fnil conj []) (select-keys %2 [:delete-deltas :insert-deltas :refresh-set]))
          {} views-with-deltas))

(defn do-view-transaction
  "Takes the following arguments:
   schema    - from edl.core/defschema
   db        - clojure.java.jdbc database connection
   all-views - the current set of views (view-maps--see view-map fn docstring for
                  description) in memory for the database
   action    - the HoneySQL pre-SQL hash-map with parameters already interpolated.
   templates - the mapping of view names (keywords) to SQL templates
                  (a.k.a. HoneySQL hash-map producing functions)

   The function will then perform the following sequence of actions, all run
   within a transaction (with isolation serializable):

   1) Create pre-check SQL for each view in the list.
   2) Run the pre-check SQL (or fail out based on some simple heuristics) to
      identify if we want to send delta messages to the view's subscribers
      (Note: this happens after the database action for *inserts only*).
   3) Run the database action (insert/action/delete).
   4) Calculate deltas based on the method described in section 5.4, \"Rule Generation\"
      of the paper \"Deriving Production Rules for Incremental Rule Maintenance\"
      by Stefano Ceri and Jennifer Widom (http://ilpubs.stanford.edu:8090/8/1/1991-4.pdf)

   The function returns a hash-map with :result-set and :new-deltas collection values.
   :new-deltas contains :insert-deltas, :delete-deltas, and :refresh-set values, as well
   as the original :view-sig the deltas apply to."
  [schema db all-views action templates]
  (j/with-db-transaction [t db :isolation :serializable]
    (let [filtered-views     (filterv #(vc/have-overlapping-tables? action (:view %)) all-views)
          {full-refresh-views true normal-views nil} (group-by :refresh-only? filtered-views)
          need-deltas        (map #(generate-view-delta-map % action) normal-views)
          table              (-> action vh/extract-tables ffirst)
          pkey               (get-primary-key schema table)
          {:keys [views-with-deltas result-set]} (perform-action-and-return-deltas schema t need-deltas action table pkey)
          deltas             (calculate-refresh-sets (format-deltas views-with-deltas) t templates full-refresh-views)]
      {:new-deltas deltas :result-set result-set})))
