(ns views.db.core
  (:import
   [java.sql SQLException BatchUpdateException]
   [org.postgresql.util PSQLException])
  (:require
   [clojure.string :refer [trim split]]
   [honeysql.core :as hsql]
   [honeysql.helpers :as hh]
   [honeysql.format :as fmt]
   [honeysql.types :as ht]
   [clojure.java.jdbc :as j]
   [clojure.tools.logging :refer [debug]]
   [views.honeysql :as vh]
   [views.subscribed-views :refer [subscribed-views broadcast-deltas]]))

(defn get-primary-key
  "Get a primary key for a table."
  [schema table]
  (or
    (keyword (get-in schema [(name table) :primary-key :column_name]))
    (throw (Exception. (str "Cannot find primary key for table: " table)))))

;;
;; Takes the HoneySQL template for a view and the arglist
;; and compiles the view with a set of dummy args in the
;; format
;;   [?1, ?2, ?3 ... ?N]
;;
;; Returns a map of the compiled hash-map and the args
;; with keys :dummy-view and :dummy-args respectively.
;;
(defn- compile-dummy-view
  [view-template args]
  (let [dummy-args (take (count args) (range))
        dummy-args (map #(str "?" %) dummy-args)]
    {:dummy-view (apply view-template dummy-args)
     :dummy-args dummy-args}))

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
;; view-check      - SQL for checking whether or not a view needs to receive deltas
;;                   upon completion of an operation.
;;

(defn view-map
  "Constructs a view map from a HoneySQL view function and its arguments.
   Contains four fields:
    :view          - the hash-map with interpolated parameters
    :view-sig      - the \"signature\" for the view, i.e. [:matter 1]
    :args          - the arguments passed in.
    :tables        - the tables present in all :from, :insert-into,
                 :update, :delete-from, :join, :left-join :right-join clauses

   Input is a view template function and a view signature. The template
   function must take the same number of paramters as the signature and
   return a honeysql data structure "
  [view-template view-sig]
  (let [compiled-view (if (> (count view-sig) 1)
                        (apply view-template (rest view-sig))
                        (view-template))]
    (merge {:args          (rest view-sig)
            :view-sig      view-sig
            :view          compiled-view
            :tables        (set (vh/extract-tables compiled-view))}
           (compile-dummy-view view-template (rest view-sig)))))

(defn view-sig->view-map
  "Takes a map of sig keys to view template function vars (templates)
   and a view signature (view-sig the key for the template map and its args)
   and returns a view-map for that view-sig."
  [templates view-sig]
  (let [lookup (first view-sig)]
    (view-map (get-in templates [lookup :fn]) view-sig)))

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
(defn- perform-action-and-return-deltas
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


(defn do-view-transaction
  "Takes the following arguments:
   schema    - from edl.core/defschema
   db        - clojure.java.jdbc database connection
   all-views - the current set of views (view-maps--see view-map fn docstring for
                  description) in memory for the database
   action    - the HoneySQL pre-SQL hash-map with parameters already interpolated.

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

   The function returns the views which received delta updates with the deltas
   keyed to each view-map at the keys :insert-deltas and :delete-deltas."
  [schema db all-views action]
  ;; Every update connected with a view is done in a transaction:
  (j/with-db-transaction [t db :isolation :serializable]
    (let [need-deltas       (do-view-pre-checks t all-views action)
          need-deltas       (map #(generate-view-delta-map % action) need-deltas)
          table             (-> action vh/extract-tables ffirst)
          pkey              (get-primary-key schema table)]
      (perform-action-and-return-deltas schema t need-deltas action table pkey))))

;;
;; Need to catch this and retry:
;; java.sql.SQLException: ERROR: could not serialize access due to concurrent update
;;
(defn get-nested-exceptions*
  [exceptions e]
  (if-let [next-e (.getNextException e)]
    (recur (conj exceptions next-e) next-e)
    exceptions))

(defn get-nested-exceptions
  [e]
  (get-nested-exceptions* [e] e))

(defn do-transaction-fn-with-retries
  [transaction-fn]
  (try
    (transaction-fn)
    (catch SQLException e
      ;; http://www.postgresql.org/docs/9.2/static/errcodes-appendix.html
      (debug "Caught exception with error code: " (.getSQLState e))
      (debug "Exception message: " (.getMessage e))
      ;; (debug "stack trace message: " (.printStackTrace e))
      (if (some #(= (.getSQLState %) "40001") (get-nested-exceptions e))
        (do-transaction-fn-with-retries transaction-fn) ;; try it again
        (throw e))))) ;; otherwise rethrow

(defmacro with-view-transaction
  [subscribed-views binding & forms]
  (let [tvar (first binding)]
    `(if (:deltas ~(second binding)) ;; check if we are in a nested transaction
       (let [~tvar ~(second binding)] ~@forms)
       (do-transaction-fn-with-retries
         (fn []
           (let [deltas# (atom [])
                 result# (j/with-db-transaction [t# ~(second binding) :isolation :serializable]
                                                (let [~tvar (assoc t# :deltas deltas#)]
                                                  ~@forms))]
               (broadcast-deltas ~subscribed-views ~(second binding) @deltas#)
               result#))))))

(defn vaction!
  "Used to perform arbitrary insert/update/delete actions on the database,
   while ensuring that view deltas are appropriately checked and calculated
   for the currently registered views as reported by a type implementing
   the SubscribedViews protocol.

   Arguments are:

   - schema: an edl schema (\"(defschema my-schema ...)\")

   - db: a clojure.java.jdbc database

   - action-map: the HoneySQL map for the insert/update/delete action

   - subscribed-views: an implementation of SubscribedViews implementing
                       the follow functions:

     - get-subscribed-views takes a database connection. It should return
       a collection of view-maps.

     - send-deltas takes a db connection, and the views which have had deltas
       calculate for them and associated with the hash-maps (appropriately
       called views-with-deltas)."
  [schema db action-map subscribed-views]
  (let [subbed-views   (subscribed-views subscribed-views db)
        transaction-fn #(do-view-transaction schema db subbed-views action-map)]
    (if-let [deltas (:deltas db)]  ;; inside a transaction we just collect deltas and do not retry
      (let [{:keys [views-with-deltas result-set]} (transaction-fn)]
        (swap! deltas into views-with-deltas)
        result-set)
      (let [{:keys [views-with-deltas result-set]} (do-transaction-fn-with-retries transaction-fn)]
        (broadcast-deltas subscribed-views db views-with-deltas)
        result-set))))
