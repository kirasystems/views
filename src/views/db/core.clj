(ns views.db.core
  (:import
   [java.sql SQLException])
  (:require
   [clojure.java.jdbc :as j]
   [clojure.tools.logging :refer [debug]]
   [views.db.deltas :as vd]
   [views.subscribed-views :refer [subscribed-views broadcast-deltas]]))

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
  [binding & forms]
  (let [tvar (first binding), vc (second binding)]
    `(if (:deltas ~vc) ;; check if we are in a nested transaction
       (let [~tvar ~vc] ~@forms)
       (do-transaction-fn-with-retries
         (fn []
           (let [base-subscribed-views# (:base-subscribed-views ~vc)
                 deltas#  (atom [])
                 result#  (j/with-db-transaction [t# (:db ~vc) :isolation :serializable]
                            (let [~tvar (assoc ~vc :deltas deltas# :db t#)]
                              ~@forms))]
             (broadcast-deltas base-subscribed-views# @deltas# (:namespace ~vc))
             result#))))))

(defn vexec!
  "Used to perform arbitrary insert/update/delete actions on the database,
   while ensuring that view deltas are appropriately checked and calculated
   for the currently registered views as reported by a type implementing
   the ISubscribedViews protocol.

   Arguments are:

   - schema: an edl schema (\"(defschema my-schema ...)\")

   - db: a clojure.java.jdbc database

   - action-map: the HoneySQL map for the insert/update/delete action

   - subscribed-views: an implementation of ISubscribedViews implementing
                       the follow functions:

     - subscribed-views takes a ... . It should return
       a collection of view-maps.

     - broadcast-deltas takes ... ."
  [{:keys [db schema base-subscribed-views templates namespace deltas] :as conf} action-map]
  (let [subbed-views    (subscribed-views base-subscribed-views namespace)
        transaction-fn #(vd/do-view-transaction schema db subbed-views action-map templates)]
    (if deltas  ;; inside a transaction we just collect deltas and do not retry
      (let [{:keys [new-deltas result-set]} (transaction-fn)]
        (swap! deltas #(conj % new-deltas))
        result-set)
      (let [{:keys [new-deltas result-set]} (do-transaction-fn-with-retries transaction-fn)]
        (broadcast-deltas base-subscribed-views [new-deltas] namespace)
        result-set))))
