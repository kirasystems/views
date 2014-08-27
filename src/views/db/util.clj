(ns views.db.util
  (:import
    [java.sql SQLException])
  (:require
    [clojure.tools.logging :refer [debug]]))

;; Need to catch this and retry:
;; java.sql.SQLException: ERROR: could not serialize access due to concurrent update
;;
(defn get-nested-exceptions*
  [exceptions e]
  (if-let [next-e (.getNextException e)]
    (recur (conj exceptions next-e) next-e)
    exceptions))

(defn get-nested-exceptions
  "Return the current exception and all nested exceptions as a vector."
  [e]
  (get-nested-exceptions* [e] e))

;; TODO: update to avoid stack overflow.
(defn retry-on-transaction-failure
  "Retry a function whenever we receive a transaction failure."
  [transaction-fn]
  (try
    (transaction-fn)
    (catch SQLException e
      ;; http://www.postgresql.org/docs/9.2/static/errcodes-appendix.html
      (debug "Caught exception with error code: " (.getSQLState e))
      (debug "Exception message: " (.getMessage e))

      ;; (debug "stack trace message: " (.printStackTrace e))
      (if (some #(= (.getSQLState %) "40001") (get-nested-exceptions e))
        (retry-on-transaction-failure transaction-fn) ;; try it again
        (throw e))))) ;; otherwise rethrow

(defmacro with-retry
  "Retry a transaction forever."
  [ & body]
  `(let [tfn# (fn [] ~@body)]
    (retry-on-transaction-failure tfn#)))