(ns views.db.core
  (:require
   [clojure.java.jdbc :as j]
   [clojure.tools.logging :refer [debug]]
   [views.db.deltas :as vd]
   [views.db.util :refer [with-retry retry-on-transaction-failure]]
   [views.subscribed-views :refer [subscribed-views broadcast-deltas persistence]]))

(defmacro with-view-transaction
  "Like with-db-transaction, but operates with views. If you want to use a
  standard jdbc function, the transcation database map is accessible with
  (:db vt) where vt is the bound view transaction."
  [binding & forms]
  (let [tvar (first binding), vc (second binding)]
    `(if (:deltas ~vc) ;; check if we are in a nested transaction
       (let [~tvar ~vc] ~@forms)
       (let [base-subscribed-views# (:base-subscribed-views ~vc)
             deltas#  (atom [])
             result#  (with-retry
                        (j/with-db-transaction [t# (:db ~vc) :isolation :serializable]
                          (let [~tvar (assoc ~vc :deltas deltas# :db t#)]
                            ~@forms)))]
         (broadcast-deltas base-subscribed-views# @deltas# (:namespace ~vc))
         result#))))

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
        transaction-fn #(vd/do-view-transaction (persistence base-subscribed-views) namespace schema db subbed-views action-map templates)]
    (if deltas  ;; inside a transaction we just collect deltas and do not retry
      (let [{:keys [new-deltas result-set]} (transaction-fn)]
        (swap! deltas #(conj % new-deltas))
        result-set)
      (let [{:keys [new-deltas result-set]} (retry-on-transaction-failure transaction-fn)]
        (broadcast-deltas base-subscribed-views [new-deltas] namespace)
        result-set))))
