(ns views.persistor
  (:require
   [clojure.java.jdbc :as j]
   [views.subscriptions :refer [add-subscription! compiled-view-for]]
   [views.db.load :refer [initial-view]]))

(defprotocol IPersistor
  (subscribe-to-view [this db view-sig opts]))

(deftype InMemoryPersistor []
  IPersistor
  (subscribe-to-view
    [persistor db view-sig {:keys [templates subscriber-key namespace]}]
    (j/with-db-transaction [t db :isolation :serializable]
      (add-subscription! subscriber-key view-sig templates namespace)
      (initial-view t view-sig templates (:view (compiled-view-for view-sig))))))
