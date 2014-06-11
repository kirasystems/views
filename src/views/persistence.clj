(ns views.persistence
  (:require
   [clojure.java.jdbc :as j]
   [views.subscriptions :refer [add-subscription! remove-subscription! compiled-view-for compiled-views-for subscriptions-for]]
   [views.db.load :refer [initial-view]]))

(defprotocol IPersistence
  (subscribe-to-view! [this db view-sig opts])
  (unsubscribe-from-view! [this view-sig subscriber-key namespace])
  (unsubscribe-from-all-views! [this subscriber-key namespace])
  (get-subscribed-views [this namespace]))

(deftype InMemoryPersistence []
  IPersistence
  (subscribe-to-view!
    [persistor db view-sig {:keys [templates subscriber-key namespace]}]
    (j/with-db-transaction [t db :isolation :serializable]
      (add-subscription! view-sig templates subscriber-key namespace)
      (initial-view t view-sig templates (:view (compiled-view-for view-sig)))))

  (unsubscribe-from-view!
    [this view-sig subscriber-key namespace]
    (remove-subscription! view-sig subscriber-key namespace))

  (unsubscribe-from-all-views!
    [this subscriber-key namespace]
    (doseq [vs (subscriptions-for subscriber-key namespace)]
      (remove-subscription! vs subscriber-key namespace)))

  (get-subscribed-views [this namespace] (compiled-views-for namespace)))
