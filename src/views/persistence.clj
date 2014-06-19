(ns views.persistence
  (:require
   [views.subscriptions :refer [add-subscription! remove-subscription! compiled-view-for compiled-views-for subscriptions-for]]))

(defprotocol IPersistence
  (subscribe-to-view! [this db view-sig opts])
  (unsubscribe-from-view! [this view-sig subscriber-key namespace])
  (unsubscribe-from-all-views! [this subscriber-key namespace])
  (get-subscribed-views [this namespace]))

(deftype InMemoryPersistence []
  IPersistence
  (subscribe-to-view!
    [persistor db view-sig {:keys [templates subscriber-key namespace]}]
    (add-subscription! view-sig templates subscriber-key namespace))

  (unsubscribe-from-view!
    [this view-sig subscriber-key namespace]
    (remove-subscription! view-sig subscriber-key namespace))

  (unsubscribe-from-all-views!
    [this subscriber-key namespace]
    (doseq [vs (subscriptions-for subscriber-key namespace)]
      (remove-subscription! vs subscriber-key namespace)))

  (get-subscribed-views [this namespace] (compiled-views-for namespace)))
