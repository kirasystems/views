(ns views.subscribed-views)

(defprotocol SubscribedViews
  ;; Subscription and Delta routing
  (subscribe-views [this sub-request])
  (unsubscribe-views [this unsub-request])
  (disconnect [this disconnect-request])
  (subscriber-key-fn [this msg])
  (namespace-fn [this msg])

  ;; DB interaction
  (broadcast-deltas [this db views-with-deltas])
  (subscribed-views [this]))
