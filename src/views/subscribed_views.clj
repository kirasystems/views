(ns views.subscribed-views)

(defprotocol ISubscribedViews
  ;; Subscription and Delta routing
  (subscribe-views [this sub-request])
  (unsubscribe-views [this unsub-request])
  (disconnect [this disconnect-request])

  ;; DB interaction
  (broadcast-deltas [this deltas])
  (subscribed-views [this args]))
