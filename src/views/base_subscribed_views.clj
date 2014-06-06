(ns views.base-subscribed-views
  (:require
   [views.db.core :refer [initial-views]]
   [views.subscribed-views :refer [SubscribedViews subscriber-key-fn prefix-fn send-message]]
   [views.subscriptions :as vs :refer [add-subscriptions!]]
   [clojure.tools.logging :refer [debug info warn error]]
   [clojure.core.async :refer [put! <! go thread]]))

(defrecord BaseSubscribedViews [db templates delta-broadcast-chan]
  SubscribedViews
  (subscribe-views
    [this sub-req]
    ;;    (let [view-sigs  (view-filter sub-req (:body sub-req) templates)] ; this is where security comes in.
    (let [subscriber-key (subscriber-key-fn this sub-req)
          view-sigs      (:view-sigs sub-req)]
      (info "Subscribing views: " view-sigs)
      (when (seq view-sigs)
        (add-subscriptions! subscriber-key view-sigs (prefix-fn this sub-req))
        (thread
          (->> (initial-views db view-sigs templates @vs/compiled-views)
               (send-message this subscriber-key))))))

  (unsubscribe-views [this unsub-req])

  (disconnect [this disconnect-req])

  (subscribed-views [this] @vs/compiled-views)

  (broadcast-deltas [this fdb views-with-deltas])

  (send-message [this address msg]
    (warn "IMPLEMENT ME. Got message " msg " sent to address " address))

  (subscriber-key-fn [this msg] (:subscriber-key msg))

  (prefix-fn [this msg] nil))
