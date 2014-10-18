(ns views.router
  (:require
   [views.subscribed-views :refer [subscribe-views unsubscribe-views disconnect]]
   [clojure.core.async :refer [go go-loop chan pub sub unsub close! >! >!! <! <!! filter<]]
   [clojure.tools.logging :refer [debug]]))

(defn handle-subscriptions!
  [subscribed-views subscriptions]
  (go (while true
        (let [sub (<! subscriptions)]
          (debug "Subscribing (in router): " sub)
          (subscribe-views subscribed-views sub)))))

(defn handle-unsubscriptions!
  [subscribed-views unsubscriptions]
  (go (while true
        (let [unsub (<! unsubscriptions)]
          (debug "Unsubscribing (in router): " unsub)
          (unsubscribe-views subscribed-views unsub)))))

(defn handle-disconnects!
  [subscribed-views disconnects]
  (go (while true
        (let [disc (<! disconnects)]
          (debug "Disconnecting (in router): " disc)
          (disconnect subscribed-views disc)))))

(defn init!
  [{:keys [base-subscribed-views] :as conf} client-chan]
  (let [subs        (chan 200)
        unsubs      (chan 200)
        control     (chan 200)
        disconnects (filter< #(= :disconnect (:body %)) control)]
    (sub client-chan :views.subscribe subs)
    (sub client-chan :views.unsubscribe unsubs)
    (sub client-chan :client-channel disconnects)
    (handle-subscriptions! base-subscribed-views subs)
    (handle-unsubscriptions! base-subscribed-views unsubs)
    (handle-disconnects! base-subscribed-views disconnects)))
