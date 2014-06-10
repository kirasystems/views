(ns views.base-subscribed-views
  (:require
   [views.persistor :refer [subscribe-to-view! unsubscribe-from-view! unsubscribe-from-all-views! get-subscribed-views]]
   [views.subscribed-views :refer [ISubscribedViews]]
   [views.subscriptions :refer [default-ns]]
   [views.filters :refer [view-filter]]
   [clojure.tools.logging :refer [debug info warn error]]
   [clojure.core.async :refer [put! <! go thread]]))

(defn send-fn*
  [send-fn address msg]
  (if send-fn
    (send-fn address msg)
    (warn "IMPLEMENT ME. Got message " msg " sent to address " address)))

(defn subscriber-key-fn*
  [subscriber-key-fn msg]
  (if subscriber-key-fn (subscriber-key-fn msg) (:subscriber-key msg)))

(defn namespace-fn*
  [namespace-fn msg]
  (if namespace-fn (namespace-fn msg) default-ns))

(deftype BaseSubscribedViews [opts]
  ISubscribedViews
  (subscribe-views
    [this {db :db :as msg}]
    (let [{:keys [persistor templates send-fn subscriber-key-fn namespace-fn unsafe?]} opts
          db             (if db db (:db opts))
          subscriber-key (subscriber-key-fn* subscriber-key-fn msg)
          namespace      (namespace-fn* namespace-fn msg)
          view-sigs      (view-filter msg (:views msg) templates {:unsafe? unsafe?}) ; this is where security comes in. Move?
          popts          {:templates templates :subscriber-key subscriber-key :namespace namespace}]
      (info "Subscribing views: " view-sigs " for subscriber " subscriber-key ", in namespace " namespace)
      (when (seq view-sigs)
        (thread
          (println "WTF")
          (doseq [vs view-sigs]
            (send-fn* send-fn subscriber-key (subscribe-to-view! persistor db vs popts)))))))

  (unsubscribe-views
    [this msg]
    (let [{:keys [subscriber-key-fn namespace-fn persistor]} opts
          subscriber-key (subscriber-key-fn* subscriber-key-fn msg)
          namespace      (namespace-fn* namespace-fn msg)
          view-sigs      (:views msg)]
      (info "Unsubscribing views: " view-sigs " for subscriber " subscriber-key)
      (doseq [vs view-sigs] (unsubscribe-from-view! persistor vs subscriber-key namespace))))

  (disconnect [this msg]
    (let [{:keys [subscriber-key-fn namespace-fn persistor]} opts
          subscriber-key (subscriber-key-fn* subscriber-key-fn msg)
          namespace      (namespace-fn* namespace-fn msg)]
      (unsubscribe-from-all-views! persistor subscriber-key namespace)))

  (subscribed-views [this args]
    (get-subscribed-views (:persistor opts) (namespace-fn* (:namespace-fn opts) args)))

  (broadcast-deltas [this fdb views-with-deltas]
    (println "broadcasting 1 2 3")))
