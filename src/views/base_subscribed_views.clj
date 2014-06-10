(ns views.base-subscribed-views
  (:require
   [views.persistor :refer [subscribe-to-view]]
   [views.subscribed-views :refer [SubscribedViews]]
   [views.subscriptions :as vs :refer [add-subscriptions! remove-subscription! subscriptions-for]]
   [views.filters :refer [view-filter]]
   [clojure.tools.logging :refer [debug info warn error]]
   [clojure.core.async :refer [put! <! go thread]]))

(defmacro config
  [{:keys [db schema templates send-fn subscriber-fn namespace-fn unsafe?]}]
  `(do (defschema schema db "public")
       {:db db :schema schema :subscribed-views (BaseSubscribedViews. db templates send-fn subscriber-fn namespace-fn unsafe?)}))

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
  (if namespace-fn (namespace-fn msg) vs/default-ns))

(deftype BaseSubscribedViews [opts]
  SubscribedViews
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
;;        (thread
          (doseq [vs view-sigs]
            (send-fn* send-fn subscriber-key (subscribe-to-view persistor db vs popts))))))

  (unsubscribe-views
    [this msg]
    (let [{:keys [subscriber-key-fn namespace-fn]} opts
          subscriber-key (subscriber-key-fn* subscriber-key-fn msg)
          namespace      (namespace-fn* namespace-fn msg)
          view-sigs      (:views msg)]
      (info "Unsubscribing views: " view-sigs " for subscriber " subscriber-key)
      (doseq [vs view-sigs] (remove-subscription! subscriber-key vs namespace))))

  (disconnect [this msg]
    (let [{:keys [subscriber-key-fn namespace-fn]} opts
          subscriber-key (subscriber-key-fn* subscriber-key-fn msg)
          namespace      (namespace-fn* namespace-fn msg)
          view-sigs      (if namespace (subscriptions-for subscriber-key namespace) (subscriptions-for subscriber-key))]
      (doseq [vs view-sigs] (remove-subscription! subscriber-key vs namespace))))

  ;; DB interaction
  (subscribed-views [this] ) ;; (vs/compiled-views))

  (broadcast-deltas [this fdb views-with-deltas]))
