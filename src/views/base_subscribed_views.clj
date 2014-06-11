(ns views.base-subscribed-views
  (:require
   [views.persistence :refer [subscribe-to-view! unsubscribe-from-view! unsubscribe-from-all-views! get-subscribed-views]]
   [views.subscribed-views :refer [ISubscribedViews]]
   [views.subscriptions :refer [default-ns subscribed-to]]
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
    [this {tdb :tdb :as msg}]
    (let [{:keys [persistence templates send-fn subscriber-key-fn namespace-fn unsafe?]} opts
          db             (if tdb tdb (:db opts))
          subscriber-key (subscriber-key-fn* subscriber-key-fn msg)
          namespace      (namespace-fn* namespace-fn msg)
          view-sigs      (view-filter msg (:views msg) templates {:unsafe? unsafe?}) ; this is where security comes in. Move?
          popts          {:templates templates :subscriber-key subscriber-key :namespace namespace}]
      (info "Subscribing views: " view-sigs " for subscriber " subscriber-key ", in namespace " namespace)
      (when (seq view-sigs)
        (thread
          (doseq [vs view-sigs]
            (->> (subscribe-to-view! persistence db vs popts)
                 (send-fn* send-fn subscriber-key)))))))

  (unsubscribe-views
    [this msg]
    (let [{:keys [subscriber-key-fn namespace-fn persistence]} opts
          subscriber-key (subscriber-key-fn* subscriber-key-fn msg)
          namespace      (namespace-fn* namespace-fn msg)
          view-sigs      (:views msg)]
      (info "Unsubscribing views: " view-sigs " for subscriber " subscriber-key)
      (doseq [vs view-sigs] (unsubscribe-from-view! persistence vs subscriber-key namespace))))

  (disconnect [this msg]
    (let [{:keys [subscriber-key-fn namespace-fn persistence]} opts
          subscriber-key (subscriber-key-fn* subscriber-key-fn msg)
          namespace      (namespace-fn* namespace-fn msg)]
      (unsubscribe-from-all-views! persistence subscriber-key namespace)))

  (subscribed-views [this args]
    (map :view-data (vals (get-subscribed-views (:persistence opts) (namespace-fn* (:namespace-fn opts) args)))))

  (broadcast-deltas [this deltas]
    (doseq [vs (keys deltas)]
      (doseq [sk (subscribed-to vs)]
        (send-fn* (:send-fn opts) sk (get deltas vs))))))
