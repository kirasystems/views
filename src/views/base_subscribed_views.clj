(ns views.base-subscribed-views
  (:require
   [views.persistence :refer [subscribe-to-view! unsubscribe-from-view! unsubscribe-from-all-views! get-subscribed-views]]
   [views.subscribed-views :refer [ISubscribedViews]]
   [views.subscriptions :refer [default-ns subscribed-to compiled-view-for]]
   [views.filters :refer [view-filter]]
   [views.db.load :refer [initial-view]]
   [clojure.tools.logging :refer [debug info warn error]]
   [clojure.core.async :refer [put! <! go thread]]
   [clojure.java.jdbc :as j]))

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

(defn view-sig-fn*
  [view-sig-fn msg]
  (if view-sig-fn (view-sig-fn msg) (:body msg)))

(deftype BaseSubscribedViews [opts]
  ISubscribedViews
  (subscribe-views
    [this msg]
    (let [{:keys [persistence templates db-fn send-fn view-sig-fn subscriber-key-fn namespace-fn unsafe?]} opts
          db             (if db-fn (db-fn msg) (:db opts))
          subscriber-key (subscriber-key-fn* subscriber-key-fn msg)
          namespace      (namespace-fn* namespace-fn msg)
          view-sigs      (view-filter msg (view-sig-fn* view-sig-fn msg) templates {:unsafe? unsafe?}) ; this is where security comes in. Move?
          popts          {:templates templates :subscriber-key subscriber-key :namespace namespace}]
      (debug "Subscribing views: " view-sigs " for subscriber " subscriber-key ", in namespace " namespace)
      (when (seq view-sigs)
        (thread
          (doseq [vs view-sigs]
            (j/with-db-transaction [t db :isolation :serializable]
              (subscribe-to-view! persistence db vs popts)
              (let [view (:view (if namespace (compiled-view-for vs namespace) (compiled-view-for vs)))
                    iv   (initial-view t vs templates view)]
                (send-fn* send-fn subscriber-key iv))))))))

  (unsubscribe-views
    [this msg]
    (let [{:keys [subscriber-key-fn namespace-fn persistence view-sig-fn]} opts
          subscriber-key (subscriber-key-fn* subscriber-key-fn msg)
          namespace      (namespace-fn* namespace-fn msg)
          view-sigs      (view-sig-fn* view-sig-fn msg)]
      (debug "Unsubscribing views: " view-sigs " for subscriber " subscriber-key)
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
