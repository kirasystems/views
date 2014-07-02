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

(declare post-process-deltas)

(defn send-fn*
  [send-fn address subject msg]
  (if send-fn
    (send-fn address subject msg)
    (warn "IMPLEMENT ME. Got message " msg " with subject " subject " sent to address " address)))

(defn subscriber-key-fn*
  [subscriber-key-fn msg]
  (if subscriber-key-fn (subscriber-key-fn msg) (:subscriber-key msg)))

(defn namespace-fn*
  [namespace-fn msg]
  (if namespace-fn (namespace-fn msg) default-ns))

(defn view-sig-fn*
  [view-sig-fn msg]
  (if view-sig-fn (view-sig-fn msg) (:body msg)))

(deftype BaseSubscribedViews [config]
  ISubscribedViews
  (subscribe-views
    [this msg]
    (let [{:keys [persistence templates db-fn send-fn view-sig-fn subscriber-key-fn namespace-fn unsafe?]} config
          db             (if db-fn (db-fn msg) (:db config))
          subscriber-key (subscriber-key-fn* subscriber-key-fn msg)
          namespace      (namespace-fn* namespace-fn msg)
          view-sigs      (view-filter msg (view-sig-fn* view-sig-fn msg) templates {:unsafe? unsafe?}) ; this is where security comes in.
          pconfig        {:templates templates :subscriber-key subscriber-key :namespace namespace}]
      (debug "Subscribing views: " view-sigs " for subscriber " subscriber-key ", in namespace " namespace)
      (when (seq view-sigs)
        (thread
          (doseq [vs view-sigs]
            (j/with-db-transaction [t db :isolation :serializable]
              (subscribe-to-view! persistence db vs pconfig)
              (let [view (:view (if namespace (compiled-view-for vs namespace) (compiled-view-for vs)))
                    iv   (initial-view t vs templates view)]
                (send-fn* send-fn subscriber-key :views.init iv))))))))

  (unsubscribe-views
    [this msg]
    (let [{:keys [subscriber-key-fn namespace-fn persistence view-sig-fn]} config
          subscriber-key (subscriber-key-fn* subscriber-key-fn msg)
          namespace      (namespace-fn* namespace-fn msg)
          view-sigs      (view-sig-fn* view-sig-fn msg)]
      (debug "Unsubscribing views: " view-sigs " for subscriber " subscriber-key)
      (doseq [vs view-sigs] (unsubscribe-from-view! persistence vs subscriber-key namespace))))

  (disconnect [this msg]
    (let [{:keys [subscriber-key-fn namespace-fn persistence]} config
          subscriber-key (subscriber-key-fn* subscriber-key-fn msg)
          namespace      (namespace-fn* namespace-fn msg)]
      (debug "Disconnecting subscriber " subscriber-key " in namespace " namespace)
      (unsubscribe-from-all-views! persistence subscriber-key namespace)))

  ;;
  ;; The two below functions get called by vexec!
  ;;

  (subscribed-views [this namespace]
    (map :view-data (vals (get-subscribed-views (:persistence config) namespace))))

  (broadcast-deltas [this deltas namespace]
    (let [{:keys [templates]} config]
      (doseq [vs (keys deltas)]
        (debug "Subscribers subscribed to " vs " are "
               (if namespace (subscribed-to vs namespace) (subscribed-to vs)))
        (doseq [sk (if namespace (subscribed-to vs namespace) (subscribed-to vs))]
          (doseq [ds (map #(post-process-deltas templates %) (get deltas vs))]
            (debug "Sending delta " {vs (dissoc ds :view-sig)} " to addr " sk)
            (send-fn* (:send-fn config) sk :views.deltas {vs (dissoc ds :view-sig)})))))))

(defn post-process-deltas
  [templates delta-map]
  (let [vs (:view-sig delta-map)
        dm (dissoc delta-map :view-sig)]
    (if-let [post-fn (get-in templates [(first vs) :post-fn])]
      (reduce #(assoc %1 %2 (map post-fn (get dm %2))) {} (keys dm))
      dm)))
