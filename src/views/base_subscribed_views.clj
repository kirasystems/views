(ns views.base-subscribed-views
  (:require
   [views.persistence.core :as persist]
   [views.subscribed-views :refer [ISubscribedViews]]
   [views.filters :refer [view-filter]]
   [views.db.load :refer [initial-view]]
   [views.db.util :refer [with-retry]]
   [clojure.tools.logging :refer [debug info warn error]]
   [clojure.core.async :refer [put! <! go thread]]
   [clojure.java.jdbc :as j]

   ;; Metrics
   [views.riemann :refer [rclient]]
   [riemann.client :refer [send-event]]
   ))

(def default-ns :default-ns)

(declare send-deltas)

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

(defn subscribe-and-compute
  "Subscribe a view and return the initial values."
  [db persistence templates vs namespace subscriber-key]
  (let [view-data (persist/subscribe! persistence templates namespace vs subscriber-key)]
    (with-retry
      (j/with-db-transaction [t db :isolation :serializable]
          (initial-view t vs templates (:view view-data))))))

;; Deltas look like:
;; [{view-sig1 delta, view-sig2 delta, ...} {view-sig3 delta, ...}]

(defn delta-signatures
  "Return all the signatures mentioned by a map of deltas."
  [deltas]
  (mapcat keys deltas))

(deftype BaseSubscribedViews [config]
  ISubscribedViews
  (subscribe-views
    [this msg]
    (let [{:keys [persistence templates db-fn send-fn view-sig-fn subscriber-key-fn namespace-fn unsafe?]} config
          db             (if db-fn (db-fn msg) (:db config))
          subscriber-key (subscriber-key-fn* subscriber-key-fn msg)
          namespace      (namespace-fn* namespace-fn msg)
          view-sigs      (view-filter msg (view-sig-fn* view-sig-fn msg) templates {:unsafe? unsafe?})] ; this is where security comes in.
      (debug "Subscribing views: " view-sigs " for subscriber " subscriber-key ", in namespace " namespace)
      (when (seq view-sigs)
          (doseq [vs view-sigs]
            (thread
              (let [iv (subscribe-and-compute db persistence templates vs namespace subscriber-key)
                    start  (System/currentTimeMillis)]
                (send-fn* send-fn subscriber-key :views.init iv)
                (send-event rclient {:service "subscription-init-time" :metric (- (System/currentTimeMillis) start)})))))))

  (unsubscribe-views
    [this msg]
    (let [{:keys [subscriber-key-fn namespace-fn persistence view-sig-fn]} config
          subscriber-key (subscriber-key-fn* subscriber-key-fn msg)
          namespace      (namespace-fn* namespace-fn msg)
          view-sigs      (view-sig-fn* view-sig-fn msg)]
      (debug "Unsubscribing views: " view-sigs " for subscriber " subscriber-key)
      (doseq [vs view-sigs]
        (persist/unsubscribe! persistence namespace vs subscriber-key))))

  (disconnect [this msg]
    (let [{:keys [subscriber-key-fn namespace-fn persistence]} config
          subscriber-key (subscriber-key-fn* subscriber-key-fn msg)
          namespace      (namespace-fn* namespace-fn msg)]
      (debug "Disconnecting subscriber " subscriber-key " in namespace " namespace)
      (persist/unsubscribe-all! persistence namespace subscriber-key)))

  ;;
  ;; The two below functions get called by vexec!/with-view-transaction
  ;;

  (subscribed-views [this namespace]
    ;; Table name optimization not yet worked through the library.
    (persist/view-data (:persistence config) namespace "fix-me"))

  (broadcast-deltas [this deltas namespace]
    (let [{:keys [templates]} config
          namespace (if namespace namespace default-ns)
          subs      (persist/subscriptions (:persistence config) namespace (delta-signatures deltas))]
      (send-deltas deltas subs namespace config))))

(defn post-process-delta-map
  [post-fn delta-map]
  (if-let [rset (:refresh-set delta-map)]
    delta-map
    (reduce #(assoc %1 %2 (map post-fn (get delta-map %2))) {} (keys delta-map))))

(defn post-process-deltas
  "Run post-processing functions on each delta. NOTE: this puts things in maps
  to maintain compatability with the frontend code."
  [delta templates]
  (let [vs (first delta)]
    (if-let [post-fn (get-in templates [(first vs) :post-fn])]
      {(first delta) (mapv #(post-process-delta-map post-fn %) (second delta))}
      {(first delta) (second delta)})))

;; We flatten the above into a sequence:
;; [[view-sig1 delta-data], [view-sig2 delta-data]....]
;; where the signatures from each pack are listed in order.

(defn flatten-deltas
  "We flatten the above into a sequence:
   [[view-sig1 delta-data], [view-sig2 delta-data]....]
   where the signatures from each pack are listed in order."
  [deltas]
  (reduce #(into %1 (seq %2)) [] deltas))

(defn update-subscriber-pack
  "Given a delta [view-sig delta-data] we find the subscribers that need it
  and add to the subscriber pack vector {view-sig [delta...]}."
  [subs spacks delta]
  (let [subscribers (get subs (ffirst delta))]
    (reduce #(update-in %1 [%2] (fnil conj []) delta) spacks subscribers)))

(defn subscriber-deltas
  "Group deltas into subscriber packs."
  [subs deltas]
  (reduce #(update-subscriber-pack subs %1 %2) {} deltas))

;; Deltas looks like:
;; [delta-pack1 delta-pack2 ...]
;; where each delta pack is a map:
;; {view-sig1 delta-data, view-sig2 delta-data, ...}

(defn send-deltas
  "Send deltas out to subscribers."
  [deltas subs namespace {:keys [send-fn templates] :as config}]
  (let [deltas (mapv #(post-process-deltas % templates) (flatten-deltas deltas))
        start  (System/currentTimeMillis)]
    (doseq [[sk deltas*] (subscriber-deltas subs deltas)]
      (debug "Sending deltas " deltas* " to subscriber " sk)
      (send-fn* send-fn sk :views.deltas deltas*))
    (send-event rclient {:service "delta-send-time" :metric (- (System/currentTimeMillis) start)})))
