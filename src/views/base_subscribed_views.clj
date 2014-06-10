(ns views.base-subscribed-views
  (:require
   [views.db.load :refer [initial-views]]
   [views.subscribed-views :refer [SubscribedViews subscriber-key-fn namespace-fn]]
   [views.subscriptions :as vs :refer [add-subscriptions! remove-subscription! subscriptions-for]]
   [clojure.tools.logging :refer [debug info warn error]]
   [clojure.core.async :refer [put! <! go thread]]))

(defn view-filter
  "Takes a subscription request msg, a collection of view-sigs and
   the config templates hash-map for an app. Checks if there is
   a global filter-fn in the hash-map metadata and checks against
   that if it exists, as well as against any existing filter
   functions for individual template config entries. Template
   config hash-map entries can specify a filter-fn using the key
   :filter-fn, and the global filter-fn is the same, only on
   the config meta-data (i.e. (with-meta templates {:filter-fn ...}))

   By default throws an exception if no filters are present.
   By passing in {:unsafe true} in opts, this can be overridden."
  [msg view-sigs templates & opts]
  (let [global-filter-fn (:filter-fn (meta templates))]
    (filterv
     #(let [filter-fn (:filter-fn (get templates (first %)))]
        (cond
         (and filter-fn global-filter-fn)
         (and (global-filter-fn msg %) (filter-fn msg %))

         filter-fn
         (filter-fn msg %)

         global-filter-fn
         (global-filter-fn msg %)

         :else
         (if (-> opts first :unsafe)
           (do (warn "YOU ARE RUNNING IN UNSAFE MODE, AND NO FILTERS ARE PRESENT FOR VIEW-SIG: " %)
               true)
           (throw (Exception. (str "No filter set for view " %))))))
     view-sigs)))

(defn send-message
  [this address msg]
  (warn "IMPLEMENT ME. Got message " msg " sent to address " address))

(deftype BaseSubscribedViews [db templates send-fn broadcast-fn subscribed-views-fn opts]
  SubscribedViews
  (subscribe-views
    [this sub-req]
    (let [subscriber-key (subscriber-key-fn this sub-req)
          view-sigs      (view-filter sub-req (:views sub-req) templates opts)] ; this is where security comes in.
      (info "Subscribing views: " view-sigs " for subscriber " subscriber-key)
      (when (seq view-sigs)
        (let [subbed-views (if-let [namespace (namespace-fn this sub-req)]
                             (add-subscriptions! subscriber-key view-sigs templates namespace)
                             (add-subscriptions! subscriber-key view-sigs templates))]
          (thread
            (->> (initial-views db view-sigs templates subbed-views)
                 ((if send-fn send-fn send-message) this subscriber-key)))))))

  (unsubscribe-views
    [this unsub-req]
    (let [subscriber-key (subscriber-key-fn this unsub-req)
          view-sigs (:views unsub-req)]
      (info "Unsubscribing views: " view-sigs " for subscriber " subscriber-key)
      (if-let [namespace (namespace-fn this unsub-req)]
        (doseq [vs view-sigs] (remove-subscription! subscriber-key vs namespace))
        (doseq [vs view-sigs] (remove-subscription! subscriber-key vs)))))

  (disconnect [this disconnect-req]
    (let [subscriber-key (:subscriber-key disconnect-req)
          namespace         (namespace-fn this disconnect-req)
          view-sigs      (if namespace (subscriptions-for subscriber-key namespace) (subscriptions-for subscriber-key))]
      (if namespace
        (doseq [vs view-sigs] (remove-subscription! subscriber-key vs namespace))
        (doseq [vs view-sigs] (remove-subscription! subscriber-key vs)))))

  (subscriber-key-fn [this msg] (:subscriber-key msg))

  (namespace-fn [this msg] nil)

  ;; DB interaction
  (subscribed-views [this] @vs/compiled-views)

  (broadcast-deltas [this fdb views-with-deltas]))
