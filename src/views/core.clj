(ns views.core
  (:import
    [java.util.concurrent ArrayBlockingQueue TimeUnit])
  (:require
    [clojure.tools.logging :refer [debug error]]
    [environ.core :refer [env]]
    [views.hash :refer [md5-hash]]
    [views.protocols :refer [IView id data relevant?]]
    [views.statistics :as statistics]))

(defn ->view-sig
  ([namespace view-id parameters]
   {:namespace  namespace
    :view-id    view-id
    :parameters parameters})
  ([view-id parameters]
   {:view-id    view-id
    :parameters parameters}))

(defn poll-queue!
  "Poll the view system refresh queue returning next refresh. This blocks for 60 seconds."
  [view-system]
  (.poll ^ArrayBlockingQueue (:refresh-queue @view-system) 60 TimeUnit/SECONDS))

(defn subscribe-view
  [view-system view-sig subscriber-key]
  (-> view-system
      (update-in [:subscribed subscriber-key] (fnil conj #{}) view-sig)
      (update-in [:subscribers view-sig] (fnil conj #{}) subscriber-key)))

(defn assoc-hash
  "Set the hash for a view-sig to the given value."
  [view-system view-sig data-hash]
  (assoc-in view-system [:hashes view-sig] data-hash))

(defn assoc-hash-if-missing!
  "Add a hash if it is missing."
  [view-system view-sig data-hash]
  (swap! view-system update-in [:hashes view-sig] #(or % data-hash)))

(defn- send-view-data!
  [view-system subscriber-key view-sig data]
  (if-let [send-fn (:send-fn view-system)]
    (send-fn subscriber-key [(dissoc view-sig :namespace) data])
    (throw (Exception. "no send-fn function set in view-system"))))

(defn- authorized-subscription?
  [view-system view-sig subscriber-key context]
  (if-let [auth-fn (:auth-fn view-system)]
    (auth-fn view-sig subscriber-key context)
    ; assume that if no auth-fn is specified, that we are not doing auth checks at all
    ; so do not disallow access to any subscription
    true))

(defn- on-unauthorized-subscription
  [view-system view-sig subscriber-key context]
  (if-let [on-unauth-fn (:on-unauth-fn view-system)]
    (on-unauth-fn view-sig subscriber-key context)))

(defn- view-namespace
  "Get the namespace, either out of the view signature first, or if missing via the namespace-fn."
  [view-system view-sig subscriber-key context]
  (or (:namespace view-sig)
      (if-let [namespace-fn (:namespace-fn view-system)]
        (namespace-fn view-sig subscriber-key context))))

(defn- subscribe-and-send!
  [view-system view view-sig subscriber-key]
  (swap! view-system subscribe-view view-sig subscriber-key)
  (future
    (try
      (let [vdata     (data view (:namespace view-sig) (:parameters view-sig))
            data-hash (md5-hash vdata)]
        ;; Check to make sure that we are still subscribed. It's possible that
        ;; an unsubscription event came in while computing the view.
        (when (contains? (get-in @view-system [:subscribed subscriber-key]) view-sig)
          (assoc-hash-if-missing! view-system view-sig data-hash) ;; see note #1 in NOTES.md
          (send-view-data! @view-system subscriber-key view-sig vdata)))
      (catch Exception e
        (error "error subscribing:" view-sig "e:" e "msg:" (.getMessage e))))))

(defn subscribe!
  "Creates a subscription to a view identified by view-sig for a subscriber
   identified by subscriber-key. If the subscription is not authorized,
   returns nil. Additional context info can be passed in, which will be
   passed to the view-system's namespace-fn and auth-fn (if provided). If
   the subscription is successful, the subscriber will be sent the initial
   data for the view."
  [view-system {:keys [view-id parameters] :as view-sig} subscriber-key context]
  (if-let [view (get-in @view-system [:views view-id])]
    (let [namespace (view-namespace @view-system view-sig subscriber-key context)
          view-sig  (->view-sig namespace view-id parameters)]
      (if (authorized-subscription? @view-system view-sig subscriber-key context)
        (subscribe-and-send! view-system view view-sig subscriber-key)
        (do
          (debug "subscription not authorized" view-sig subscriber-key context)
          (on-unauthorized-subscription @view-system view-sig subscriber-key context)
          nil)))
    (throw (new Exception (str "Subscription for non-existant view: " view-id)))))

(defn- remove-from-subscribers
  [view-system view-sig subscriber-key]
  (-> view-system
      (update-in [:subscribers view-sig] disj subscriber-key)
      ; remove view-sig entry if no subscribers. helps prevent the subscribers
      ; map from e.g. endlessly filling up with all sorts of different
      ; view-sigs with crazy amounts of only-slightly-varying parameters
      (update :subscribers
              (fn [subscribers]
                (if (empty? (get subscribers view-sig))
                  (dissoc subscribers view-sig)
                  subscribers)))))

(defn- remove-from-subscribed
  [view-system view-sig subscriber-key]
  (-> view-system
      (update-in [:subscribed subscriber-key] disj view-sig)
      ; remove subscriber-key entry if no current subscriptions. this helps prevent
      ; the subscribed map from (for example) endlessly filling up with massive
      ; amounts of entries with no subscriptions. this could easily happen over time
      ; naturally for applications with long uptimes.
      (update :subscribed
              (fn [subscribed]
                (if (empty? (get subscribed subscriber-key))
                  (dissoc subscribed subscriber-key)
                  subscribed)))))

(defn- clean-up-unneeded-hashes
  "Remove hashes for view-sigs which do not have any subscribers."
  [view-system view-sig]
  (if-not (get (:subscribers view-system) view-sig)
    (update view-system :hashes dissoc view-sig)
    view-system))

(defn unsubscribe!
  "Removes a subscription to a view identified by view-sig for a subscriber
   identified by subscriber-key. Additional context info can be passed in,
   which will be passed to the view-system's namespace-fn (if provided)."
  [view-system {:keys [view-id parameters] :as view-sig} subscriber-key context]
  (debug "unsubscribing from view" view-sig subscriber-key)
  (swap! view-system
         (fn [vs]
           (let [namespace (view-namespace vs view-sig subscriber-key context)
                 view-sig  (->view-sig namespace view-id parameters)]
             (-> vs
                 (remove-from-subscribed view-sig subscriber-key)
                 (remove-from-subscribers view-sig subscriber-key)
                 (assoc-hash view-sig nil) ;; see note #2 in NOTES.md
                 (clean-up-unneeded-hashes view-sig)))))
  view-system)

(defn unsubscribe-all!
  "Remove all subscriptions by a given subscriber."
  [view-system subscriber-key]
  (debug "unsubscribing from all views" subscriber-key)
  (swap! view-system
         (fn [vs]
           (let [view-sigs (get-in vs [:subscribed subscriber-key])]
             (as-> (update vs :subscribed dissoc subscriber-key) vs*
                   (reduce #(remove-from-subscribers %1 %2 subscriber-key) vs* view-sigs)
                   (reduce #(assoc-hash %1 %2 nil) vs* view-sigs) ;; see note #2 in NOTES.md
                   (reduce #(clean-up-unneeded-hashes %1 %2) vs* view-sigs)))))
  view-system)

(defn refresh-view!
  "Schedules a view (identified by view-sig) to be refreshed by one of the worker threads
   only if the provided collection of hints is relevant to that view."
  [view-system hints {:keys [namespace view-id parameters] :as view-sig}]
  (let [v (get-in @view-system [:views view-id])]
    (try
      (if (relevant? v namespace parameters hints)
        (if-not (.contains ^ArrayBlockingQueue (:refresh-queue @view-system) view-sig)
          (when-not (.offer ^ArrayBlockingQueue (:refresh-queue @view-system) view-sig)
            (when (statistics/collecting? view-system)
              (statistics/dropped! view-system))
            (error "refresh-queue full, dropping refresh request for" view-sig))
          (do
            (when (statistics/collecting? view-system)
              (statistics/deduplicated! view-system))
            (debug "already queued for refresh" view-sig))))
      (catch Exception e
        (error "error determining if view is relevant, view-id:" view-id "e:" e))))
  view-system)

(defn subscribed-views
  "Returns a list of all views in the system that have subscribers."
  [view-system]
  (reduce into #{} (vals (:subscribed @view-system))))

(defn pop-hints!
  "Return hints and clear hint set atomicly."
  [view-system]
  (let [old-val @view-system
        new-val (assoc old-val :hints #{})]
    (if (compare-and-set! view-system old-val new-val)
      (or (:hints old-val) #{})
      (recur view-system))))

(defn refresh-views!
  "Given a collection of hints, or a single hint, find all dirty views and schedule them for a refresh."
  ([view-system hints]
   (when (seq hints)
     (debug "refresh hints:" hints)
     (doseq [view-sig (subscribed-views view-system)]
       (refresh-view! view-system hints view-sig)))
   (swap! view-system assoc :last-update (System/currentTimeMillis)))
  ([view-system]
   (refresh-views! view-system (pop-hints! view-system))))

(defn can-refresh?
  [last-update min-refresh-interval]
  (> (- (System/currentTimeMillis) last-update) min-refresh-interval))

(defn wait
  [last-update min-refresh-interval]
  (Thread/sleep (max 0 (- min-refresh-interval (- (System/currentTimeMillis) last-update)))))

(defn do-view-refresh!
  [view-system {:keys [namespace view-id parameters] :as view-sig}]
  (when (statistics/collecting? view-system)
    (statistics/refreshed! view-system))
  (try
    (let [view  (get-in @view-system [:views view-id])
          vdata (data view namespace parameters)
          hdata (md5-hash vdata)]
      (when-not (= hdata (get-in @view-system [:hashes view-sig]))
        (doseq [subscriber-key (get-in @view-system [:subscribers view-sig])]
          (send-view-data! @view-system subscriber-key view-sig vdata))
        (swap! view-system assoc-in [:hashes view-sig] hdata)))
    (catch Exception e
      (error "error refreshing:" namespace view-id parameters
             "e:" e "msg:" (.getMessage e)))))

(defn- refresh-worker-thread
  "Handles refresh requests."
  [view-system]
  (fn []
    (try
      (when-let [view-sig (poll-queue! view-system)]
        (debug "worker running refresh for" view-sig)
        (do-view-refresh! view-system view-sig))
      (catch InterruptedException e))
    (if-not (:stop-workers? @view-system)
      (recur)
      (debug "exiting worker thread"))))

(defn- refresh-watcher-thread
  [view-system min-refresh-interval]
  (fn []
    (let [last-update (:last-update @view-system)]
      (try
        (if (can-refresh? last-update min-refresh-interval)
          (refresh-views! view-system)
          (wait last-update min-refresh-interval))
        (catch InterruptedException e)
        (catch Exception e
          (error "exception in views e:" e  "msg:"(.getMessage e))))
      (if-not (:stop-refresh-watcher? @view-system)
        (recur)
        (debug "exiting refresh watcher thread")))))

(defn start-update-watcher!
  "Starts threads for the views refresh watcher and worker threads that handle queued
   hints and view refresh requests."
  [view-system min-refresh-interval threads]
  (debug "starting refresh watcher at" min-refresh-interval "ms interval and" threads "workers")
  (if (and (:refresh-watcher @view-system)
           (:workers @view-system))
    (error "cannot start new watcher and worker threads until existing threads are stopped")
    (let [refresh-watcher (Thread. ^Runnable (refresh-watcher-thread view-system min-refresh-interval))
          worker-threads  (mapv (fn [_] (Thread. ^Runnable (refresh-worker-thread view-system)))
                                (range threads))]
      (swap! view-system assoc
             :last-update 0
             :refresh-watcher refresh-watcher
             :stop-refresh-watcher? false
             :workers worker-threads
             :stop-workers? false)
      (.start refresh-watcher)
      (doseq [^Thread t worker-threads]
        (.start t))
      view-system)))

(defn stop-update-watcher!
  "Stops threads for the views refresh watcher and worker threads."
  [view-system & [dont-wait-for-threads?]]
  (debug "stopping refresh watcher and workers")
  (let [worker-threads (:workers @view-system)
        watcher-thread (:refresh-watcher @view-system)
        threads        (->> worker-threads
                            (cons watcher-thread)
                            (remove nil?))]
    (swap! view-system assoc
           :stop-refresh-watcher? true
           :stop-workers? true)
    (doseq [^Thread t threads]
      (.interrupt t))
    (if-not dont-wait-for-threads?
      (doseq [^Thread t threads]
        (.join t)))
    (swap! view-system assoc
           :refresh-watcher nil
           :workers nil))
  view-system)

(defn hint
  "Create a hint."
  [namespace hint type]
  {:namespace namespace :hint hint :type type})

(defn queue-hints!
  "Queues up hints in the view system so that they will be picked up by the refresh
   watcher and dispatched to the workers resulting in view updates being sent out
   for the relevant views/subscribers."
  [view-system hints]
  (debug "queueing hints" hints)
  (swap! view-system update :hints (fnil into #{}) hints)
  view-system)

(defn put-hints!
  "Adds a collection of hints to the view system by using the view system
   configuration's :put-hints-fn."
  [view-system hints]
  ((:put-hints-fn @view-system) view-system hints)
  view-system)

(defn- id-view-pairs
  "Transform a collection of views into a collection of pairs [id view]."
  [views]
  (map vector (map id views) views))

(defn add-views!
  "Add a collection of views to the system."
  [view-system views]
  (swap! view-system update :views (fnil into {}) (id-view-pairs views)))

(def default-options
  "Default options used to initialize the views system via init!

   :send-fn (fn [subscriber-key [view-sig view-data]] ...)
   *REQUIRED*
   A function that is used to send view refresh data to subscribers.
   This function must be set for normal operation of the views system.

   :put-hints-fn       (fn [^Atom view-system hints] (refresh-views! view-system hints))
   *REQUIRED*
   A function that adds hints to the view system. this function will be used
   by other libraries that implement IView. This function must be set for
   normal operation of the views system. The default function provided
   will trigger relevant view refreshes immediately.

   :refresh-queue-size 1000
   *REQUIRED*
   The size of the queue used to hold view refresh requests for
   the worker threads. For very heavy systems, this can be set
   higher if you start to get warnings about dropped refresh requests.

   :refresh-interval   1000
   *REQUIRED*
   Interval in milliseconds at which the refresh watcher thread will
   check for any queued up hints and dispatch relevant view refresh
   updates to the worker threads.

   :worker-threads     8
   *REQUIRED*
   The number of refresh worker threads that poll for view refresh
   requests and dispatch updated view data to subscribers.

   :views              nil
   A list of IView instances. these are the views that can be subscribed
   to. views can also be added/replaced after system initialization through
   the use of add-views!

   :auth-fn            (fn [view-sig subscriber-key context] ... )
   A function that authorizes view subscriptions. should return true if the
   subscription is authorized. If not set, no view subscriptions will require
   any authorization.

   :on-unauth-fn       (fn [view-sig subscriber-key context] ... )
   A function that is called when subscription authorization fails.

   :namespace-fn       (fn [view-sig subscriber-key context] ... )
   A function that returns a namespace to use for view subscriptions.

   :stats-log-interval nil
   Interval in milliseconds at which a logger will write view system
   statistics to the log. if not set, the logger will be disabled."
  {:send-fn            nil
   :put-hints-fn       (fn [view-system hints] (refresh-views! view-system hints))
   :refresh-queue-size (if-let [n (:views-refresh-queue-size env)]
                         (Long/parseLong n)
                         1000)
   :refresh-interval   1000
   :worker-threads     8
   :views              nil
   :auth-fn            nil
   :on-unauth-fn       nil
   :namespace-fn       nil
   :stats-log-interval nil})

(defn view-system
  "Return a new view system."
  [options]
  (atom (merge default-options options)))

(defn init!
  "Initializes the view system for use with the list of views provided.

   An existing atom that will be used to store the state of the views
   system can be provided, otherwise one will be created. Either way,
   the atom with the initialized view system is returned.

   Options is a map of options to configure the view system with. See
   views.core/default-options for a description of the available options
   and the defaults that will be used for any options not provided in
   the call to init!."
  ([view-system options]
   (let [options (merge default-options options)
         stats-log-interval (:stats-log-interval options)]
     (debug "initializing views system using options:" options)
     (reset! view-system
             {:refresh-queue (ArrayBlockingQueue. (:refresh-queue-size options))
              :views         (into {} (id-view-pairs (:views options)))
              :send-fn       (:send-fn options)
              :put-hints-fn  (:put-hints-fn options)
              :auth-fn       (:auth-fn options)
              :on-unauth-fn  (:on-unauth-fn options)
              :namespace-fn  (:namespace-fn options)
              ; keeping a copy of the options used during init allows other libraries
              ; that plugin/extend views functionality (e.g. IView implementations)
              ; to make use of any options themselves
              :options       options})
     (start-update-watcher! view-system (:refresh-interval options) (:worker-threads options))
     (when (some-> stats-log-interval pos?)
       (statistics/start-logger! view-system stats-log-interval))
     view-system))
  ([options]
   (init! (atom {}) options)))

(defn shutdown!
  "Shuts the view system down, terminating all worker threads and clearing
   all view subscriptions and data."
  [view-system & [dont-wait-for-threads?]]
  (debug "shutting down views sytem")
  (stop-update-watcher! view-system dont-wait-for-threads?)
  (if (:logging? @view-system)
    (statistics/stop-logger! view-system dont-wait-for-threads?))
  (reset! view-system {})
  view-system)

