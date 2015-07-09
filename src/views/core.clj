(ns views.core
  (:import
    [java.util.concurrent ArrayBlockingQueue TimeUnit])
  (:require
    [views.protocols :refer [IView id data relevant?]]
    [plumbing.core :refer [swap-pair!]]
    [clojure.tools.logging :refer [info debug error]]
    [environ.core :refer [env]]))

;; The view-system data structure has this shape:
;;
;; {:views {:id1 view1, id2 view2, ...}
;;  :send-fn (fn [subscriber-key data] ...)
;;
;;  :hashes {view-sig hash, ...}
;;  :subscribed {subscriber-key #{view-sig, ...}}
;;  :subscribers {view-sig #{subscriber-key, ...}}
;;  :hints #{hint1 hint2 ...}
;;
;;  }
;;
;;  Each hint has the form {:namespace x :hint y}

(def refresh-queue-size
  (if-let [n (:views-refresh-queue-size env)]
    (Long/parseLong n)
    1000))

(def statistics (atom {}))

(defn reset-stats!
  []
  (swap! statistics (fn [s] {:enabled (boolean (:enabled s)), :refreshes 0, :dropped 0, :deduplicated 0})))

(defn collect-stats? [] (:enabled @statistics))

(reset-stats!)


(def refresh-queue (ArrayBlockingQueue. refresh-queue-size))

(defn subscribe-view!
  [view-system view-sig subscriber-key data-hash]
  (-> view-system
      (update-in [:subscribed subscriber-key] (fnil conj #{}) view-sig)
      (update-in [:subscribers view-sig] (fnil conj #{}) subscriber-key)
      (update-in [:hashes view-sig] #(or % data-hash)))) ;; see note #1

(defn subscribe!
  [view-system namespace view-id parameters subscriber-key]
  (when-let [view (get-in @view-system [:views view-id])]
    (future
      (try
        (let [vdata (data view namespace parameters)]
          (swap! view-system subscribe-view! [namespace view-id parameters] subscriber-key (hash vdata))
          ((get @view-system :send-fn) subscriber-key [[view-id parameters] vdata]))
        (catch Exception e
          (error "error subscribing:" namespace view-id parameters
                 "e:" e "msg:" (.getMessage e)))))))

(defn remove-from-subscribers
  [view-system view-sig subscriber-key]
  (update-in view-system [:subscribers view-sig] disj subscriber-key))

(defn unsubscribe!
  [view-system namespace view-id parameters subscriber-key]
  (swap! view-system
         (fn [vs]
           (-> vs
               (update-in [:subscribed subscriber-key] disj [namespace view-id parameters])
               (remove-from-subscribers [namespace view-id parameters] subscriber-key)))))

(defn unsubscribe-all!
  "Remove all subscriptions by a given subscriber."
  [view-system subscriber-key]
  (swap! view-system
         (fn [vs]
           (let [view-sigs (get-in vs [:subscribed subscriber-key])
                 vs*       (update-in vs [:subscribed] dissoc subscriber-key)]
             (reduce #(remove-from-subscribers %1 %2 subscriber-key) vs* view-sigs)))))

(defn refresh-view!
  "We refresh a view if it is relevant and its data hash has changed."
  [view-system hints [namespace view-id parameters :as view-sig]]
  (let [v (get-in @view-system [:views view-id])]
    (try
      (if (relevant? v namespace parameters hints)
        (if-not (.contains ^ArrayBlockingQueue refresh-queue view-sig)
          (when-not (.offer ^ArrayBlockingQueue refresh-queue view-sig)
            (when (collect-stats?) (swap! statistics update-in [:dropped] inc))
            (error "refresh-queue full, dropping refresh request for" view-sig))
          (do
            (when (collect-stats?) (swap! statistics update-in [:deduplicated] inc))
            (debug "already queued for refresh" view-sig))))
      (catch Exception e (error "error determining if view is relevant, view-id:"
                                view-id "e:" e)))))

(defn subscribed-views
  [view-system]
  (reduce into #{} (vals (:subscribed view-system))))

(defn active-view-count
  "Returns a count of views with at least one subscriber."
  [view-system]
  (count (remove #(empty? (val %)) (:subscribers view-system))))

(defn pop-hints!
  "Return hints and clear hint set atomicly."
  [view-system]
  (let [p (swap-pair! view-system assoc :hints #{})]
    (or (:hints (first p)) #{})))

(defn refresh-views!
  "Given a collection of hints, or a single hint, find all dirty views and schedule them for a refresh."
  ([view-system hints]
   (debug "refresh hints:" hints)
   (mapv #(refresh-view! view-system hints %) (subscribed-views @view-system))
   (swap! view-system assoc :last-update (System/currentTimeMillis)))
  ([view-system]
   (refresh-views! view-system (pop-hints! view-system))))

(defn can-refresh?
  [last-update min-refresh-interval]
  (> (- (System/currentTimeMillis) last-update) min-refresh-interval))

(defn wait
  [last-update min-refresh-interval]
  (Thread/sleep (max 0 (- min-refresh-interval (- (System/currentTimeMillis) last-update)))))

(defn worker-thread
  "Handles refresh requests."
  [view-system]
  (fn []
    (when-let [[namespace view-id parameters :as view-sig] (.poll ^ArrayBlockingQueue refresh-queue 60 TimeUnit/SECONDS)]
      (when (collect-stats?) (swap! statistics update-in [:refreshes] inc))
      (try
        (let [view  (get-in @view-system [:views view-id])
              vdata (data view namespace parameters)
              hdata (hash vdata)]
          (when-not (= hdata (get-in @view-system [:hashes view-sig]))
            (doseq [s (get-in @view-system [:subscribers view-sig])]
              ((:send-fn @view-system) s [[view-id parameters] vdata]))
            (swap! view-system assoc-in [:hashes view-sig] hdata)))
        (catch Exception e
          (error "error refreshing:" namespace view-id parameters
                 "e:" e "msg:" (.getMessage e)))))
    (recur)))

(defn update-watcher!
  "A single threaded view update mechanism."
  [view-system min-refresh-interval threads]
  (swap! view-system assoc :last-update 0)
  (.start (Thread. (fn [] (let [last-update (:last-update @view-system)]
                            (try
                              (if (can-refresh? last-update min-refresh-interval)
                                (refresh-views! view-system)
                                (wait last-update min-refresh-interval))
                              (catch Exception e (error "exception in views e:" e  "msg:"(.getMessage e))))
                            (recur)))))
  (dotimes [i threads] (.start (Thread. ^Runnable (worker-thread view-system)))))

(defn log-statistics!
  "Run a thread that logs statistics every msecs."
  [view-system msecs]
  (swap! statistics assoc-in [:enabled] true)
  (let [secs (/ msecs 1000)]
    (.start (Thread. (fn []
                       (Thread/sleep msecs)
                       (let [stats @statistics]
                         (reset-stats!)
                         (info "subscribed views:" (active-view-count @view-system)
                               (format "refreshes/sec: %.1f" (double (/ (:refreshes stats) secs)))
                               (format "dropped/sec: %.1f" (double (/ (:dropped stats) secs)))
                               (format "deduped/sec: %.1f" (double (/ (:deduplicated stats) secs))))
                         (recur)))))))

(defn hint
  "Create a hint."
  [namespace hint]
  {:namespace namespace :hint hint})

(defn add-hint!
  "Add a hint to the system."
  [view-system hint]
  (swap! view-system update-in [:hints] (fnil conj #{}) hint))

(defn add-views!
  "Add a collection of views to the system."
  [view-system views]
  (swap! view-system update-in [:views] (fnil into {}) (map vector (map id views) views)))

(comment
  (defrecord SQLView [id query-fn]
    IView
    (id [_] id)
    (data [_ namespace parameters]
      (j/query (db/firm-connection namespace) (hsql/format (apply query-fn parameters))))
    (relevant? [_ namespace parameters hints]
      (let [tables (query-tables (apply query-fn parameters))]
        (boolean (some #(not-empty (intersection % talbes)) hints)))))

  (def memory-system (atom {}))

  (reset! memory-system {:a {:foo 1 :bar 200 :baz [1 2 3]}
                         :b {:foo 2 :bar 300 :baz [2 3 4]}})

  (defrecord MemoryView [id ks]
    IView
    (id [_] id)
    (data [_ namespace parameters]
      (get-in @memory-system (-> [namespace] (into ks) (into parameters))))
    (relevant? [_ namespace parameters hints]
      (some #(and (= namespace (:namespace %)) (= ks (:hint %))) hints)))

  (def view-system
    (atom
      {:views   {:foo (MemoryView. :foo [:foo])
                 :bar (MemoryView. :bar [:bar])
                 :baz (MemoryView. :baz [:baz])}
       :send-fn (fn [subscriber-key data] (println "sending to:" subscriber-key "data:" data))}))

  (subscribe! view-system :a :foo [] 1)
  (subscribe! view-system :b :foo [] 2)
  (subscribe! view-system :b :baz [] 2)

  (subscribed-views @view-system)

  (doto view-system
    (add-hint! [:foo])
    (add-hint! [:baz]))


  (refresh-views! view-system)

  ;; Example of function that updates and hints the view system.
  (defn massoc-in!
    [memory-system namespace ks v]
    (let [ms (swap! memory-system assoc-in (into [namespace] ks) v)]
      (add-hint! view-system ks)
      ms))

  (massoc-in! memory-system :a [:foo] 1)
  (massoc-in! memory-system :b [:baz] [2 4 3])


  (start-update-watcher! view-system 1000)

  )
