(ns views.statistics
  (:require
    [clojure.tools.logging :refer [info error debug]]))

(defn active-view-count
  "Returns a count of views with at least one subscriber."
  [view-system]
  (count (remove #(empty? (val %)) (:subscribers @view-system))))

(defn collecting?
  "Whether view statem statistics collection and logging is enabled or not."
  [view-system]
  (boolean (get-in @view-system [:statistics :logger])))

(defn reset-stats!
  "Resets statistics collected back to zero."
  [view-system]
  (swap! view-system update-in [:statistics] assoc
         :refreshes 0
         :dropped 0
         :deduplicated 0)
  view-system)

(defn refreshed!
  "Record a refresh event."
  [view-system]
  (swap! view-system update-in [:statistics :refreshes] inc))

(defn dropped!
  "Record a dropped refresh."
  [view-system]
  (swap! view-system update-in [:statistics :dropped] inc))

(defn deduplicated!
  "Record a deduplicated refresh."
  [view-system]
  (swap! view-system update-in [:statistics :deduplicated] inc))

(defn- logger-thread
  [view-system msecs]
  (let [secs (/ msecs 1000)]
    (fn []
      (try
        (Thread/sleep msecs)
        (let [stats (:statistics @view-system)]
          (reset-stats! view-system)
          (info "subscribed views:" (active-view-count view-system)
                (format "refreshes/sec: %.1f" (double (/ (:refreshes stats) secs)))
                (format "dropped/sec: %.1f" (double (/ (:dropped stats) secs)))
                (format "deduped/sec: %.1f" (double (/ (:deduplicated stats) secs)))))
        (catch InterruptedException e))
      (if-not (get-in @view-system [:statistics :stop?])
        (recur)))))

(defn start-logger!
  "Starts a logger thread that will enable collection of view statistics
   which the logger will periodically write out to the log."
  [view-system log-interval]
  (debug "starting logger. logging at" log-interval "secs intervals")
  (if (get-in @view-system [:statistics :logger])
    (error "cannot start new logger thread until existing thread is stopped")
    (let [logger (Thread. ^Runnable (logger-thread view-system log-interval))]
      (swap! view-system update-in [:statistics] assoc
             :logger logger
             :stop? false)
      (reset-stats! view-system)
      (.start logger)
      (swap! view-system assoc :logging? true)))
  view-system)

(defn stop-logger!
  "Stops the logger thread."
  [view-system & [dont-wait-for-thread?]]
  (debug "stopping logger")
  (let [^Thread logger-thread (get-in @view-system [:statistics :logger])]
    (swap! view-system assoc-in [:statistics :stop?] true)
    (if logger-thread (.interrupt logger-thread))
    (if-not dont-wait-for-thread? (.join logger-thread))
    (swap! view-system assoc-in [:statistics :logger] nil))
  view-system)
