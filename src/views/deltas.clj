(ns view.deltas)

;; ;; This preprocesses a batch of view deltas. They get packaged into batches for each
;; ;; subscriber in send-delta.
;; (defn preprocess-delta
;;   "Returns a pair [firm_id, deltas]."
;;   [fdb templates v]
;;   (let [insert-deltas (post-process-result-sets templates [[(:view-sig v) (:insert-deltas v)]])
;;         delete-deltas (post-process-result-sets templates [[(:view-sig v) (:delete-deltas v)]])
;;         deltas        [(:view-sig v) {:insert-deltas (seq (last (last insert-deltas)))
;;                                       :delete-deltas (seq (last (last delete-deltas)))}]]
;;     [(:fid fdb) deltas]))

;; (defn build-delta-batch
;;   [fdb templates views-with-deltas]
;;   (doall (map #(preprocess-delta fdb templates %) views-with-deltas)))


;; The following creates delta batches for each subscriber.

;; If we have a batch of deltas:
;; - there are multiple views
;; - each view has a list of clients
;; - produce client -> deltas

(defn add-message
  [msg batches subscriber]
  (debug "DELTAs to S: " msg " | " subscriber)
  (update-in batches [subscriber] (fnil conj []) msg))

(defn build-messages
  [delta-batch]
  (loop [batches {}, deltas (seq delta-batch)]
    (if-not deltas
      batches
      (let [[firm_id [view-sig delta]] (first deltas)
            subscribed      (get-in @subscribed-views [firm_id view-sig :sessions])]
        (debug "\n\nSUBSCRIBED: " subscribed)
        (recur (reduce #(add-message {view-sig delta} %1 %2) batches subscribed) (next deltas))))))
