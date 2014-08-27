(ns views.persistence.memory
  (:require
    [views.persistence.core :refer :all]
    [views.db.deltas :as vd]))

(defn ns-subscribe!
  "Subscribe to a view inside a namespace."
  [namespace-views view-sig templates subscriber-key]
  (-> namespace-views
      (update-in [view-sig :subscriptions] (fnil conj #{}) subscriber-key)
      (assoc-in  [view-sig :view-data]     (vd/view-map (get-in templates [(first view-sig) :fn]) view-sig))))

(defn ns-unsubscribe!
  "Unsubscribe from a view inside a namespace. If there are no more subscribers,
  we remove the view itself as well."
  [namespace-views view-sig subscriber-key]
  (let [path    [view-sig :subscriptions]
        updated (update-in namespace-views path disj subscriber-key)]
    (if (seq (get-in updated path))
      updated
      (dissoc updated view-sig))))

(defn ns-unsubscribe-all!
  "Unsubscribe a subscriber from all views in a namespace."
  [namespace-views subscriber-key]
  (reduce #(ns-unsubscribe! %1 %2 subscriber-key) namespace-views (keys namespace-views)))

(defn ns-subscriptions
  "Find subscribers for a signature and add to a map."
  [namespace-views result-map sig]
  (if-let [subscribers (get-in namespace-views [sig :subscriptions])]
    (assoc result-map sig subscribers)
    result-map))

(deftype ViewsMemoryPersistence [subbed-views]
  IPersistence
  (subscribe!
    [this db templates namespace view-sig subscriber-key]
    (let [sv (swap! subbed-views (fn [sv] (update-in sv [namespace] ns-subscribe! view-sig templates subscriber-key)))]
      (get-in sv [namespace view-sig :view-data])))

  (unsubscribe!
    [this namespace view-sig subscriber-key]
    (swap! subbed-views
           (fn [sv] (update-in sv [namespace] ns-unsubscribe! view-sig subscriber-key))))

  (unsubscribe-all!
    [this namespace subscriber-key ]
    (swap! subbed-views
           (fn [sv] (update-in sv [namespace] ns-unsubscribe-all! subscriber-key))))

  (view-data [this namespace table]
    ;; We don't yet use table name as an optimization here.
    (map :view-data (vals (get @subbed-views namespace))))

  (subscriptions [this namespace signatures]
    (let [namespace-views (get @subbed-views namespace)]
      (reduce #(ns-subscriptions namespace-views %1 %2) {} signatures))))

(defn new-memory-persistence
  []
  (->ViewsMemoryPersistence (atom {})))
