(ns views.subscriptions
  (:require
   [views.db.core :as vdb]))

;;
;; {namespace {[:view-sig 1 "arg2"] {:subscriptions [1 2 3 4 ... ] :view-data {:view ...}}}}
;;

(def subscribed-views (atom {}))

(def default-ns :default-ns)

(defn- add-subscriber-key
  [subscriber-key]
  (fn [view-subs]
    (if (seq view-subs)
      (conj view-subs subscriber-key)
      #{subscriber-key})))

(defn add-subscription*
  [view-sig templates subscriber-key namespace]
  (fn [svs]
    (-> svs
        (update-in [namespace view-sig :subscriptions] (add-subscriber-key subscriber-key))
        (assoc-in  [namespace view-sig :view-data]     (vdb/view-map (get-in templates [(first view-sig) :fn]) view-sig)))))

(defn add-subscription!
  ([view-sig templates subscriber-key]
     (add-subscription! view-sig templates subscriber-key default-ns))
  ([view-sig templates subscriber-key namespace]
     (swap! subscribed-views (add-subscription* view-sig templates subscriber-key namespace))))

(defn add-subscriptions!
  ([view-sigs templates subscriber-key]
     (add-subscriptions! view-sigs templates subscriber-key default-ns))
  ([view-sigs templates subscriber-key namespace]
     (mapv #(add-subscription! % templates subscriber-key namespace) view-sigs)))

(defn subscriptions-for
  ([subscriber-key] (subscriptions-for subscriber-key default-ns))
  ([subscriber-key namespace]
     (reduce
      #(if (contains? (:subscriptions (second %2)) subscriber-key)
         (conj %1 (first %2))
         %1)
      [] (get @subscribed-views namespace))))

(defn subscribed-to
  ([view-sig] (subscribed-to view-sig default-ns))
  ([view-sig namespace]
     (get-in @subscribed-views [namespace view-sig :subscriptions])))

(defn subscribed-to?
  ([view-sig subscriber-key]
     (subscribed-to? view-sig subscriber-key default-ns))
  ([view-sig subscriber-key namespace]
     (if-let [view-subs (subscribed-to view-sig namespace)]
       (view-subs subscriber-key))))

(defn- remove-key-or-view
  [view-sig subscriber-key namespace]
  (fn [subbed-views]
    (let [path    [namespace view-sig :subscriptions]
          updated (update-in subbed-views path disj subscriber-key)]
      (if (seq (get-in updated path))
        updated
        (update-in updated [namespace] dissoc view-sig)))))

(defn remove-subscription!
  ([view-sig subscriber-key]
     (remove-subscription! view-sig subscriber-key default-ns))
  ([view-sig subscriber-key namespace]
     (when (subscribed-to? view-sig subscriber-key namespace)
       (swap! subscribed-views (remove-key-or-view view-sig subscriber-key namespace)))))

(defn compiled-view-for
  ([view-sig] (compiled-view-for view-sig default-ns))
  ([view-sig namespace]
     (get-in @subscribed-views [namespace view-sig :view-data])))
