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
  [subscriber-key view-sig templates namespace]
  (fn [svs]
    (-> svs
        (update-in [namespace view-sig :subscriptions] (add-subscriber-key subscriber-key))
        (assoc-in  [namespace view-sig :view-data]     (vdb/view-map (get-in templates [(first view-sig) :fn]) view-sig)))))

(defn add-subscription!
  ([subscriber-key view-sig templates]
     (add-subscription! subscriber-key view-sig templates default-ns))
  ([subscriber-key view-sig templates namespace]
     (swap! subscribed-views (add-subscription* subscriber-key view-sig templates namespace))))

(defn add-subscriptions!
  ([subscriber-key view-sigs templates]
     (add-subscriptions! subscriber-key view-sigs templates default-ns))
  ([subscriber-key view-sigs templates namespace]
     (mapv #(add-subscription! subscriber-key % templates namespace) view-sigs)))

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
  ([subscriber-key view-sig]
     (subscribed-to? subscriber-key view-sig default-ns))
  ([subscriber-key view-sig namespace]
     (if-let [view-subs (subscribed-to view-sig namespace)]
       (view-subs subscriber-key))))

(defn- remove-key-or-view
  [subscriber-key view-sig namespace]
  (fn [subbed-views]
    (let [path    [namespace view-sig :subscriptions]
          updated (update-in subbed-views path disj subscriber-key)]
      (if (seq (get-in updated path))
        updated
        (update-in updated [namespace] dissoc view-sig)))))

(defn remove-subscription!
  ([subscriber-key view-sig]
     (remove-subscription! subscriber-key view-sig default-ns))
  ([subscriber-key view-sig namespace]
     (when (subscribed-to? subscriber-key view-sig namespace)
       (swap! subscribed-views (remove-key-or-view subscriber-key view-sig namespace)))))

(defn compiled-view-for
  ([view-sig] (compiled-view-for view-sig default-ns))
  ([view-sig namespace]
     (get-in @subscribed-views [namespace view-sig :view-data])))
