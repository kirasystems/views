(ns views.subscriptions
  (:require
   [views.db.core :as vdb]))

;;
;; {[:view-sig 1 "arg2"] {:keys [1 2 3 4 ... ] :view-map {:view ...}}}
;;
;;  or
;;
;; {namespace {[:view-sig 1 "arg2"] {:keys [1 2 3 4 ... ] :view-map {:view ...}}}}
;;

(def subscribed-views (atom {}))
(def compiled-views (atom {}))

(def default-ns :default-ns)

(defn- add-subscriber-key
  [subscriber-key]
  (fn [view-subs]
    (if (seq view-subs)
      (conj view-subs subscriber-key)
      #{subscriber-key})))

(defn- add-compiled-view!
  [view-sig templates]
  (swap! compiled-views #(assoc % view-sig (vdb/view-map (get-in templates [(first view-sig) :fn]) view-sig))))

(defn add-subscription!
  ([subscriber-key view-sig templates]
     (add-subscription! subscriber-key view-sig templates default-ns))
  ([subscriber-key view-sig templates namespace]
     (swap! subscribed-views #(update-in % [namespace view-sig] (add-subscriber-key subscriber-key)))
     (add-compiled-view! view-sig templates)))

(defn add-subscriptions!
  ([subscriber-key view-sigs templates]
     (add-subscriptions! subscriber-key view-sigs templates default-ns))
  ([subscriber-key view-sigs templates namespace]
     (last (mapv #(add-subscription! subscriber-key % templates namespace) view-sigs))))

(defn subscriptions-for
  ([subscriber-key] (subscriptions-for subscriber-key default-ns))
  ([subscriber-key namespace]
     (reduce #(if (contains? (second %2) subscriber-key) (conj %1 (first %2)) %1) [] (get @subscribed-views namespace))))

(defn subscribed-to
  ([view-sig] (subscribed-to view-sig default-ns))
  ([view-sig namespace]
     (get-in @subscribed-views [namespace view-sig])))

(defn subscribed-to?
  ([subscriber-key view-sig]
     (subscribed-to? subscriber-key view-sig default-ns))
  ([subscriber-key view-sig namespace]
     (if-let [view-subs (subscribed-to view-sig namespace)]
       (view-subs subscriber-key))))

(defn- remove-key-or-view
  [subscriber-key view-sig namespace]
  (fn [subbed-views]
    (let [path    [namespace view-sig]
          updated (update-in subbed-views path disj subscriber-key)]
      (if (seq (get-in updated path))
        updated
        (do (swap! compiled-views dissoc view-sig) ; remove the compiled view as well
            (update-in updated [namespace] dissoc view-sig))))))

(defn remove-subscription!
  ([subscriber-key view-sig]
     (remove-subscription! subscriber-key view-sig default-ns))
  ([subscriber-key view-sig namespace]
     (when (subscribed-to? subscriber-key view-sig namespace)
       (swap! subscribed-views (remove-key-or-view subscriber-key view-sig namespace)))))

(defn compiled-view-for
  [view-sig]
  (get @compiled-views view-sig))
