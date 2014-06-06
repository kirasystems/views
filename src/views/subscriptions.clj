(ns views.subscriptions
  (:require
   [views.db :as vdb]))

;;
;; {[:view-sig 1 "arg2"] {:keys [1 2 3 4 ... ] :view-map {:view ...}}}
;;
;;  or
;;
;; {prefix {[:view-sig 1 "arg2"] {:keys [1 2 3 4 ... ] :view-map {:view ...}}}}
;;

(def subscribed-views (atom {}))
(def compiled-views (atom {}))

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
     (swap! subscribed-views #(update-in % [view-sig] (add-subscriber-key subscriber-key)))
     (add-compiled-view! view-sig templates))
  ([subscriber-key view-sig templates prefix]
     (swap! subscribed-views #(update-in % [prefix view-sig] (add-subscriber-key subscriber-key)))
     (add-compiled-view! view-sig templates)))

(defn add-subscriptions!
  ([subscriber-key view-sigs templates]
     (add-subscriptions! subscriber-key view-sigs templates nil))
  ([subscriber-key view-sigs templates prefix]
     (doseq [vs view-sigs]
       (if prefix
         (add-subscription! subscriber-key vs templates prefix)
         (add-subscription! subscriber-key vs templates)))))

(defn subscribed-to
  ([view-sig]
     (get @subscribed-views view-sig))
  ([view-sig prefix]
     (get-in @subscribed-views [prefix view-sig])))

(defn subscribed-to?
  ([subscriber-key view-sig]
     (subscribed-to? subscriber-key view-sig nil))
  ([subscriber-key view-sig prefix]
     (if-let [view-subs (if prefix (subscribed-to view-sig prefix) (subscribed-to view-sig))]
       (view-subs subscriber-key))))

(defn- remove-key-or-view
  [subscriber-key view-sig prefix]
  (fn [subbed-views]
    (let [path    (if prefix [prefix view-sig] [view-sig])
          updated (update-in subbed-views path disj subscriber-key)]
      (if (seq (get-in updated path))
        updated
        (do (swap! compiled-views dissoc view-sig) ; remove the compiled view as well
            (if prefix
              (update-in updated [prefix] dissoc view-sig)
              (dissoc updated view-sig)))))))

(defn remove-subscription!
  ([subscriber-key view-sig]
     (remove-subscription! subscriber-key view-sig nil))
  ([subscriber-key view-sig prefix]
     (when (subscribed-to? subscriber-key view-sig (if prefix prefix))
       (swap! subscribed-views (remove-key-or-view subscriber-key view-sig prefix)))))

(defn compiled-view-for
  [view-sig]
  (get @compiled-views view-sig))
