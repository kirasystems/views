(ns views.db.checks
  (:require
   [views.db.honeysql :as vh]
   [clojure.set :refer [intersection]]
   [clojure.zip :as z]
   [zip.visit :as zv]
   [honeysql.core :as hsql]))

(defn replace-param-pred
  []
  (zv/visitor
   :pre [n s]
   (if (and (coll? n) (string? (last n)) (= (subs (last n) 0 1) "?"))
     {:node true
      :state (conj s n)})))

(defn swap-wc-preds
  [wc]
  (let [root (z/vector-zip wc)]
    (zv/visit root nil [(replace-param-pred)])))

(defn swap-preds
  [vm]
  (let [{:keys [node state]} (swap-wc-preds (:where vm))]
    {:q (assoc vm :where node) :p state}))

(defn view-sig->dummy-args
  [view-sig]
  (map #(str "?" %) (range 0 (count (rest view-sig)))))

(defn view-check
  [action dummy-vm]
  (let [{:keys [p q]} (swap-preds dummy-vm)]
    (-> q
        (update-in [:where] #(merge % (:where action)))
        (assoc :select (mapv second p)))))

(defn have-overlapping-tables?
  "Takes two Honeysql hash-maps, one for action, one for view, and returns
   boolean value representing whether or not their set of tables intersect."
  [action view]
  (->> [action view]
       (map (comp set #(map first %) vh/extract-tables))
       (apply intersection)
       seq
       boolean))
