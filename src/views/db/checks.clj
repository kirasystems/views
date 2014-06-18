(ns views.db.checks
  (:require
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
  [action view-fn view-sig]
  (let [view-map (apply view-fn (view-sig->dummy-args view-sig))
        {:keys [p q]} (swap-preds view-map)]
    (-> q
        (update-in [:where] #(merge % (:where action)))
        (assoc :select (mapv second p)))))
