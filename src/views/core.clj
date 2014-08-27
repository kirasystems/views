(ns views.core
  (:require
   [views.base-subscribed-views :as bsv]
   [views.core :as vp]
   [edl.schema :refer [denormalized-schema get-schema]]
   [views.persistence.memory :refer [new-memory-persistence]])
  (:import
   [views.base_subscribed_views BaseSubscribedViews]))

(defn config
  [{:keys [db templates persistence vexec-ns-fn] :as conf}]
  (let [schema (denormalized-schema (get-schema db (get conf :schema-name "public")))
        conf   (if persistence conf (assoc conf :persistence (new-memory-persistence)))]
    {:db                    db
     :schema                schema
     :templates             templates
     :vexec-ns-fn           vexec-ns-fn
     :base-subscribed-views (BaseSubscribedViews. conf)}))
