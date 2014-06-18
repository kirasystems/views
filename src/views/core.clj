(ns views.core
  (:require
   [views.base-subscribed-views :as bsv]
   [views.persistence :as vp]
   [edl.schema :refer [denormalized-schema get-schema]])
  (:import
   [views.persistence InMemoryPersistence]
   [views.base_subscribed_views BaseSubscribedViews]))

(defn config
  [{:keys [db templates persistence] :as conf}]
  (let [schema (denormalized-schema (get-schema db (get conf :schema-name "public")))
        conf (if persistence conf (assoc conf :persistence (InMemoryPersistence.)))]
    {:db db :schema schema :templates templates :base-subscribed-views (BaseSubscribedViews. conf)}))
