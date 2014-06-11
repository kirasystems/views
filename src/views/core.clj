(ns views.core
  (:require
   [views.base-subscribed-views :as bsv]
   [views.persistence :as vp])
  (:import
   [views.persistence InMemoryPersistence]
   [views.base_subscribed_views BaseSubscribedViews]))

(defn config
  [{:keys [db schema templates persistence] :as opts}]
  (let [opts (if persistence opts (assoc opts :persistence (InMemoryPersistence.)))]
    {:db db :schema schema :templates templates :base-subscribed-views (BaseSubscribedViews. opts)}))
