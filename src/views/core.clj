(ns views.core
  (:require
   [views.base-subscribed-views :as bsv]
   [views.persistence :as vp])
  (:import
   [views.persistence InMemoryPersistence]
   [views.base_subscribed_views BaseSubscribedViews]))

(defn config
  [{:keys [db schema persistence] :as opts}]
  (let [opts (if persistence opts (assoc opts :persistence (InMemoryPersistence.)))]
    {:db db :schema schema :base-subscribed-views (BaseSubscribedViews. opts)}))
