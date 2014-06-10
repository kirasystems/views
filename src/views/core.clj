(ns views.core
  (:require
   [views.base-subscribed-views])
  (:import
   [views.base_subscribed_views BaseSubscribedViews]))

(defmacro config
  [{:keys [db schema] :as opts}]
  {:db db :schema schema :subscribed-views (BaseSubscribedViews. opts)})
