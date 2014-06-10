(ns views.core
  (:require
   [views.base-subscribed-views :as bsv]; :refer [BaseSubscribedViews]]
   [views.persistor :as vp])); :refer [InMemoryPersistor]]))
  ;; (:import
  ;;  [views.persistor InMemoryPersistor]
  ;;  [views.base_subscribed_views BaseSubscribedViews]))

(defmacro config
  [{:keys [db schema persistor] :as opts}]
  (let [opts (if persistor opts (assoc opts :persistor (vp/->InMemoryPersistor)))]
    {:db db :schema schema :base-subscribed-views (bsv/->BaseSubscribedViews opts)}))
