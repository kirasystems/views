(ns views.core-test
  (:require
   [clojure.test :refer [use-fixtures deftest is]]
   [edl.core :refer [defschema]]
   [views.fixtures :as vf]
   [views.subscribed-views :as vs] ;; :refer [SubscribedViews]]
   [views.core :refer [config]]))

(defschema schema vf/db "public")

(deftest configures-views
  (let [conf (config {:db vf/db :schema schema :templates vf/templates :unsafe? true})]
    (println (satisfies? views.subscribed-views/SubscribedViews (:subscribed-views conf))) ; wtf?!
    ;; (is (satisfies? views.subscribed-views/SubscribedViews (:subscribed-views conf)))))
    ))
