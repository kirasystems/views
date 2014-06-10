(ns views.core-test
  (:require
   [clojure.test :refer [use-fixtures deftest is]]
   [edl.core :refer [defschema]]
   [views.fixtures :as vf]
   [views.subscribed-views :as vs]
   [views.core :refer [config]]))

(defschema schema vf/db "public")

(deftest configures-views
  (let [conf (config {:db vf/db :schema schema :templates vf/templates :unsafe? true})]
    ;; wtf is this false?!
    (is (satisfies? views.subscribed-views/ISubscribedViews (:subscribed-views conf)))))
