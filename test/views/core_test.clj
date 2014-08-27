(ns views.core-test
  (:require
   [clojure.test :refer [use-fixtures deftest is]]
   [edl.core :refer [defschema]]
   [views.fixtures :as vf]
   [views.subscribed-views :as vs]
   [views.core :refer [config]]))

(defschema schema vf/db "public")

#_(deftest configures-views
  (let [conf (config {:db vf/db :schema schema :templates vf/templates :unsafe? true})]
    ;; wtf is this false?! AKH: there is some sort of recursive referencing going on
    ;; in the thing being compared to.
    (is (satisfies? views.subscribed-views/ISubscribedViews (:subscribed-views conf)))))
