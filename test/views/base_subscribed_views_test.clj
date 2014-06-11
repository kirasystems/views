(ns views.base-subscribed-views-test
  (:require
   [views.base-subscribed-views :as bsv]
   [views.persistence]
   [views.subscribed-views :refer [subscribe-views unsubscribe-views disconnect]]
   [views.subscriptions :as vs :refer [subscribed-to?]]
   [views.fixtures :as vf]
   [clojure.test :refer [use-fixtures deftest is]]
   [clojure.java.jdbc :as j]
   [clj-logging-config.log4j :refer [set-logger! set-loggers!]])
  (:import
   [views.persistence InMemoryPersistence]
   [views.base_subscribed_views BaseSubscribedViews]))

(set-loggers!
 "views.base-subscribed-views" {:level :error}
 "views.filters"               {:level :error})

(defn- subscription-fixtures!
  [f]
  (reset! vs/subscribed-views {})
  (f))

(use-fixtures :each vf/database-fixtures! subscription-fixtures!)

(def persistence (InMemoryPersistence.))

(deftest subscribes-and-dispatches-initial-view-result-set
  (let [send-fn #(is (and (= %1 1) (= %2 {[:users] []})))
        base-subbed-views (BaseSubscribedViews. {:persistence persistence :db vf/db :templates vf/templates :send-fn send-fn :unsafe? true})]
    (subscribe-views base-subbed-views {:subscriber-key 1 :views [[:users]]})))

(deftest unsubscribes-view
  (let [base-subbed-views (BaseSubscribedViews. {:persistence persistence :db vf/db :templates vf/templates :unsafe? true})]
    (subscribe-views base-subbed-views {:subscriber-key 1 :views [[:users]]})
    (unsubscribe-views base-subbed-views {:subscriber-key 1 :views [[:users]]})
    (is (not (subscribed-to? 1 [:users])))))

(deftest filters-subscription-requests
  (let [templates         (assoc-in vf/templates [:users :filter-fn] (fn [msg _] (:authorized? msg)))
        base-subbed-views (BaseSubscribedViews. {:persistence persistence :db vf/db :templates templates})]
    (subscribe-views base-subbed-views {:subscriber-key 1 :views [[:users]]})
    (is (not (subscribed-to? 1 [:users])))))

(deftest removes-all-subscriptions-on-disconnect
  (let [base-subbed-views (BaseSubscribedViews. {:persistence persistence :db vf/db :templates vf/templates :unsafe? true})]
    (subscribe-views base-subbed-views {:subscriber-key 1 :views [[:users][:user-posts 1]]})
    (disconnect base-subbed-views {:subscriber-key 1})
    (is (not (subscribed-to? 1 [:user-posts 1])))
    (is (not (subscribed-to? 1 [:users])))))
