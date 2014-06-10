(ns views.base-subscribed-views-test
  (:require
   [views.base-subscribed-views :as bsv] ; :refer [BaseSubscribedViews]]
   [views.subscribed-views :refer [SubscribedViews subscriber-key-fn prefix-fn subscribe-views unsubscribe-views disconnect]]
   [views.subscriptions :as vs :refer [subscribed-to?]]
   [views.fixtures :as vf]
   [clojure.test :refer [use-fixtures deftest is]]
   [clojure.java.jdbc :as j]
   [clj-logging-config.log4j :refer [set-logger! set-loggers!]])
  (:import
   [views.base_subscribed_views BaseSubscribedViews]))

(set-loggers! "views.base-subscribed-views" {:level :error})

(defn- subscription-fixtures!
  [f]
  (reset! vs/subscribed-views {})
  (reset! vs/compiled-views {})
  (f))

(use-fixtures :each vf/database-fixtures! subscription-fixtures!)

(deftest subscribes-and-dispatches-initial-view-result-set
  (let [send-fn #(is (and (= %2 1) (= %3 {[:users] []})))
        base-subbed-views (BaseSubscribedViews. vf/db vf/templates send-fn nil nil {:unsafe true})]
    (subscribe-views base-subbed-views {:subscriber-key 1 :views [[:users]]})))

(deftest unsubscribes-view
  (let [base-subbed-views (BaseSubscribedViews. vf/db vf/templates nil nil nil {:unsafe true})]
    (subscribe-views base-subbed-views {:subscriber-key 1 :views [[:users]]})
    (unsubscribe-views base-subbed-views {:subscriber-key 1 :views [[:users]]})
    (is (not (subscribed-to? 1 [:users])))))

(deftest filters-subscription-requests
  (let [templates (assoc-in vf/templates [:users :filter-fn] (fn [msg _] (:authorized? msg)))
        base-subbed-views (BaseSubscribedViews. vf/db templates nil nil nil nil)]
    (subscribe-views base-subbed-views {:subscriber-key 1 :views [[:users]]})
    (is (not (subscribed-to? 1 [:users])))))

(deftest removes-all-subscriptions-on-disconnect
  (let [base-subbed-views (BaseSubscribedViews. vf/db vf/templates nil nil nil {:unsafe true})]
    (subscribe-views base-subbed-views {:subscriber-key 1 :views [[:users][:user-posts 1]]})
    (disconnect base-subbed-views {:subscriber-key 1})
    (is (not (subscribed-to? 1 [:user-posts 1])))
    (is (not (subscribed-to? 1 [:users])))))
