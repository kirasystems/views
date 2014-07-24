(ns views.base-subscribed-views-test
  (:require
   [views.base-subscribed-views :as bsv]
   [views.persistence]
   [views.subscribed-views :refer [subscribe-views unsubscribe-views disconnect broadcast-deltas]]
   [views.subscriptions :as vs :refer [add-subscription! default-ns subscribed-to?]]
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

(use-fixtures :each (vf/database-fixtures!) subscription-fixtures!)

(def persistence (InMemoryPersistence.))

(def view-config-fixture
  {:persistence persistence
   :db vf/db
   :templates vf/templates
   :view-sig-fn :views
   :unsafe? true})

(deftest subscribes-and-dispatches-initial-view-result-set
  (let [send-fn #(is (and (= %1 1) (= %2 :views.init) (= %3 {[:users] []})))
        base-subbed-views (BaseSubscribedViews. (assoc view-config-fixture :send-fn send-fn))]
    (subscribe-views base-subbed-views {:subscriber-key 1 :views [[:users]]})))

(deftest unsubscribes-view
  (let [base-subbed-views (BaseSubscribedViews. view-config-fixture)]
    (subscribe-views base-subbed-views {:subscriber-key 1 :views [[:users]]})
    (unsubscribe-views base-subbed-views {:subscriber-key 1 :views [[:users]]})
    (is (not (subscribed-to? 1 [:users])))))

(deftest filters-subscription-requests
  (let [templates         (assoc-in vf/templates [:users :filter-fn]
                                    (fn [msg _] (:authorized? msg)))
        view-config (-> view-config-fixture (assoc :templates templates) (dissoc :unsafe?))
        base-subbed-views (BaseSubscribedViews. view-config)]
    (subscribe-views base-subbed-views {:subscriber-key 1 :views [[:users]]})
    (is (not (subscribed-to? 1 [:users])))))

(deftest removes-all-subscriptions-on-disconnect
  (let [base-subbed-views (BaseSubscribedViews. view-config-fixture)]
    (subscribe-views base-subbed-views {:subscriber-key 1 :views [[:users][:user-posts 1]]})
    (disconnect base-subbed-views {:subscriber-key 1})
    (is (not (subscribed-to? 1 [:user-posts 1])))
    (is (not (subscribed-to? 1 [:users])))))

(deftest sends-deltas
  (let [deltas {[:users] [{:view-sig [:users] :insert-deltas [{:foo "bar"}]}]}
        sent-delta {[:users] {:insert-deltas [{:foo "bar"}]}}
        send-fn #(do (is (#{1 2} %1))
                     (is (= %2 :views.deltas))
                     (is (= %3 sent-delta)))
        base-subbed-views (BaseSubscribedViews. (assoc view-config-fixture :send-fn send-fn))]
    (add-subscription! [:users] vf/templates 1 default-ns)
    (add-subscription! [:users] vf/templates 2 default-ns)
    (broadcast-deltas base-subbed-views deltas nil)))

(deftest sends-deltas-in-batch
  (let [deltas {[:users] [{:view-sig [:users] :insert-deltas [{:id 1 :name "Bob"} {:id 2 :name "Alice"}]}]}
        sent-delta {[:users] {:insert-deltas [{:id 1 :name "Bob"} {:id 2 :name "Alice"}]}}
        send-fn #(do (is (#{1 2} %1))
                     (is (= %2 :views.deltas))
                     (is (= %3 sent-delta)))
        base-subbed-views (BaseSubscribedViews. (assoc view-config-fixture :send-fn send-fn))]
    (add-subscription! [:users] vf/templates 1 default-ns)
    (broadcast-deltas base-subbed-views deltas nil)))
