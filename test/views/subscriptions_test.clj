(ns views.subscriptions-test
  (:require
   [clojure.test :refer [use-fixtures deftest is]]
   [views.fixtures :refer [templates user-posts-tmpl]]
   [views.subscriptions :as vs]))

(defn- reset-subscribed-views!
  [f]
  (reset! vs/subscribed-views {})
  (f))

(use-fixtures :each reset-subscribed-views!)

(deftest adds-a-subscription
  (let [key 1, view-sig [:user-posts 1]]
    (vs/add-subscription! view-sig templates key)
    (is (vs/subscribed-to? view-sig key))))

(deftest can-use-namespace
  (let [namespace1 1, namespace2 2, key 1, view-sig [:user-posts 1]]
    (vs/add-subscription! view-sig templates key namespace1)
    (vs/add-subscription! view-sig templates key namespace2)
    (is (vs/subscribed-to? view-sig key namespace1))
    (is (vs/subscribed-to? view-sig key namespace2))))

(deftest removes-a-subscription
  (let [key 1, view-sig [:user-posts 1]]
    (vs/add-subscription! view-sig templates key)
    (vs/remove-subscription! view-sig key)
    (is (not (vs/subscribed-to? view-sig key)))))

(deftest doesnt-fail-or-create-view-entry-when-empty
  (vs/remove-subscription! 1 [:user-posts 1])
  (is (= {} @vs/subscribed-views)))

(deftest removes-a-subscription-with-namespace
  (let [namespace1 1, namespace2 2, key 1, view-sig [:user-posts 1]]
    (vs/add-subscription! view-sig templates key namespace1)
    (vs/add-subscription! view-sig templates key namespace2)
    (vs/remove-subscription! view-sig key namespace1)
    (is (not (vs/subscribed-to? view-sig key namespace1)))
    (is (vs/subscribed-to? view-sig key namespace2))))

(deftest removes-unsubscribed-to-view-from-subscribed-views
  (let [key 1, view-sig [:user-posts 1]]
    (vs/add-subscription! view-sig templates key)
    (vs/remove-subscription! view-sig key)
    (is (= {vs/default-ns {}} @vs/subscribed-views))))

(deftest adds-multiple-views-at-a-time
  (let [key 1, view-sigs [[:user-posts 1] [:user-posts 2]]]
    (vs/add-subscriptions! view-sigs templates key)
    (is (vs/subscribed-to? (first view-sigs) key))
    (is (vs/subscribed-to? (last view-sigs) key))))

(deftest subscribing-compiles-and-stores-view-maps
  (let [key 1, view-sig [:user-posts 1]]
    (vs/add-subscription! view-sig templates key)
    (is (= (:view (vs/compiled-view-for [:user-posts 1]))
           (user-posts-tmpl 1)))))

(deftest removing-last-view-sub-removes-compiled-view
  (let [key 1, view-sig [:user-posts 1]]
    (vs/add-subscription! view-sig templates key)
    (vs/remove-subscription! view-sig key)
    (is (nil? (vs/compiled-view-for [:user-posts 1])))))

(deftest retrieves-subscriptions-for-subscriber
  (let [key 1, view-sigs [[:users][:user-posts 1]]]
    (vs/add-subscriptions! view-sigs templates key)
    (is (= (set (vs/subscriptions-for 1)) (set view-sigs)))))

(deftest retrieves-subscriptions-for-subscriber-with-namespace
  (let [key 1, view-sigs [[:users][:user-posts 1]] namespace 1]
    (vs/add-subscriptions! view-sigs templates key namespace)
    (is (= (set (vs/subscriptions-for 1 namespace)) (set view-sigs)))))
