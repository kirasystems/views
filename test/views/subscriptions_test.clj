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
    (vs/add-subscription! key view-sig templates)
    (is (vs/subscribed-to? key view-sig))))

(deftest can-use-namespace
  (let [namespace1 1, namespace2 2, key 1, view-sig [:user-posts 1]]
    (vs/add-subscription! key view-sig templates namespace1)
    (vs/add-subscription! key view-sig templates namespace2)
    (is (vs/subscribed-to? key view-sig namespace1))
    (is (vs/subscribed-to? key view-sig namespace2))))

(deftest removes-a-subscription
  (let [key 1, view-sig [:user-posts 1]]
    (vs/add-subscription! key view-sig templates)
    (vs/remove-subscription! key view-sig)
    (is (not (vs/subscribed-to? key view-sig)))))

(deftest doesnt-fail-or-create-view-entry-when-empty
  (vs/remove-subscription! 1 [:user-posts 1])
  (is (= {} @vs/subscribed-views)))

(deftest removes-a-subscription-with-namespace
  (let [namespace1 1, namespace2 2, key 1, view-sig [:user-posts 1]]
    (vs/add-subscription! key view-sig templates namespace1)
    (vs/add-subscription! key view-sig templates namespace2)
    (vs/remove-subscription! key view-sig namespace1)
    (is (not (vs/subscribed-to? key view-sig namespace1)))
    (is (vs/subscribed-to? key view-sig namespace2))))

(deftest removes-unsubscribed-to-view-from-subscribed-views
  (let [key 1, view-sig [:user-posts 1]]
    (vs/add-subscription! key view-sig templates)
    (vs/remove-subscription! key view-sig)
    (is (= {:default-ns {}} @vs/subscribed-views))))

(deftest adds-multiple-views-at-a-time
  (let [key 1, view-sigs [[:user-posts 1] [:user-posts 2]]]
    (vs/add-subscriptions! key view-sigs templates)
    (is (vs/subscribed-to? key (first view-sigs)))
    (is (vs/subscribed-to? key (last view-sigs)))))

(deftest subscribing-compiles-and-stores-view-maps
  (let [key 1, view-sig [:user-posts 1]]
    (vs/add-subscription! key view-sig templates)
    (is (= (:view (vs/compiled-view-for [:user-posts 1]))
           (user-posts-tmpl 1)))))

(deftest removing-last-view-sub-removes-compiled-view
  (let [key 1, view-sig [:user-posts 1]]
    (vs/add-subscription! key view-sig templates)
    (vs/remove-subscription! key view-sig)
    (is (nil? (vs/compiled-view-for [:user-posts 1])))))

(deftest retrieves-subscriptions-for-subscriber
  (let [key 1, view-sigs [[:users][:user-posts 1]]]
    (vs/add-subscriptions! key view-sigs templates)
    (is (= (set (vs/subscriptions-for 1)) (set view-sigs)))))

(deftest retrieves-subscriptions-for-subscriber-with-namespace
  (let [key 1, view-sigs [[:users][:user-posts 1]] namespace 1]
    (vs/add-subscriptions! key view-sigs templates namespace)
    (is (= (set (vs/subscriptions-for 1 namespace)) (set view-sigs)))))
