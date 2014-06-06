(ns views.subscriptions-test
  (:require
   [clojure.test :refer [use-fixtures deftest is]]
   [views.subscriptions :as vs]))

(defn- reset-subscribed-views!
  [f]
  (reset! vs/subscribed-views {})
  (f))

(use-fixtures :each reset-subscribed-views!)

(deftest adds-a-subscription
  (let [key 1, view-sig [:view 1]]
    (vs/add-subscription! key view-sig)
    (is (vs/subscribed-to? key view-sig))))

(deftest can-use-prefix
  (let [prefix1 1, prefix2 2, key 1, view-sig [:view 1]]
    (vs/add-subscription! key view-sig prefix1)
    (vs/add-subscription! key view-sig prefix2)
    (is (vs/subscribed-to? key view-sig prefix1))
    (is (vs/subscribed-to? key view-sig prefix2))))

(deftest removes-a-subscription
  (let [key 1, view-sig [:view 1]]
    (vs/add-subscription! key view-sig)
    (vs/remove-subscription! key view-sig)
    (is (not (vs/subscribed-to? key view-sig)))))

(deftest doesnt-fail-or-create-view-entry-when-empty
  (vs/remove-subscription! 1 [:view 1])
  (is (= {} @vs/subscribed-views)))

(deftest removes-a-subscription-with-prefix
  (let [prefix1 1, prefix2 2, key 1, view-sig [:view 1]]
    (vs/add-subscription! key view-sig prefix1)
    (vs/add-subscription! key view-sig prefix2)
    (vs/remove-subscription! key view-sig prefix1)
    (is (not (vs/subscribed-to? key view-sig prefix1)))
    (is (vs/subscribed-to? key view-sig prefix2))))

(deftest removes-unsubscribed-to-view-from-subscribed-views
  (let [key 1, view-sig [:view 1]]
    (vs/add-subscription! key view-sig)
    (vs/remove-subscription! key view-sig)
    (is (= {} @vs/subscribed-views))))

(deftest adds-multiple-views-at-a-time
  (let [key 1, view-sigs [[:view 1] [:view 2]]]
    (vs/add-subscriptions! key view-sigs)
    (is (vs/subscribed-to? key (first view-sigs)))
    (is (vs/subscribed-to? key (last view-sigs)))))

;; (deftest subscribing-compiles-and-stores-view-maps
;;   (let [key 1, view-sig [:view 1]]
;;     (vs/add-subscriptions! key view-sigs)
;;     (is (vs/subscribed-to? key (first view-sigs)))
;;     (is (vs/subscribed-to? key (last view-sigs)))))
