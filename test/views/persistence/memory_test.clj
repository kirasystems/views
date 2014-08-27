(ns views.persistence.memory-test
  (:require
    [views.persistence.core :refer :all]
    [views.persistence.memory :refer [new-memory-persistence]]
    [views.fixtures :as vf]
    [clojure.test :refer [use-fixtures deftest is run-all-tests]]))

(deftest memory-persistence
  (let [p  (new-memory-persistence)
        vd (subscribe! p vf/db vf/templates :ns [:users] 1)]
    ;; This sort of test isn't great as it depends on the internal
    ;; structure unrlated to memory persistence.
    (is (= vd
           {:view-sig [:users], :view {:from [:users], :select [:id :name :created_on]}, :refresh-only? nil}))

    ;; Ensure that we are subscribed.
    (is (= (subscriptions p :ns [[:users]])
           {[:users] #{1}}))

    ;; Subsequent calls return same vd.
    (is (= (subscribe! p vf/db vf/templates :ns [:users] 3)
           vd))

    ;; And subscription is correct.
    (is (= (subscriptions p :ns [[:users]])
           {[:users] #{1 3}}))

    ;; Missing subscription returns nothing.
    (is (= (subscriptions p :ns [[:missing]])
           {}))

    ;; Duplicate subscription is ignored.
    (subscribe! p vf/db vf/templates :ns [:users] 3)
    (is (= (subscriptions p :ns [[:users]])
           {[:users] #{1 3}}))

    ;; We can subscribe to multiple views.
    (subscribe! p vf/db vf/templates :ns [:user-posts 1] 5)
    (is (= (subscriptions p :ns [[:users] [:user-posts 1]])
           {[:users]      #{1 3}
            [:user-posts 1] #{5}}))

    ;; Can we unsubscribe a view.
    (unsubscribe! p :ns [:users] 3)
    (is (= (subscriptions p :ns [[:users]])
           {[:users] #{1}}))

    ;; Remove last item in a view makes it go away.
    (unsubscribe! p :ns [:users] 1)
    (is (= (subscriptions p :ns [[:users]])
           {}))
    (is (= (map :view-sig (view-data p :ns :users))
           [[:user-posts 1]]))

    ;; Unsubscribe all works.
    (subscribe! p vf/db vf/templates :ns [:users] 7)
    (subscribe! p vf/db vf/templates :ns [:users] 5)
    (unsubscribe-all! p :ns 5)
    (is (= (subscriptions p :ns [[:users] [:user-posts 1]])
           {[:users] #{7}}))))

