(ns views.db.core-test
  (:require
   [clojure.test :refer [use-fixtures deftest is]]
   [views.subscriptions :as vs]
   [views.subscribed-views :refer [ISubscribedViews]]
   [views.fixtures :as vf :refer [vschema sql-ts]]
   [views.db.core :as vdb]))

(def received-deltas (atom nil))

;; Very barebones subscribed-views instance which merely satisfies what vexec! needs:
(deftype TestSubscribedViews []
  ISubscribedViews
  (subscribed-views [this namespace]
    (map :view-data (vals (vs/compiled-views-for))))

  (broadcast-deltas [this new-deltas namespace]
    (reset! received-deltas new-deltas)))

(def test-subscribed-views (TestSubscribedViews.))
(def test-config {:db vf/db :schema vschema :templates vf/templates :base-subscribed-views test-subscribed-views})

(defn reset-fixtures!
  [f]
  (reset! vs/subscribed-views {})
  (reset! received-deltas {})
  (f))

(use-fixtures :each vf/with-user-fixture! (vf/database-fixtures! [:posts :comments]) reset-fixtures!)
(use-fixtures :once (vf/database-fixtures! [:users]))

(deftest vexec-sends-deltas
  (let [view-sig     [:user-posts (:id @vf/user-fixture)]
        sub-to-it    (vs/add-subscription! view-sig vf/templates (:id @vf/user-fixture))
        posted       (first (vdb/vexec! test-config (vf/insert-post-tmpl (:id @vf/user-fixture) "title" "body")))
        delta-vs     (ffirst @received-deltas)
        insert-delta (-> @received-deltas first second first :insert-deltas first)]

    (is (= (ffirst @received-deltas) view-sig))
    (is (= (:name insert-delta) (:name @vf/user-fixture)))
    (is (= (:body insert-delta) (:body posted)))
    (is (= (:title insert-delta) (:title posted)))))

(deftest with-view-transaction-sends-deltas
  (let [view-sig     [:user-posts (:id @vf/user-fixture)]
        sub-to-it    (vs/add-subscription! view-sig vf/templates (:id @vf/user-fixture))
        posted       (first (vdb/with-view-transaction
                              [tc test-config]
                              (vdb/vexec! tc (vf/insert-post-tmpl (:id @vf/user-fixture) "title" "body"))))
        delta-vs     (ffirst @received-deltas)
        insert-delta (-> @received-deltas first second first :insert-deltas first)]

    (is (= (ffirst @received-deltas) view-sig))
    (is (= (:name insert-delta) (:name @vf/user-fixture)))
    (is (= (:body insert-delta) (:body posted)))
    (is (= (:title insert-delta) (:title posted)))))

;; (deftest removes-nil-deltas
;;   (let [deltas {[:foo 1] {:insert-deltas '() :delete-deltas []}
;;                 [:bar 2] {:insert-deltas '() :delete-deltas [] :refresh-set []}
;;                 [:baz 2] {:insert-deltas '() :delete-deltas [{:baz 1}]}}]
;;     (is (= #{[:bar 2] [:baz 2]} (into #{} (keys (vdb/remove-nil-deltas deltas)))))
;; ))
