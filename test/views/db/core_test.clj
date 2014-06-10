(ns views.db.core-test
  (:require
   [clojure.test :refer [deftest is run-tests]]
   [honeysql.core :as hsql]
   [honeysql.helpers :as hh]
   [views.fixtures :as vf]
   [views.db.core :as vdb]
   [views.base-subscribed-views :as bsv])
  (:import
   [views.base_subscribed_views BaseSubscribedViews]))

(defn join-test-template
  [id val3]
  (-> (hh/select :f.id :f.val1 :f.val2 :b.val1)
      (hh/from [:foo :f])
      (hh/join [:bar :b] [:= :b.id :f.b_id])
      (hh/left-join [:baz :ba] [:= :ba.id :b.ba_id])
      (hh/right-join [:qux :q] [:= :q.id :ba.q_id])
      (hh/where [:= :f.id id] [:= :f.val3 val3] [:= :f.val2 "constant"])))

(defn no-where-view-template
  []
  (-> (hh/select :f.id :f.val1 :f.val2)
      (hh/from [:foo :f])))

(defn bar-template
  [id]
  (-> (hh/select :b.id :b.val1)
      (hh/from [:bar :b])
      (hh/where [:= :val2 "some constant"]
                [:= :id id])))

(defn unrelated-template
  [id]
  (-> (hh/select :u.id :u.val1)
      (hh/from :unrelated
      (hh/where [:= :val "some constant"]
                [:= :id id]))))

(defn update-bar-template
  [val1 wc]
  (-> (hh/update :bar)
      (hh/values {:val1 val1})
      (hh/where wc)))

(deftest constructs-view-check-template
  (let [update-bar (update-bar-template "foo" [:= :id 123])
        vm         (vdb/view-map join-test-template [:join-test 1 "foo"])
        check-template (:view-check (vdb/view-check-template vm update-bar))]
    (is (= (set (:select check-template)) #{:f.id :f.val3}))
    (is (= (set (rest (:where check-template))) #{[:= :f.val2 "constant"] [:= :b.id 123]}))))

(deftest view-check-template-generates-proper-sql
  (let [update-bar (update-bar-template "foo" [:= :id 123])
        vm         (vdb/view-map join-test-template [:join-test 1 "foo"])
        check-template (:view-check (vdb/view-check-template vm update-bar))]
    (is (= (hsql/format check-template)
           ["SELECT f.id, f.val3 FROM foo f INNER JOIN bar b ON b.id = f.b_id LEFT JOIN baz ba ON ba.id = b.ba_id RIGHT JOIN qux q ON q.id = ba.q_id WHERE (b.id = 123 AND f.val2 = ?)" "constant"]))))

(deftest creates-collection-of-views-to-check
  (let [views         [(vdb/view-map no-where-view-template [:no-where])      ; no :bar
                       (vdb/view-map no-where-view-template [:no-where])      ; no :bar
                       (vdb/view-map bar-template [:bar 1])              ; has :bar
                       (vdb/view-map unrelated-template [:unrelated 2])        ; no :bar
                       (vdb/view-map join-test-template [:join-test 1 "foo"])  ; has :bar
                       (vdb/view-map join-test-template [:join-test 2 "bar"])] ; has :bar
        update-bar    (update-bar-template "foo" [:= :id 123])
        checked-views (vdb/prepare-view-checks views update-bar)]

    ;; It should return one check for the bar-template above,
    ;; and 1 for *both* the joint-test-templates.
    (is (= (count checked-views) 2))))

;; What is this for?
(def left-join-example (hsql/build :select [:R.a :S.C] :from :R :left-join [:S [:= :R.B :S.B]] :where [:!= :S.C 20]))

(deftest notes-view-map-as-no-delta-calc
  (let [tmpl (with-meta vf/users-tmpl {:bulk-update? true})]
    (is (:bulk-update? (vdb/view-map tmpl [:users])))))

;; (defschema schema vf/db "public")

;; (deftest sends-entire-view-on-every-update-with-bulk-update
;;   (let [tmpl (with-meta vf/users-tmpl {:bulk-update? true})
;;         vm   (vdb/view-map tmpl [:users])
;;         bsv  (BaseSubscribedViews. vf/db 

