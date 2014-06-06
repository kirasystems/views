(ns views.db.honeysql-test
  (:require
   [clojure.test :refer [deftest is run-tests]]
   [views.db.honeysql :as vh]
   [honeysql.helpers :as hh]))

(def simple-test
  (-> (hh/select :a)
      (hh/from :foo)))

(def insert-test
  (-> (hh/insert-into :foo)
      (hh/values [{:foo "foo"}])))

(def join-test
  (-> (hh/select :a)
      (hh/from :foo)
      (hh/join :bar [:= :bar.id :foo.bar_id])))

(def join-with-alias-test
  (-> (hh/select :a)
      (hh/from :foo)
      (hh/join [:bar :b] [:= :b.id :foo.bar_id])))

(def join-and-from-with-alias-test
  (-> (hh/select :a)
      (hh/from [:foo :f])
      (hh/join [:bar :b] [:= :b.id :foo.bar_id])))

(deftest extracts-tables-from-specs
  (is (= (vh/extract-tables simple-test) #{[:foo]}))
  (is (= (vh/extract-tables insert-test) #{[:foo]}))
  (is (= (vh/extract-tables join-test) #{[:foo] [:bar]}))
  (is (= (vh/extract-tables join-with-alias-test) #{[:foo] [:bar :b]}))
  (is (= (vh/extract-tables join-and-from-with-alias-test) #{[:foo :f] [:bar :b]})))

;; Do we really need to test the new version?
(deftest merges-where-clauses
  (is (= (vh/merge-where-clauses [:= :foo 1] [:= :bar 2])
         {:where [:and [:= :foo 1] [:= :bar 2]]}))
  #_(is (= (vh/merge-where-clauses [[:= :foo 1]] [:= :bar 2])
         {:where [:and [:= :foo 1] [:= :bar 2]]}))
  #_(is (= (vh/merge-where-clauses [[:= :foo 1]] [:and [:= :bar 2] [:not= :baz 3]])
         {:where [:and [:= :foo 1] [:= :bar 2] [:not= :baz 3]]}))
  #_(is (= (vh/merge-where-clauses [[:= :foo 1]] [nil])
         {:where [:= :foo 1]}))
  #_(is (= (vh/merge-where-clauses [nil] [:= :bar 2])
         {:where [:= :bar 2]})))

(deftest table-alias-tests
  (is (= (vh/table-alias [:bar]) :bar))
  (is (= (vh/table-alias [:bar :a]) :a))
  (is (= (vh/table-alias :bar) :bar)))

(deftest table-name-tests
  (is (= (vh/table-name [:bar]) :bar))
  (is (= (vh/table-name [:bar :a]) :bar))
  (is (= (vh/table-name :bar) :bar)))

(deftest prefix-columns-tests
  (is (= (vh/prefix-columns [:= :id 1] :bar) [:= :bar.id 1]))
  (is (= (vh/prefix-columns [:and [:= :id 1] [:= :val "foo"]] :b)
         [:and [:= :b.id 1] [:= :b.val "foo"]]))
  (is (= (vh/prefix-columns [:and [:= :id 1] [:or [:> :x 3] [:= :val "foo"]]] :b)
         [:and [:= :b.id 1] [:or [:> :b.x 3] [:= :b.val "foo"]]])))

(deftest replace-table-tests
  (is (= (vh/replace-table [:= :bar.id 1] :bar :b) [:= :b.id 1]))
  (is (= (vh/replace-table [:= :bar.id 1] :baz :b) [:= :bar.id 1]))
  (is (= (vh/replace-table [:and [:= :bar.id 1] [:= :bar.val "foo"]] :bar :b)
         [:and [:= :b.id 1] [:= :b.val "foo"]])))
