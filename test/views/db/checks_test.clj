(ns views.db.checks-test
  (:require
   [clojure.test :refer [deftest is run-tests]]
   [honeysql.core :as hsql]
   [honeysql.helpers :as hh]
   [views.fixtures :as vf]
   [views.db.checks :as vc]))

(defn view [a b] (hsql/build :select [:c :d :f] :from {:foo :f} :where [:and [:and [:= :a a] [:= :b b]]]))

(deftest swaps-predicates-and-extracts-clauses
  (let [{:keys [p q]} (vc/swap-preds (view "?1" "?2"))
        swapped {:where [:and [:and true true]], :from {:foo :f}, :select [:c :d :f]}]
    (is (= (set p) #{[:= :a "?1"] [:= :b "?2"]}))
    (is (= (:where q) (:where swapped)))))

(deftest constructs-view-check
  (let [update (hsql/build :update :foo :set {:d "d"} :where [:= :c "c"])
        check  (hsql/build :select [:a :b] :from :foo :where [:and [:and true true] [:= :c "c"]])
        calcc  (vc/view-check update view [:view 1 2])]
    (is (= (into #{} (:select check)) (into #{} (:select calcc))))
    (is (= (:where check) (:where calcc)))))
