(ns views.all-tests
  (:require
   [clojure.test :refer [run-tests]]
   [views.base-subscribed-views-test]
   [views.persistence.memory-test]
   [views.db.core-test]
   [views.db.deltas-test]
   [views.db.checks-test] ; STILL SPECULATIVE
   [views.db.honeysql-test]
   [views.db.load-test]))

(defn run-all-tests
  []
  (run-tests 'views.base-subscribed-views-test
             'views.persistence.memory-test
             'views.db.core-test
             'views.db.deltas-test
             'views.db.checks-test
             'views.db.honeysql-test
             'views.db.load-test))
