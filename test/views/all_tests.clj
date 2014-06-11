(ns views.all-tests
  (:require
   [clojure.test :refer [run-tests]]
   [views.subscriptions-test]
   [views.base-subscribed-views-test]
   ;; [views.db.core-test]
   [views.db.deltas-test]
   [views.db.honeysql-test]
   [views.db.load-test]))

(defn run-all-tests
  []
  (run-tests 'views.subscriptions-test
             'views.base-subscribed-views-test
;;             'views.db.core-test
             'views.db.deltas-test
             'views.db.honeysql-test
             'views.db.load-test))
