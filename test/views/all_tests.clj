(ns views.all-tests
  (:require
   [clojure.test :refer [run-tests]]
   [views.subscriptions-test]
   [views.db.core-test]))

(defn run-all-tests
  []
  (run-tests 'views.subscriptions-test
             'views.db.core-test))
