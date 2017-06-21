(ns views.hash-test
  (:require
    [clojure.test :refer [are is deftest testing]]
    [views.hash :refer [md5-hash]]))

(deftest hash-algorithm
  (testing "Basic tests to make sure it produces a consistent hash"
    (are [x y]
      (= (md5-hash x) (md5-hash y))
      1 1
      0.5 0.5
      1N 1N
      1M 1M
      1N 1M
      true true
      "test string" "test string"
      [0 1 2 3] [0 1 2 3]
      '(0 1 2 3) '(0 1 2 3)
      #{0 1 2 3} #{3 2 1 0})

    (are [x y]
      (not= (md5-hash x) (md5-hash y))
      1 0
      1 1.0
      true false
      "test string" "string"
      [0 1 2 3] [2 3]
      '(0 1 2 3) '(2 3)
      #{0 1 2 3} #{2 3}
      1 true
      false [1]))

  (testing "Zero and nils are not equal"
    (are [x y]
      (not= (md5-hash x) (md5-hash y))
      0 nil
      {:a 0} {:a nil}
      #{0} #{nil}
      [0] [nil])))