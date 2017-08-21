(ns views.initialization-tests
  (:import
    (views.test_view_system MemoryView))
  (:require
    [clojure.test :refer :all]
    [views.core :as views]
    [views.statistics :as statistics]
    [views.test-helpers :as test-helpers]
    [views.test-view-system :refer [test-views-system reset-test-views-system views]]))

(use-fixtures :each reset-test-views-system)

(defn dummy-send-fn [subscriber-key [view-sig view-data]])

(def test-options (merge views/default-options
                         {:views views
                          :send-fn dummy-send-fn}))

(deftest inits-with-correct-config-and-shutsdown-correctly
  (testing "initialize view system with supplied atom and shutdown correctly"
    (let [options test-options
          ; 1. init views
          init-returned-atom (views/init! test-views-system test-options)]
      (is (instance? clojure.lang.Atom init-returned-atom))
      (is (= init-returned-atom test-views-system))
      (is (seq @test-views-system))
      (is (= dummy-send-fn (:send-fn @test-views-system)))
      (is (and (test-helpers/contains-view? test-views-system :foo)
               (test-helpers/contains-view? test-views-system :bar)
               (test-helpers/contains-view? test-views-system :baz)))
      (is (not (:logging? @test-views-system)))
      (is (not (statistics/collecting? test-views-system)))
      (is (empty? (views/subscribed-views test-views-system)))
      (let [refresh-watcher (:refresh-watcher @test-views-system)
            workers         (:workers @test-views-system)]
        (is (.isAlive ^Thread refresh-watcher))
        (is (= (:worker-threads options) (count workers)))
        (doseq [^Thread t workers]
          (is (.isAlive t)))
        ; 2. shutdown views (and wait for all threads to also finish)
        (views/shutdown! test-views-system)
        (is (empty? @test-views-system))
        (is (not (.isAlive ^Thread refresh-watcher)))
        (doseq [^Thread t workers]
          (is (not (.isAlive t))))))))

(deftest init-without-existing-view-system-atom
  (testing "initialize view sytem with only options and shutdown correctly"
    (let [options test-options]
      (let [init-created-atom (views/init! options)]
        (is (instance? clojure.lang.Atom init-created-atom))
        (is (seq @init-created-atom))
        (is (= dummy-send-fn (:send-fn @init-created-atom)))
        (is (and (test-helpers/contains-view? init-created-atom :foo)
                 (test-helpers/contains-view? init-created-atom :bar)
                 (test-helpers/contains-view? init-created-atom :baz)))
        (views/shutdown! init-created-atom)
        (is (empty? @init-created-atom))))))

(deftest init-can-also-start-logger
  (testing "initialize view system with logging and shutdown"
    (let [options (assoc test-options :stats-log-interval 10000)]
      ; 1. init views
      (is (not (statistics/collecting? test-views-system)))
      (views/init! test-views-system options)
      (is (seq (:statistics @test-views-system)))
      (is (:logging? @test-views-system))
      (is (statistics/collecting? test-views-system))
      (let [logger-thread (get-in @test-views-system [:statistics :logger])]
        (is (.isAlive ^Thread logger-thread))
        ; 2. shutdown views
        (views/shutdown! test-views-system)
        (is (nil? (get-in @test-views-system [:statistics :logger])))
        (is (not (.isAlive ^Thread logger-thread)))))))

(deftest can-add-new-views-after-init
  (let [options test-options]
    (testing "initialize view system"
      (views/init! test-views-system options)
      (is (and (test-helpers/contains-view? test-views-system :foo)
               (test-helpers/contains-view? test-views-system :bar)
               (test-helpers/contains-view? test-views-system :baz))))
    (testing "add new views"
      (views/add-views! test-views-system
                        [(MemoryView. :one [:one])
                         (MemoryView. :two [:two])])
      (is (and (test-helpers/contains-view? test-views-system :foo)
               (test-helpers/contains-view? test-views-system :bar)
               (test-helpers/contains-view? test-views-system :baz)
               (test-helpers/contains-view? test-views-system :one)
               (test-helpers/contains-view? test-views-system :two))))
    (testing "shutdown"
      (views/shutdown! test-views-system))))

(deftest can-replace-views-after-init
  (let [options          test-options
        replacement-view (MemoryView. :foo [:new-foo])]
    (testing "initialize view system"
      (views/init! test-views-system options)
      (is (and (test-helpers/contains-view? test-views-system :foo)
               (test-helpers/contains-view? test-views-system :bar)
               (test-helpers/contains-view? test-views-system :baz)))
      (is (not= replacement-view (get-in @test-views-system [:views :foo]))))
    (testing "add view, has same id so should replace existing one"
      (views/add-views! test-views-system [replacement-view])
      (is (and (test-helpers/contains-view? test-views-system :foo)
               (test-helpers/contains-view? test-views-system :bar)
               (test-helpers/contains-view? test-views-system :baz)))
      (is (= replacement-view (get-in @test-views-system [:views :foo]))))
    (testing "shutdown"
      (views/shutdown! test-views-system))))
