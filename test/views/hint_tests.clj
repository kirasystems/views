(ns views.hint-tests
  (:require
    [clojure.test :refer :all]
    [views.core :as views]
    [views.hash :refer [md5-hash]]
    [views.test-helpers :as test-helpers]
    [views.test-view-system :refer :all]))

(defonce test-sent-data
  (atom []))

(defn test-send-fn [subscriber-key [view-sig view-data]]
  (swap! test-sent-data conj {:subscriber-key subscriber-key
                              :view-sig       view-sig
                              :view-data      view-data}))

(def test-options (merge views/default-options
                         {:views views
                          :send-fn test-send-fn}))

(defn clear-sent-data-fixture [f]
  (reset! test-sent-data [])
  (f))

(use-fixtures :each clear-sent-data-fixture reset-test-views-system reset-memory-db-fixture)

(deftest refresh-views!-instantly-attempts-view-refresh-with-given-hints
  (let [options         test-options
        hints-refreshed (atom [])]
    (testing "initialize views"
      (views/init! test-views-system options))
    (testing "refresh with a view subscription"
      (with-redefs [views/subscribed-views (fn [_] #{(views/->view-sig :namespace :fake-view [])})
                    views/refresh-view!    (fn [_ hints _] (swap! hints-refreshed into hints))]
        (testing "trigger refresh by calling refresh-views! with one hint"
          (views/refresh-views! test-views-system [(views/hint :namespace [:foo] :fake-type)])
          (is (test-helpers/contains-only? @hints-refreshed
                                           [(views/hint :namespace [:foo] :fake-type)]))
          (reset! hints-refreshed []))
        (testing "trigger refresh by calling refresh-views! with multiple hints"
          (views/refresh-views! test-views-system
                                [(views/hint :namespace [:foo] :fake-type)
                                 (views/hint :namespace [:bar] :fake-type)])
          (is (test-helpers/contains-only? @hints-refreshed
                                           [(views/hint :namespace [:foo] :fake-type)
                                            (views/hint :namespace [:bar] :fake-type)]))
          (reset! hints-refreshed []))))

    (testing "refresh without a view subscription"
      (with-redefs [views/subscribed-views (fn [_] #{})
                    views/refresh-view!    (fn [_ hints _] (swap! hints-refreshed into hints))]
        (testing "trigger refresh by calling refresh-views! with one hint"
          (views/refresh-views! test-views-system [(views/hint :namespace [:foo] :fake-type)])
          (is (empty? @hints-refreshed))
          (reset! hints-refreshed []))))))

(deftest refresh-watcher-runs-at-specified-interval-and-picks-up-queued-hints
  (let [options         test-options
        hints-refreshed (atom [])]
    (with-redefs [views/subscribed-views (fn [_] #{(views/->view-sig :namespace :fake-view [])})
                  views/refresh-view!    (fn [_ hints _] (swap! hints-refreshed into hints))]
      (testing "initialize views"
        (views/init! test-views-system options))
      (testing "queue a hint and wait until the next refresh interval"
        (views/queue-hints! test-views-system [(views/hint :namespace [:foo] :fake-type)])
        (test-helpers/wait-for-refresh-interval options)
        (is (test-helpers/contains-only? @hints-refreshed [(views/hint :namespace [:foo] :fake-type)]))
        (reset! hints-refreshed []))
      (testing "queue multiple hints and wait until the next refresh interval"
        (views/queue-hints! test-views-system
                            [(views/hint :namespace [:foo] :fake-type)
                             (views/hint :namespace [:bar] :fake-type)])
        (test-helpers/wait-for-refresh-interval options)
        (is (test-helpers/contains-only? @hints-refreshed
                                         [(views/hint :namespace [:foo] :fake-type)
                                          (views/hint :namespace [:bar] :fake-type)]))
        (reset! hints-refreshed []))
      (testing "queue up no hints and wait until the next refresh interval"
        (test-helpers/wait-for-refresh-interval options)
        (reset! hints-refreshed [])))))

(deftest refresh-worker-thread-processes-relevant-hints
  (let [options         test-options
        views-refreshed (atom [])]
    (testing "initialize views"
      (views/init! test-views-system options))
    (testing "trigger refresh by calling refresh-views!"
      (with-redefs [views/subscribed-views (fn [_] #{(views/->view-sig :a :foo [])})
                    views/do-view-refresh! (fn [_ view-sig] (swap! views-refreshed into [view-sig]))]
        (testing "with relevant hint"
          (views/refresh-views! test-views-system [(views/hint :a [:foo] memory-view-hint-type)])
          (test-helpers/wait-for-refresh-views)
          (is (test-helpers/contains-only? @views-refreshed [(views/->view-sig :a :foo [])]))
          (reset! views-refreshed []))
        (testing "with multiple hints (1 relevant, 1 not)"
          (views/refresh-views! test-views-system [(views/hint :a [:foo] memory-view-hint-type)
                                                   (views/hint :a [:bar] memory-view-hint-type)])
          (test-helpers/wait-for-refresh-views)
          (is (test-helpers/contains-only? @views-refreshed [(views/->view-sig :a :foo [])]))
          (reset! views-refreshed []))
        (testing "with only irrelevant hints"
          (views/refresh-views! test-views-system
                                [(views/hint :b [:foo] memory-view-hint-type)
                                 (views/hint :a [:foo] :some-other-type)])
          (test-helpers/wait-for-refresh-views)
          (is (empty? @views-refreshed))
          (reset! views-refreshed []))))))

; this test is really just testing that our helper function memory-db-assoc-in! works as we expect it to
; (otherwise, it is entirely redundant given the above tests)
(deftest test-memory-db-operation-triggers-proper-refresh-hints
  (let [options         test-options
        hints-refreshed (atom [])
        views-refreshed (atom [])]
    (testing "initialize views"
      (views/init! test-views-system options))
    (testing "verify that correct hints are being sent out (don't care if relevant or not yet)"
      (with-redefs [views/subscribed-views (fn [_] #{(views/->view-sig :a :foo [])})
                    views/refresh-view!    (fn [_ hints _] (swap! hints-refreshed into hints))]
        (memory-db-assoc-in! test-views-system :a [:foo] 42)
        (memory-db-assoc-in! test-views-system :a [:bar] 3.14)
        (memory-db-assoc-in! test-views-system :b [:baz] [10 20 30])
        (test-helpers/wait-for-refresh-views)
        (is (test-helpers/contains-only? @hints-refreshed
                                         [(views/hint :a [:foo] memory-view-hint-type)
                                          (views/hint :a [:bar] memory-view-hint-type)
                                          (views/hint :b [:baz] memory-view-hint-type)]))
        (reset! views-refreshed [])))
    (testing "test that relevant views were recognized as relevant and forwarded on to be used to
              trigger actual refreshes of view data"
      (with-redefs [views/subscribed-views (fn [_] #{(views/->view-sig :a :foo [])})
                    views/do-view-refresh! (fn [_ view-sig] (swap! views-refreshed into [view-sig]))]
        (testing "update memory database in a location covered by the subscribed view"
          (memory-db-assoc-in! test-views-system :a [:foo] 1337)
          (test-helpers/wait-for-refresh-interval options)
          (is (test-helpers/contains-only? @views-refreshed [(views/->view-sig :a :foo [])]))
          (reset! views-refreshed []))
        (testing "update memory database in a different location not covered by any subscription"
          (memory-db-assoc-in! test-views-system :a [:bar] 1234.5678)
          (test-helpers/wait-for-refresh-interval options)
          (is (empty? @views-refreshed)))))))

(deftest relevant-hints-cause-refreshed-data-to-be-sent-to-subscriber
  (let [options        test-options
        subscriber-key 123
        view-sig       (views/->view-sig :a :foo [])]
    (testing "initialize views"
      (views/init! test-views-system options))
    (testing "subscribe to a view"
      (let [original-view-data (test-helpers/get-view-data test-views-system view-sig)
            updated-view-data  21
            subscribe-result   (views/subscribe! test-views-system view-sig subscriber-key nil)]
        (testing "block until subscription finishes. we don't care about the initial view data refresh"
          (while (not (realized? subscribe-result)))
          (reset! test-sent-data [])
          (is (= (md5-hash original-view-data) (get-in @test-views-system [:hashes view-sig]))))
        (testing "change some test data that is covered by the view subscription"
          (memory-db-assoc-in! test-views-system :a [:foo] updated-view-data)
          (test-helpers/wait-for-refresh-views)
          (is (= (md5-hash updated-view-data) (get-in @test-views-system [:hashes view-sig])))
          (is (test-helpers/contains-only? @test-sent-data
                                           [{:subscriber-key subscriber-key
                                             :view-sig       (dissoc view-sig :namespace)
                                             :view-data      updated-view-data}])))))))

(deftest irrelevant-hints-dont-trigger-refreshes
  (let [options        test-options
        subscriber-key 123
        view-sig       (views/->view-sig :a :foo [])]
    (testing "initialize views"
      (views/init! test-views-system options))
    (testing "subscribe to a view"
      (let [subscribe-result (views/subscribe! test-views-system view-sig subscriber-key nil)]
        (testing "block until subscription finishes. we don't care about the initial view data refresh"
          (while (not (realized? subscribe-result)))
          (reset! test-sent-data []))
        (testing "change some test data that is NOT covered by the view subscription"
          (memory-db-assoc-in! test-views-system :b [:foo] 6)
          (memory-db-assoc-in! test-views-system :a [:bar] 7)
          (test-helpers/wait-for-refresh-views)
          (is (empty? @test-sent-data)))))))

(deftest refreshes-not-sent-if-view-data-is-unchanged-since-last-refresh
  (let [options        test-options
        subscriber-key 123
        view-sig       (views/->view-sig :a :foo [])]
    (testing "initialize views"
      (views/init! test-views-system options))
    (testing "subscribe to a view"
      (let [updated-view-data 1111
            subscribe-result  (views/subscribe! test-views-system view-sig subscriber-key nil)]
        (testing " and block until subscription finishes. we don't care about the initial view data refresh"
          (while (not (realized? subscribe-result)))
          (reset! test-sent-data []))
        (testing "and change some test data, will cause a refresh to be sent out"
          (memory-db-assoc-in! test-views-system :a [:foo] updated-view-data)
          (test-helpers/wait-for-refresh-views)
          (is (= (md5-hash updated-view-data) (get-in @test-views-system [:hashes view-sig])))
          (is (test-helpers/contains-only? @test-sent-data
                                           [{:subscriber-key subscriber-key
                                             :view-sig       (dissoc view-sig :namespace)
                                             :view-data      updated-view-data}]))
          (reset! test-sent-data []))
        (testing "manually trigger another refresh for the view"
          (views/refresh-views! test-views-system [(views/hint :a [:foo] memory-view-hint-type)])
          (test-helpers/wait-for-refresh-views)
          (is (empty? @test-sent-data)))
        (testing "also try updating the db with the same values"
          (memory-db-assoc-in! test-views-system :a [:foo] updated-view-data)
          (test-helpers/wait-for-refresh-views)
          (is (empty? @test-sent-data)))))))

(deftest refresh-queue-drops-duplicate-hints
  (let [options        (assoc test-options :stats-log-interval 10000) ; enable statistics collection
        subscriber-key 123
        view-sig       (views/->view-sig :a :foo [])]
    (testing "initialize views"
      (views/init! test-views-system options))
    (testing "prematurely stop refresh worker threads so that we can more easily inspect the
              internal refresh queue's entries. the refresh worker threads are what remove
              hints from the refresh queue as they are added to it."
      (test-helpers/stop-refresh-worker-threads test-views-system))
    (testing "subscribe to a view"
      (let [subscribe-result (views/subscribe! test-views-system view-sig subscriber-key nil)]
        (testing "block until subscription finishes"
          (while (not (realized? subscribe-result)))
          (is (= 0 (get-in @test-views-system [:statistics :deduplicated]))))
        (testing "add duplicate hints by changing the same set of data twice
                  (hints will stay in the queue forever because we stopped the worker threads)"
          (memory-db-assoc-in! test-views-system :a [:foo] 6)
          (memory-db-assoc-in! test-views-system :a [:foo] 7)
          (test-helpers/wait-for-refresh-views)
          (is (= 1 (get-in @test-views-system [:statistics :deduplicated])))
          (is (= [view-sig] (vec (:refresh-queue @test-views-system)))))))))

(deftest refresh-queue-drops-hints-when-full
  (let [options        (assoc test-options
                         :stats-log-interval 10000
                         :refresh-queue-size 1)
        subscriber-key 123
        view-sig-a     (views/->view-sig :a :foo [])
        view-sig-b     (views/->view-sig :b :foo [])]
    (testing "initialize views"
      (views/init! test-views-system options))
    (testing "prematurely stop refresh worker threads so that we can more easily inspect the
              internal refresh queue's entries. the refresh worker threads are what remove
              hints from the refresh queue as they are added to it."
      (test-helpers/stop-refresh-worker-threads test-views-system))
    (testing "subscribe to a view"
      ; note: log* redef is to suppress error log output which will normally happen whenever
      ;       another item is added to the refresh queue when it's already full
      (with-redefs [clojure.tools.logging/log* (fn [& _])]
        (let [subscribe-a (views/subscribe! test-views-system view-sig-a subscriber-key nil)
              subscribe-b (views/subscribe! test-views-system view-sig-b subscriber-key nil)]
          (testing "block until subscription finishes"
            (while (or (not (realized? subscribe-a))
                       (not (realized? subscribe-b))))
            (is (= 0 (get-in @test-views-system [:statistics :dropped]))))
          (testing "change some data affecting the subscribed view, resulting in more then 1 hint
                    being added to the refresh queue"
            (memory-db-assoc-in! test-views-system :a [:foo] 101010)
            (memory-db-assoc-in! test-views-system :b [:foo] 010101)
            (test-helpers/wait-for-refresh-views)
            (is (= 1 (get-in @test-views-system [:statistics :dropped])))
            (is (= [view-sig-a] (vec (:refresh-queue @test-views-system))))))))))
