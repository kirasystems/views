(ns views.subscription-tests
  (:require
    [clojure.test :refer :all]
    [views.core :as views]
    [views.hash :refer [md5-hash]]
    [views.test-helpers :as test-helpers]
    [views.test-view-system :refer :all]))

(def test-sent-data
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

(deftest can-subscribe-to-a-view
  (let [options        test-options
        subscriber-key 123
        view-sig       (views/->view-sig :namespace :foo [])
        context        {:my-data "arbitrary application context data"}]
    ; 1. init views
    (views/init! test-views-system options)
    ; 2. subscribe to a view
    (let [subscribe-result (views/subscribe! test-views-system view-sig subscriber-key context)]
      (is (future? subscribe-result))
      (is (= [subscriber-key] (keys (:subscribed @test-views-system))))
      (is (= #{view-sig} (get-in @test-views-system [:subscribed subscriber-key])))
      (is (= #{subscriber-key} (get-in @test-views-system [:subscribers view-sig])))
      ; 3. block until subscription finishes (data retrieval + initial view refresh)
      ;    (in this particular unit test, there is really no point in waiting)
      (while (not (realized? subscribe-result)))
      (is (= #{view-sig} (views/subscribed-views test-views-system))))))

(deftest subscribing-results-in-initial-view-data-being-sent
  (let [options        test-options
        subscriber-key 123
        view-sig       (views/->view-sig :a :foo [])
        context        {:my-data "arbitrary application context data"}]
    ; 1. init views
    (views/init! test-views-system options)
    ; 2. subscribe to a view
    (let [view-data        (test-helpers/get-view-data test-views-system view-sig)
          subscribe-result (views/subscribe! test-views-system view-sig subscriber-key context)]
      ; 3. block until subscription finishes (data retrieval + initial view refresh)
      (while (not (realized? subscribe-result)))
      (is (= #{view-sig} (views/subscribed-views test-views-system)))
      (is (= (md5-hash view-data) (get-in @test-views-system [:hashes view-sig])))
      (is (test-helpers/contains-only? @test-sent-data
                                       [{:subscriber-key subscriber-key
                                         :view-sig       (dissoc view-sig :namespace)
                                         :view-data      view-data}])))))

(deftest can-unsubscribe-from-a-view
  (let [options        test-options
        subscriber-key 123
        view-sig       (views/->view-sig :a :foo [])
        context        {:my-data "arbitrary application context data"}]
    ; 1. init views
    (views/init! test-views-system options)
    ; 2. subscribe to a view
    (let [view-data        (test-helpers/get-view-data test-views-system view-sig)
          subscribe-result (views/subscribe! test-views-system view-sig subscriber-key context)]
      (is (= [subscriber-key] (keys (:subscribed @test-views-system))))
      (is (= #{view-sig} (get-in @test-views-system [:subscribed subscriber-key])))
      (is (= #{subscriber-key} (get-in @test-views-system [:subscribers view-sig])))
      (is (= #{view-sig} (views/subscribed-views test-views-system)))
      ; 3. block until subscription finishes
      (while (not (realized? subscribe-result)))
      (is (= (md5-hash view-data) (get-in @test-views-system [:hashes view-sig])))
      ; 4. unsubscribe
      (views/unsubscribe! test-views-system view-sig subscriber-key context)
      (is (empty? (keys (:subscribed @test-views-system))))
      (is (empty? (keys (:subscribers @test-views-system))))
      (is (empty? (views/subscribed-views test-views-system)))
      (is (empty? (:hashes @test-views-system))))))

(deftest multiple-subscription-and-unsubscriptions
  (let [options          test-options
        subscriber-key-a 123
        subscriber-key-b 456
        view-sig         (views/->view-sig :a :foo [])]
    ; 1. init views
    (views/init! test-views-system options)
    ; 2. subscribe to a view
    (let [view-data        (test-helpers/get-view-data test-views-system view-sig)
          subscribe-a      (views/subscribe! test-views-system view-sig subscriber-key-a nil)
          subscribe-b      (views/subscribe! test-views-system view-sig subscriber-key-b nil)]
      ; 3. block until both subscriptions finish
      (while (or (not (realized? subscribe-a))
                 (not (realized? subscribe-b))))
      (is (= #{view-sig} (views/subscribed-views test-views-system)))
      (is (= [subscriber-key-a subscriber-key-b] (keys (:subscribed @test-views-system))))
      (is (= #{view-sig} (get-in @test-views-system [:subscribed subscriber-key-a])))
      (is (= #{view-sig} (get-in @test-views-system [:subscribed subscriber-key-b])))
      (is (= #{subscriber-key-a subscriber-key-b} (get-in @test-views-system [:subscribers view-sig])))
      (is (= (md5-hash view-data) (get-in @test-views-system [:hashes view-sig])))
      (is (test-helpers/contains-only? @test-sent-data
                          [{:subscriber-key subscriber-key-a
                            :view-sig       (dissoc view-sig :namespace)
                            :view-data      view-data}
                           {:subscriber-key subscriber-key-b
                            :view-sig       (dissoc view-sig :namespace)
                            :view-data      view-data}]))
      ; 4. have one of the subscribers unsubscribe
      (views/unsubscribe! test-views-system view-sig subscriber-key-a nil)
      (is (= #{view-sig} (views/subscribed-views test-views-system)))
      (is (= [subscriber-key-b] (keys (:subscribed @test-views-system))))
      (is (= #{view-sig} (get-in @test-views-system [:subscribed subscriber-key-b])))
      (is (= #{subscriber-key-b} (get-in @test-views-system [:subscribers view-sig])))
      (testing "hash should be nil because we wipe on every unsubscribe"
        (is (nil? (get-in @test-views-system [:hashes view-sig]))))
      ; 5. have the last subscriber also unsubscribe
      (views/unsubscribe! test-views-system view-sig subscriber-key-b nil)
      (is (empty? (keys (:subscribed @test-views-system))))
      (is (empty? (keys (:subscribers @test-views-system))))
      (is (empty? (views/subscribed-views test-views-system)))
      (is (empty? (:hashes @test-views-system))))))

(deftest subscriptions-to-different-views
  (let [options          test-options
        subscriber-key-a 123
        subscriber-key-b 456
        view-sig-a       (views/->view-sig :a :foo [])
        view-sig-b       (views/->view-sig :a :bar [])]
    ; 1. init views
    (views/init! test-views-system options)
    ; 2. subscribe to a view
    (let [view-data-a (test-helpers/get-view-data test-views-system view-sig-a)
          view-data-b (test-helpers/get-view-data test-views-system view-sig-b)
          subscribe-a (views/subscribe! test-views-system view-sig-a subscriber-key-a nil)
          subscribe-b (views/subscribe! test-views-system view-sig-b subscriber-key-b nil)]
      ; 3. block until both subscriptions finish
      (while (or (not (realized? subscribe-a))
                 (not (realized? subscribe-b))))
      (is (= #{view-sig-a view-sig-b} (views/subscribed-views test-views-system)))
      (is (= [subscriber-key-a subscriber-key-b] (keys (:subscribed @test-views-system))))
      (is (= #{view-sig-a} (get-in @test-views-system [:subscribed subscriber-key-a])))
      (is (= #{view-sig-b} (get-in @test-views-system [:subscribed subscriber-key-b])))
      (is (= #{subscriber-key-a} (get-in @test-views-system [:subscribers view-sig-a])))
      (is (= #{subscriber-key-b} (get-in @test-views-system [:subscribers view-sig-b])))
      (is (= (md5-hash view-data-a) (get-in @test-views-system [:hashes view-sig-a])))
      (is (= (md5-hash view-data-b) (get-in @test-views-system [:hashes view-sig-b])))
      (is (test-helpers/contains-only? @test-sent-data 
                                       [{:subscriber-key subscriber-key-a 
                                         :view-sig       (dissoc view-sig-a :namespace)
                                         :view-data      view-data-a}
                                        {:subscriber-key subscriber-key-b
                                         :view-sig       (dissoc view-sig-b :namespace)
                                         :view-data      view-data-b}]))
      ; 4. have one of the subscribers unsubscribe
      (views/unsubscribe! test-views-system view-sig-a subscriber-key-a nil)
      (is (= #{view-sig-b} (views/subscribed-views test-views-system)))
      (is (= [subscriber-key-b] (keys (:subscribed @test-views-system))))
      (is (empty? (get-in @test-views-system [:subscribed subscriber-key-a])))
      (is (= #{view-sig-b} (get-in @test-views-system [:subscribed subscriber-key-b])))
      (is (= #{subscriber-key-b} (get-in @test-views-system [:subscribers view-sig-b])))
      (is (empty? (get-in @test-views-system [:subscribers view-sig-a])))
      (is (empty? (get-in @test-views-system [:hashes view-sig-a])))
      (is (= (md5-hash view-data-b) (get-in @test-views-system [:hashes view-sig-b])))
      ; 5. have the last subscriber also unsubscribe
      (views/unsubscribe! test-views-system view-sig-b subscriber-key-b nil)
      (is (empty? (keys (:subscribed @test-views-system))))
      (is (empty? (keys (:subscribers @test-views-system))))
      (is (empty? (views/subscribed-views test-views-system)))
      (is (empty? (:hashes @test-views-system))))))

(deftest duplicate-subscriptions-do-not-cause-problems
  (let [options        test-options
        subscriber-key 123
        view-sig       (views/->view-sig :a :foo [])]
    ; 1. init views
    (views/init! test-views-system options)
    ; 2. subscribe to a view
    (let [view-data        (test-helpers/get-view-data test-views-system view-sig)
          first-subscribe  (views/subscribe! test-views-system view-sig subscriber-key nil)
          second-subscribe (views/subscribe! test-views-system view-sig subscriber-key nil)]
      ; 3. block until both subscriptions finish
      (while (or (not (realized? first-subscribe))
                 (not (realized? second-subscribe))))
      (is (= #{view-sig} (views/subscribed-views test-views-system)))
      (is (= [subscriber-key] (keys (:subscribed @test-views-system))))
      (is (= #{view-sig} (get-in @test-views-system [:subscribed subscriber-key])))
      (is (= #{subscriber-key} (get-in @test-views-system [:subscribers view-sig])))
      (is (= (md5-hash view-data) (get-in @test-views-system [:hashes view-sig])))
      (is (test-helpers/contains-only? @test-sent-data
                                       [{:subscriber-key subscriber-key
                                         :view-sig       (dissoc view-sig :namespace)
                                         :view-data      view-data}
                                        {:subscriber-key subscriber-key
                                         :view-sig       (dissoc view-sig :namespace)
                                         :view-data      view-data}]))
      ; 4. unsubscribe. only need to do this once, since only one subscription
      ;    should exist in the view system
      (views/unsubscribe! test-views-system view-sig subscriber-key nil)
      (is (empty? (keys (:subscribed @test-views-system))))
      (is (empty? (keys (:subscribers @test-views-system))))
      (is (empty? (views/subscribed-views test-views-system)))
      (is (empty? (:hashes @test-views-system))))))

(deftest subscribing-to-non-existant-view-raises-exception
  (let [options        test-options
        subscriber-key 123
        view-sig       (views/->view-sig :namespace :non-existant-view [])]
    ; 1. init views
    (views/init! test-views-system options)
    ; 2. subscribe to a view
    (is (thrown? Exception (views/subscribe! test-views-system view-sig subscriber-key nil)))))

(deftest subscribe-and-unsubscribe-use-namespace-fn-if-set-and-no-namespace-in-view-sig
  (let [subscriber-key 123
        view-sig       (views/->view-sig :foo [])
        context        "some arbitrary context data"
        namespace-fn   (fn [view-sig* subscriber-key* context*]
                         (is (= view-sig view-sig*))
                         (is (= subscriber-key subscriber-key*))
                         (is (= context context*))
                         :b)
        options        (-> test-options
                           (assoc :namespace-fn namespace-fn))]
    ; 1. init views
    (views/init! test-views-system options)
    ; 2. subscribe to a view
    (let [; with the above namespace-fn, subscribe will internally use this view sig
          ; when setting up subscription info within view-system. application code
          ; shouldn't need to worry about this, but we will in this unit test
          view-sig-with-ns (views/->view-sig :b :foo [])
          ; such as right here, we need to use the actual namespace that was set in
          ; view-system to pass in the same parameters that subscribe! will use for
          ; the view during it's initial view data refresh
          view-data        (test-helpers/get-view-data test-views-system view-sig-with-ns)
          ; passing in view-sig *without* namespace
          subscribe-result (views/subscribe! test-views-system view-sig subscriber-key context)]
      ; 3. block until subscription finishes
      (while (not (realized? subscribe-result)))
      (is (= #{view-sig-with-ns} (views/subscribed-views test-views-system)))
      (is (= [subscriber-key] (keys (:subscribed @test-views-system))))
      (is (= #{view-sig-with-ns} (get-in @test-views-system [:subscribed subscriber-key])))
      (is (= #{subscriber-key} (get-in @test-views-system [:subscribers view-sig-with-ns])))
      (is (= (md5-hash view-data) (get-in @test-views-system [:hashes view-sig-with-ns])))
      (is (test-helpers/contains-only? @test-sent-data
                                       [{:subscriber-key subscriber-key
                                         :view-sig       (dissoc view-sig :namespace)
                                         :view-data      view-data}]))
      ; 4. unsubscribe.
      ; NOTE: we are passing in view-sig, not view-sig-with-ns. this is because
      ;       proper namespace-fn's should be consistent with what namespace they
      ;       return given the same inputs. ideal namespace-fn implementations will
      ;       also keep this in mind even if context could vary between subscribe!
      ;       and unsubscribe! calls.
      (views/unsubscribe! test-views-system view-sig subscriber-key context)
      (is (empty? (keys (:subscribed @test-views-system))))
      (is (empty? (keys (:subscribers @test-views-system))))
      (is (empty? (views/subscribed-views test-views-system)))
      (is (empty? (:hashes @test-views-system))))))

(deftest subscribe-and-unsubscribe-do-not-use-namespace-fn-if-namespace-included-in-view-sig
  (let [subscriber-key 123
        view-sig       (views/->view-sig :a :foo [])
        context        "some arbitrary context data"
        namespace-fn   (fn [view-sig* subscriber-key* context*]
                         ; if this function is used, it will mess up several assertions in this unit test
                         :b)
        options        (-> test-options
                           (assoc :namespace-fn namespace-fn))]
    ; 1. init views
    (views/init! test-views-system options)
    ; 2. subscribe to a view
    (let [view-data        (test-helpers/get-view-data test-views-system view-sig)
          subscribe-result (views/subscribe! test-views-system view-sig subscriber-key context)]
      ; 3. block until subscription finishes
      (while (not (realized? subscribe-result)))
      (is (= #{view-sig} (views/subscribed-views test-views-system)))
      (is (= [subscriber-key] (keys (:subscribed @test-views-system))))
      (is (= #{view-sig} (get-in @test-views-system [:subscribed subscriber-key])))
      (is (= #{subscriber-key} (get-in @test-views-system [:subscribers view-sig])))
      (is (= (md5-hash view-data) (get-in @test-views-system [:hashes view-sig])))
      (is (test-helpers/contains-only? @test-sent-data
                                       [{:subscriber-key subscriber-key
                                         :view-sig       (dissoc view-sig :namespace)
                                         :view-data      view-data}]))
      ; 4. unsubscribe.
      (views/unsubscribe! test-views-system view-sig subscriber-key context)
      (is (empty? (keys (:subscribed @test-views-system))))
      (is (empty? (keys (:subscribers @test-views-system))))
      (is (empty? (views/subscribed-views test-views-system)))
      (is (empty? (:hashes @test-views-system))))))

(deftest unauthorized-subscription-using-auth-fn
  (let [subscriber-key 123
        view-sig       (views/->view-sig :a :foo [])
        context        "some arbitrary context data"
        auth-fn        (fn [view-sig* subscriber-key* context*]
                         (is (= view-sig view-sig*))
                         (is (= subscriber-key subscriber-key*))
                         (is (= context context*))
                         ; false = unauthorized
                         false)
        options        (-> test-options
                           (assoc :auth-fn auth-fn))]
    ; 1. init views
    (views/init! test-views-system options)
    ; 2. subscribe to a view
    (let [subscribe-result (views/subscribe! test-views-system view-sig subscriber-key context)]
      (is (nil? subscribe-result))
      (is (empty? (keys (:subscribed @test-views-system))))
      (is (empty? (keys (:subscribers @test-views-system))))
      (is (empty? (views/subscribed-views test-views-system)))
      (is (empty? (:hashes @test-views-system))))))

(deftest unauthorized-subscription-using-auth-fn-calls-on-unauth-fn-when-set
  (let [subscriber-key    123
        view-sig          (views/->view-sig :a :foo [])
        context           "some arbitrary context data"
        auth-fn           (fn [view-sig* subscriber-key* context*]
                            (is (= view-sig view-sig*))
                            (is (= subscriber-key subscriber-key*))
                            (is (= context context*))
                            ; false = unauthorized
                            false)
        on-unauth-called? (atom false)
        on-unauth-fn      (fn [view-sig* subscriber-key* context*]
                            (is (= view-sig view-sig*))
                            (is (= subscriber-key subscriber-key*))
                            (is (= context context*))
                            (reset! on-unauth-called? true))
        options           (-> test-options
                              (assoc :auth-fn auth-fn
                                     :on-unauth-fn on-unauth-fn))]
    ; 1. init views
    (views/init! test-views-system options)
    ; 2. subscribe to a view
    (let [subscribe-result (views/subscribe! test-views-system view-sig subscriber-key context)]
      (is (nil? subscribe-result))
      (is @on-unauth-called?)
      (is (empty? (keys (:subscribed @test-views-system))))
      (is (empty? (keys (:subscribers @test-views-system))))
      (is (empty? (views/subscribed-views test-views-system)))
      (is (empty? (:hashes @test-views-system))))))

(deftest authorized-subscription-using-auth-fn
  (let [subscriber-key 123
        view-sig       (views/->view-sig :a :foo [])
        context        "some arbitrary context data"
        auth-fn        (fn [view-sig* subscriber-key* context*]
                         (is (= view-sig view-sig*))
                         (is (= subscriber-key subscriber-key*))
                         (is (= context context*))
                         true)
        options        (-> test-options
                           (assoc :auth-fn auth-fn))]
    ; 1. init views
    (views/init! test-views-system options)
    ; 2. subscribe to a view
    (let [view-data        (test-helpers/get-view-data test-views-system view-sig)
          subscribe-result (views/subscribe! test-views-system view-sig subscriber-key context)]
      (while (not (realized? subscribe-result)))
      (is (= #{view-sig} (views/subscribed-views test-views-system)))
      (is (= [subscriber-key] (keys (:subscribed @test-views-system))))
      (is (= #{view-sig} (get-in @test-views-system [:subscribed subscriber-key])))
      (is (= #{subscriber-key} (get-in @test-views-system [:subscribers view-sig])))
      (is (= (md5-hash view-data) (get-in @test-views-system [:hashes view-sig])))
      (is (test-helpers/contains-only? @test-sent-data
                                       [{:subscriber-key subscriber-key
                                         :view-sig       (dissoc view-sig :namespace)
                                         :view-data      view-data}])))))

(deftest unsubscribe-before-subscription-finishes-does-not-result-in-stuck-view
  (let [subscriber-key 123
        view-sig       (views/->view-sig :a :foo [])
        options        (-> test-options
                           (assoc :views slow-views))]
    ; 1. init views
    (views/init! test-views-system options)
    ; 2. subscribe to a view
    (let [subscribe-result (views/subscribe! test-views-system view-sig subscriber-key nil)]
      (is (= #{view-sig} (views/subscribed-views test-views-system)))
      (is (not (realized? subscribe-result)))
      ; 3. unsubscribe before subscription finishes (still waiting on initial data
      ;    retrieval to finish)
      (views/unsubscribe! test-views-system view-sig subscriber-key nil)
      (is (empty? (keys (:subscribed @test-views-system))))
      (is (empty? (keys (:subscribers @test-views-system))))
      (is (empty? (views/subscribed-views test-views-system)))
      (is (empty? (:hashes @test-views-system)))
      (is (empty? @test-sent-data))
      ; 4. wait for subscription to finish finally
      (while (not (realized? subscribe-result)))
      (is (empty? (keys (:subscribed @test-views-system))))
      (is (empty? (keys (:subscribers @test-views-system))))
      (is (empty? (views/subscribed-views test-views-system)))
      (is (empty? (:hashes @test-views-system)))
      (is (empty? @test-sent-data)))))
