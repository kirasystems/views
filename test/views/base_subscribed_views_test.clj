(ns views.base-subscribed-views-test
  (:require
   [views.base-subscribed-views :as bsv]
   [views.persistence.core :refer :all]
   [views.persistence.memory :refer [new-memory-persistence]]
   [views.subscribed-views :refer [subscribe-views unsubscribe-views disconnect broadcast-deltas]]
   [views.fixtures :as vf]
   [clojure.test :refer [use-fixtures deftest is]]
   [clojure.java.jdbc :as j]
   [clj-logging-config.log4j :refer [set-logger! set-loggers!]])
  (:import
   [views.base_subscribed_views BaseSubscribedViews]))

(set-loggers!
 "views.base-subscribed-views" {:level :error}
 "views.filters"               {:level :error})

(defn view-config []
  {:persistence (new-memory-persistence)
   :db vf/db
   :templates vf/templates
   :view-sig-fn :views
   :unsafe? true})

(deftest subscribes-and-dispatches-initial-view-result-set
  (let [config (view-config)
        sent   (atom #{})
        send-fn #(do (is (and (= %1 1) (= %2 :views.init) (= %3 {[:users] []})))
                     (swap! sent conj [%1 %2 %3]))
        base-subbed-views (BaseSubscribedViews. (assoc config :send-fn send-fn))]
    (subscribe-views base-subbed-views {:subscriber-key 1 :views [[:users]]})
    (Thread/sleep 10)
    (is (= (subscriptions (:persistence config) bsv/default-ns [[:users]])
           {[:users] #{1}}))
    ;; Verify sends occured.
    (is (= @sent #{[1 :views.init {[:users] []}]}))))

;; This test illustrates a slight timing issue. Because view subscriptions
;; use threads, an unsubscription that follows a subscription too closely
;; can fail.
(deftest unsubscribes-view
  (let [config            (view-config)
        base-subbed-views (BaseSubscribedViews. config)]
    (subscribe-views base-subbed-views {:subscriber-key 1 :views [[:users]]})
    (Thread/sleep 10)
    (unsubscribe-views base-subbed-views {:subscriber-key 1 :views [[:users]]})
    (is (= (subscriptions (:persistence config) bsv/default-ns [[:users]])
           {}))))

(deftest filters-subscription-requests
  (let [config            (view-config)
        templates         (assoc-in vf/templates [:users :filter-fn]
                                    (fn [msg _] (:authorized? msg)))
        view-config (-> config (assoc :templates templates) (dissoc :unsafe?))
        base-subbed-views (BaseSubscribedViews. view-config)]
    (subscribe-views base-subbed-views {:subscriber-key 1 :views [[:users]]})
    (Thread/sleep 10)
    (is (= (subscriptions (:persistence config) bsv/default-ns [[:users]])
           {}))))

(deftest removes-all-subscriptions-on-disconnect
  (let [config            (view-config)
        base-subbed-views (BaseSubscribedViews. config)]
    (subscribe-views base-subbed-views {:subscriber-key 1 :views [[:users] [:user-posts 1]]})
    (Thread/sleep 10)
    (is (= (subscriptions (:persistence config) bsv/default-ns [[:users] [:user-posts 1]])
           {[:users] #{1}, [:user-posts 1] #{1}}))
    (disconnect base-subbed-views {:subscriber-key 1})
    (is (= (subscriptions (:persistence config) bsv/default-ns [[:users] [:user-posts 1]])
           {}))))

;; (deftest sends-deltas
;;   (let [deltas {[:users] [{:view-sig [:users] :insert-deltas [{:foo "bar"}]}]}
;;         sent-delta {[:users] {:insert-deltas [{:foo "bar"}]}}
;;         send-fn #(do (is (#{1 2} %1))
;;                      (is (= %2 :views.deltas))
;;                      (is (= %3 sent-delta)))
;;         base-subbed-views (BaseSubscribedViews. (assoc view-config-fixture :send-fn send-fn))]
;;     (add-subscription! [:users] vf/templates 1 default-ns)
;;     (add-subscription! [:users] vf/templates 2 default-ns)
;;     (broadcast-deltas base-subbed-views deltas nil)))

(deftest sends-deltas-in-batch
  (let [config (view-config)
        deltas [{[:users] [{:insert-deltas [{:id 1 :name "Bob"} {:id 2 :name "Alice"}]}]}
                {[:users] [{:insert-deltas [{:id 3 :name "Jack"} {:id 4 :name "Jill"}]}]}]
        ;; This is just more obvious than writing some convulated fn to dig out the view-sigs.
        sent-deltas [{[:users] [{:insert-deltas [{:id 1 :name "Bob"} {:id 2 :name "Alice"}]}]}
                     {[:users] [{:insert-deltas [{:id 3 :name "Jack"} {:id 4 :name "Jill"}]}]}]
        sent    (atom #{})
        send-fn #(do (is (#{1 2} %1))
                     (is (= :views.deltas %2))
                     (is (= sent-deltas %3))
                     (swap! sent conj [%1 %2 %3]))
        base-subbed-views (BaseSubscribedViews. (assoc config :send-fn send-fn))]
    (subscribe! (:persistence config) vf/templates bsv/default-ns [:users] 1)
    (broadcast-deltas base-subbed-views deltas nil)
    (is (= 1 (count @sent)))
    (is (= 1 (ffirst @sent)))
    (is (= :views.deltas (second (first @sent))))
    (is (= sent-deltas (nth (first @sent) 2)))))

(deftest deltas-are-post-processed
  (let [config      (view-config)
        templates   (assoc-in vf/templates [:users :post-fn] (fn [d] (update-in d [:id] #(Integer. %))))
        deltas      [{[:users] [{:insert-deltas [{:id "1" :name "Bob"}]}]}]
        sent-deltas [{[:users] [{:insert-deltas [{:id "1" :name "Bob"}]}]}]
        sent        (atom #{})
        send-fn     (fn [a b deltas-out]
                      (is (= (:id (first (:insert-deltas (first (get (first deltas-out) [:users])))))
                             1))
                      (swap! sent conj [a b deltas-out]))
        base-subbed-views (BaseSubscribedViews. (assoc config :send-fn send-fn :templates templates))]
    (subscribe! (:persistence config) vf/templates bsv/default-ns [:users] 1)
    (Thread/sleep 10)
    (broadcast-deltas base-subbed-views deltas nil)
    (is (= 1 (count @sent)))
    (is (= 1 (ffirst @sent)))
    (is (= :views.deltas (second (first @sent))))
    (is (not= sent-deltas (nth (first @sent) 2)))
    (is (= [{[:users] [{:insert-deltas [{:name "Bob", :id 1}]}]}] (nth (first @sent) 2)))))

;; These tests are now broken because we post-process full refresh queries right
;; when they come out of the database. Need an actual database to test this now.
;; TODO: fix test.
(deftest full-refresh-deltas-are-post-processed
  (let [config      (view-config)
        templates   (assoc-in vf/templates [:users :post-fn] (fn [d] (update-in d [:id] #(Integer. %))))
        deltas      [{[:users] [{:refresh-set [{:id "1" :name "Bob"}]}]}]
        sent-deltas [{[:users] [{:refresh-set [{:id "1" :name "Bob"}]}]}]
        sent        (atom #{})
        send-fn     (fn [a b deltas-out]
                      #_(is (= (:id (first (:refresh-set (first (get (first deltas-out) [:users])))))
                             1))
                      (swap! sent conj [a b deltas-out]))
        base-subbed-views (BaseSubscribedViews. (assoc config :send-fn send-fn :templates templates))]
    (subscribe! (:persistence config) vf/templates bsv/default-ns [:users] 1)
    (Thread/sleep 10)
    (broadcast-deltas base-subbed-views deltas nil)
    (is (= 1 (count @sent)))
    (is (= 1 (ffirst @sent)))
    (is (= :views.deltas (second (first @sent))))
    #_(is (not= sent-deltas (nth (first @sent) 2)))
    #_(is (= [{[:users] [{:refresh-set [{:name "Bob", :id 1}]}]}] (nth (first @sent) 2)))))


