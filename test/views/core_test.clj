(ns views.core-test
  (:require
   [clojure.test :refer [is deftest use-fixtures]]
   [views.protocols :refer [IView id data relevant?]]
   [views.core :as v]))

(def memory-system (atom {}))

(defrecord MemoryView [id ks]
  IView
  (id [_] id)
  (data [_ namespace parameters]
    (get-in @memory-system (-> [namespace] (into ks) (into parameters))))
  (relevant? [_ namespace parameters hints]
    (some #(and (= namespace (:namespace %)) (= ks (:hint %))) hints)))

(def example-view-system
  {:views   {:foo (MemoryView. :foo [:foo])
             :bar (MemoryView. :bar [:bar])
             :baz (MemoryView. :baz [:baz])}
   :send-fn (fn [_ _])})

(deftest subscribes-views
  (let [view-system (atom example-view-system)
        subscribe1  (v/subscribe! view-system :namespace :foo [] 1)
        subscribe2  (v/subscribe! view-system :namespace :bar [] 2)]
    (while (or (not (realized? subscribe1)) (not (realized? subscribe2)))) ; block until futures are realized
    (is (= #{[:namespace :bar []] [:namespace :foo []]}
           (v/subscribed-views @view-system)))))

(deftest unsubscribes-a-view
  (let [view-system (atom example-view-system)
        subscribe1  (v/subscribe! view-system :namespace :foo [] 1)
        subscribe2  (v/subscribe! view-system :namespace :bar [] 2)]
    (while (or (not (realized? subscribe1)) (not (realized? subscribe2)))) ; block until futures are realized
    (v/unsubscribe! view-system :namespace :foo [] 1)
    (is (= #{[:namespace :bar []]} (v/subscribed-views @view-system)))))
