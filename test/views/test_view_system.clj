(ns views.test-view-system
  (:use
    views.protocols
    views.core)
  (:import (clojure.lang Atom)))

(def base-memory-db-contents
  {:a {:foo 1 :bar 200 :baz [1 2 3]}
   :b {:foo 2 :bar 300 :baz [2 3 4]}})

(def memory-database
  (atom base-memory-db-contents))

(def test-views-system
  (atom {}))

(defn reset-memory-db-fixture [f]
  (reset! memory-database base-memory-db-contents)
  (f))

(defn reset-test-views-system [f]
  (reset! test-views-system {})
  (f)
  (if (seq @test-views-system)
    (shutdown! test-views-system)))

(def memory-view-hint-type :memory-db)

(defrecord MemoryView [id ks]
  IView
  (id [_] id)
  (data [_ namespace parameters]
    (get-in @memory-database (-> [namespace]
                                 (into ks)
                                 (into parameters))))
  (relevant? [_ namespace parameters hints]
    (some #(and (= namespace (:namespace %))
                (= ks (:hint %))
                (= memory-view-hint-type (:type %)))
          hints)))

(defrecord SlowMemoryView [id ks]
  IView
  (id [_] id)
  (data [_ namespace parameters]
    ; simulate a slow database query
    (Thread/sleep 1000)
    (get-in @memory-database (-> [namespace]
                                 (into ks)
                                 (into parameters))))
  (relevant? [_ namespace parameters hints]
    (some #(and (= namespace (:namespace %))
                (= ks (:hint %))
                (= memory-view-hint-type (:type %)))
          hints)))

(def views
  [(MemoryView. :foo [:foo])
   (MemoryView. :bar [:bar])
   (MemoryView. :baz [:baz])])

(def slow-views
  [(SlowMemoryView. :foo [:foo])
   (SlowMemoryView. :bar [:bar])
   (SlowMemoryView. :baz [:baz])])

(defn memory-db-assoc-in!
  [^Atom view-system namespace ks v]
  (let [ms (swap! memory-database assoc-in (into [namespace] ks) v)]
    (put-hints! view-system [(hint namespace ks memory-view-hint-type)])
    ms))
