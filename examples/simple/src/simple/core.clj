(ns simple.core
  (:require [views.core :as views])
  (:require [views.protocols :refer [IView]]))

(def view-system (atom {}))

(def messages (atom []))

(defn send-fn
  [subscriber-key [view-sig view-data]]
  (println "view refresh" subscriber-key view-sig view-data)
  (swap! messages conj [subscriber-key view-sig view-data]))

(views/init! view-system {:send-fn send-fn})

(def memory-datastore
  (atom {:a {:foo 1
             :bar 200
             :baz [1 2 3]}
         :b {:foo 2
             :bar 300
             :baz [2 3 4]}}))

(def memory-view-hint-type :ks-path)

(defrecord MemoryView [id ks]
  IView
  (id [_] id)
  (data [_ namespace parameters]
    (get-in @memory-datastore
            (-> [namespace]
                (into ks)
                (into parameters))))
  (relevant? [_ namespace parameters hints]
    (some #(and (= namespace (:namespace %))
                (= ks (:hint %))
                (= memory-view-hint-type (:type %)))
          hints)))

; Add views for in memory store
(views/add-views!
  view-system
  [(MemoryView. :foo [:foo])
   (MemoryView. :bar [:bar])
   (MemoryView. :baz [:baz])])

; Add subscriber
(views/subscribe!
  view-system                        ; view system atom
  {:namespace :a :view-id :foo :parameters []};}
  123                                ; subscriber key
  nil)

(defn memdb-assoc-in!
  [vs namespace ks v]
  (let [path  (into [namespace] ks)
        hints [(views/hint namespace ks memory-view-hint-type)]]
    (swap! memory-datastore assoc-in path v)
    (views/refresh-views! vs hints)
    @memory-datastore))

(memdb-assoc-in! view-system :a [:foo] :a)
(memdb-assoc-in! view-system :a [:foo] :b)
(deref messages)
