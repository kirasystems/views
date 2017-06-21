(ns views.hash
  (:require
    [hasch.benc :refer [-coerce magics PHashCoercion]]
    [hasch.core :refer [edn-hash]]
    [hasch.platform :refer [encode md5-message-digest]]))

(extend-protocol PHashCoercion
  java.math.BigDecimal
  (-coerce [this md-create-fn write-handlers]
    (encode (:number magics) (.getBytes (.toString this) "UTF-8")))

  clojure.lang.BigInt
  (-coerce [this md-create-fn write-handlers]
    (encode (:number magics) (.getBytes (.toString this) "UTF-8"))))

(defn md5-hash
  "This hashing function is a drop-in replacement of Clojure's hash function. Unfortunately, Clojure's
   hash function has the same result for 0 and nil values. Therefore, passing from nil or 0 to the
   other won't trigger a view refresh."
  [value]
  (edn-hash value md5-message-digest {}))