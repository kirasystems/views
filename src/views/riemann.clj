(ns views.riemann
  (:require
   [riemann.client :refer [tcp-client]]))

(defonce rclient (tcp-client {:host "127.0.0.1" :port 5555}))
