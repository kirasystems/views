(ns views.filters
  (:require
   [clojure.tools.logging :refer [debug info warn error]]))

(defn view-filter
  "Takes a subscription request msg, a collection of view-sigs and
   the config templates hash-map for an app. Checks if there is
   a global filter-fn in the hash-map metadata and checks against
   that if it exists, as well as against any existing filter
   functions for individual template config entries. Template
   config hash-map entries can specify a filter-fn using the key
   :filter-fn, and the global filter-fn is the same, only on
   the config meta-data (i.e. (with-meta templates {:filter-fn ...}))

   By default throws an exception if no filters are present.
   By passing in {:unsafe true} in opts, this can be overridden."
  [msg view-sigs templates & opts]
  (let [global-filter-fn (:filter-fn (meta templates))]
    (filterv
     #(let [filter-fn (:filter-fn (get templates (first %)))]
        (cond
         (and filter-fn global-filter-fn)
         (and (global-filter-fn msg %) (filter-fn msg %))

         filter-fn
         (filter-fn msg %)

         global-filter-fn
         (global-filter-fn msg %)

         :else
         (if (-> opts first :unsafe?)
           (do (warn "YOU ARE RUNNING IN UNSAFE MODE, AND NO FILTERS ARE PRESENT FOR VIEW-SIG: " %)
               true)
           (throw (Exception. (str "No filter set for view " %))))))
     view-sigs)))
