(defproject views "0.1.0-SNAPSHOT"
  :description "You underestimate the power of the SQL side"

  :url "https://github.com/diligenceengine/views"

  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/tools.logging "0.2.6"]
                 [org.clojure/core.async "0.1.303.0-886421-alpha"]
                 [org.clojure/java.jdbc "0.3.3"]
                 [honeysql "0.4.3"]
                 [org.postgresql/postgresql "9.2-1003-jdbc4"]]

  :profiles {:test {:dependencies [[org.clojure/tools.nrepl "0.2.3"]
                                   [environ "0.4.0"]
                                   [org.clojure/data.generators "0.1.2"]]}}

  :plugins [[lein-environ "0.4.0"]])
