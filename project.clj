(defproject views "1.2.1"
  :description "A view to the past helps navigate the future."

  :url "https://github.com/diligenceengine/views"

  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/tools.logging "0.2.6"]
                 [honeysql "0.4.3"]
                 [clj-logging-config "1.9.10"]
                 [prismatic/plumbing "0.3.5"]
                 [pjstadig/humane-test-output "0.6.0"]
                 [environ "1.0.0"]]

  :profiles {:test {:dependencies [[org.clojure/tools.nrepl "0.2.3"]
                                   [environ "1.0.0"]
                                   [org.clojure/data.generators "0.1.2"]]

                    :injections [(require 'pjstadig.humane-test-output)
                                 (pjstadig.humane-test-output/activate!)]}}

  :plugins [[lein-environ "0.4.0"]])
