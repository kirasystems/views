(defproject kirasystems/views "1.4.5-SNAPSHOT"
  :description "A view to the past helps navigate the future."

  :url "https://github.com/kirasystems/views"

  :license {:name "MIT License"
            :url "http://opensource.org/licenses/MIT"}

  :repositories [["releases" {:url "https://clojars.org/repo"
                              :sign-releases false
                              :username :env
                              :password :env}]]

  :dependencies [[org.clojure/tools.logging "0.3.1"]
                 [clj-logging-config "1.9.12"]
                 [prismatic/plumbing "0.4.3"]
                 [pjstadig/humane-test-output "0.7.0"]
                 [environ "1.0.0"]]

  :profiles {:dev {:dependencies [[org.clojure/clojure "1.7.0"]]}
             :test {:dependencies [[org.clojure/tools.nrepl "0.2.10"]
                                   [environ "1.0.0"]
                                   [org.clojure/data.generators "0.1.2"]]

                    :injections [(require 'pjstadig.humane-test-output)
                                 (pjstadig.humane-test-output/activate!)]}}

  :plugins [[lein-ancient "0.6.7"]
            [lein-environ "0.4.0"]]

  :release-tasks [["vcs" "assert-committed"]
                  ["change" "version" "leiningen.release/bump-version" "release"]
                  ["vcs" "commit"]
                  ["vcs" "tag" "--no-sign"]
                  ["change" "version" "leiningen.release/bump-version"]
                  ["vcs" "commit"]
                  ["vcs" "push"]])
