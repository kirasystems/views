(defproject kirasystems/views "2.0.3-SNAPSHOT"
  :description   "A view to the past helps navigate the future."
  :url           "https://github.com/kirasystems/views"

  :license       {:name "MIT License"
                  :url "http://opensource.org/licenses/MIT"}

  :repositories  [["releases" {:url "https://clojars.org/repo"
                               :sign-releases false
                               :username :env
                               :password :env}]]

  :dependencies  [[clj-logging-config "1.9.12"]
                  [environ "1.1.0"]
                  [io.replikativ/hasch "0.3.4"]
                  [org.clojure/tools.logging "0.4.0"]]

  :profiles      {:dev {:dependencies [[org.clojure/clojure "1.8.0"]]}
                  :test {:dependencies [[org.clojure/tools.nrepl "0.2.13"]
                                        [org.clojure/data.generators "0.1.2"]
                                        [pjstadig/humane-test-output "0.8.2"]]

                         :injections [(require 'pjstadig.humane-test-output)
                                      (pjstadig.humane-test-output/activate!)]}}

  :plugins       [[lein-environ "1.1.0"]]

  :release-tasks [["vcs" "assert-committed"]
                  ["change" "version" "leiningen.release/bump-version" "release"]
                  ["vcs" "commit"]
                  ["vcs" "tag" "--no-sign"]
                  ["change" "version" "leiningen.release/bump-version"]
                  ["vcs" "commit"]
                  ["vcs" "push"]])
