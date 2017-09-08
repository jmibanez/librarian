(defproject com.jmibanez/librarian "0.1.0-SNAPSHOT"
  :description "Librarian Document Storage Engine"
  :url "http://librarian.jmibanez.com/"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :min-lein-version "2.0.0"
  :plugins [[lein-environ "1.1.0"]
            [lein-expectations "0.0.7"]
            [lein-ring "0.9.7"]]
  :profiles {:project/dev {:dependencies [[expectations "2.0.9"]
                                          [javax.servlet/servlet-api "2.5"]
                                          [ring/ring-mock "0.3.0"]
                                          [org.clojure/test.check "0.9.0"]
                                          [prismatic/schema-generators "0.1.0"]]}
             :project/test {}

             :profiles/dev {}
             :profiles/test {}

             :dev [:project/dev :profiles/dev]
             :test [:project/dev :project/test :profiles/test]}

  :ring {:handler com.jmibanez.librarian.handler/app
         :init mount.core/start}

  :aliases {"migrate" ["run" "-m" "com.jmibanez.librarian.migrations/migrate"]
            "rollback" ["run" "-m" "com.jmibanez.librarian.migrations/rollback"]}

  :dependencies [[org.clojure/clojure "1.8.0"]

                 [org.postgresql/postgresql "42.1.4"]
                 [environ "1.1.0"]
                 [mount "0.1.10"]
                 [com.taoensso/timbre "4.10.0"]
                 [com.taoensso/tufte "1.1.1"]
                 [com.fzakaria/slf4j-timbre "0.3.2"]
                 [org.clojure/core.cache "0.6.5"]
                 [org.clojure/core.async "0.3.443"]
                 [danlentz/clj-uuid "0.1.7"]
                 [camel-snake-kebab "0.4.0"]
                 [clj-time "0.14.0"]
                 [overtone/at-at "1.2.0"]
                 [digest "1.4.5"]
                 [com.zaxxer/HikariCP "2.6.3"]
                 [hikari-cp "1.7.6"]
                 [com.layerware/hugsql "0.4.7"]
                 [honeysql "0.9.0"]
                 [cheshire "5.7.1"]
                 [json-path "1.0.0"]
                 [ragtime "0.7.1"] ;; 0.6.3
                 [prismatic/schema "1.1.6"]
                 [compojure "1.6.0"]
                 [metosin/compojure-api "1.1.11"]
                 [metosin/ring-http-response "0.9.0"]
                 [ring/ring-defaults "0.3.1"]])
