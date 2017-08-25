(ns com.jmibanez.librarian.migrations
  (:require [ragtime.jdbc :as jdbc]
            [ragtime.repl :as repl]
            [mount.core :as mount
             :refer [defstate]]
            [taoensso.timbre :as timbre]
            [com.jmibanez.librarian
             [config :as config]
             [indexer :as idx]]))

(timbre/refer-timbre)

(defstate ^:dynamic *migration-config*
  :start {:datastore (jdbc/sql-database config/*datasource*)
          :migrations (jdbc/load-resources "migrations")})

(defn do-migrate []
  (repl/migrate *migration-config*))

(defn migrate []
  (mount/start-without #'idx/*indexer-pool*
                       #'idx/*indexer-queue*
                       #'idx/indexer-chan)
  (do-migrate)
  (mount/stop))

(defn rollback [& rest]
  (mount/start)
  (apply repl/rollback *migration-config* rest)
  (mount/stop))
