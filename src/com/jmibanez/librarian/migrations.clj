(ns com.jmibanez.librarian.migrations
  (:require [ragtime.jdbc :as jdbc]
            [ragtime.repl :as repl]
            [mount.core :as mount
             :refer [defstate]]
            [taoensso.timbre :as timbre]
            [com.jmibanez.librarian.config :as config]))

(timbre/refer-timbre)

(defstate ^:dynamic *migration-config*
  :start {:datastore (jdbc/sql-database config/*datasource*)
          :migrations (jdbc/load-resources "migrations")})

(defn migrate []
  (mount/start #'config/*datasource*
               #'*migration-config*)
  (try
    (repl/migrate *migration-config*)
    (finally
      (mount/stop))))

(defn rollback [& rest]
  (mount/start #'config/*datasource*
               #'*migration-config*)
  (apply repl/rollback *migration-config* rest)
  (mount/stop))
