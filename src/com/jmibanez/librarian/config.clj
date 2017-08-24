(ns com.jmibanez.librarian.config
  (:require [environ.core :refer [env]]
            [mount.core :refer [defstate]]
            [taoensso.timbre :as timbre]
            [hikari-cp.core :refer [make-datasource
                                    close-datasource]]))


(def db-uri (env :db-connection-uri))
(def db-user (env :db-connection-user))
(def db-password (env :db-connection-password))

(defstate ^:dynamic *datasource*
  :start {:datasource
          (make-datasource {:pool-name "librarian-pool"
                            :jdbc-url db-uri
                            :username db-user
                            :password db-password
                            :minimum-idle (Integer/parseInt
                                           (env :db-pool-initial "1"))
                            :maximum-pool-size (Integer/parseInt
                                                (env :db-pool-max "10"))})}
  :stop (close-datasource (:datasource *datasource*)))


(def event-buffer-size (Integer/parseInt (env :event-buffer-size "10")))

(def indexer-batch-size (Integer/parseInt (env :indexer-batch-size "100")))
(def indexer-period (Integer/parseInt (env :indexer-period "60000")))

(defn default-sysout-appender
  [data]
  (let [{:keys [output_]} data
        formatted-output-str (force output_)]
    (println formatted-output-str)))


(def timestamp-opts (assoc timbre/default-timestamp-opts
                           :pattern "yyyy-MM-dd HH:mm:ss"))

(timbre/set-config! {:level (keyword (env :log-level "warn"))
                     :ns-whitelist [] #_["com.jmibanez.librarian.*"]
                     :ns-blacklist [] #_[""]

                     :middleware []

                     :timestamp-opts timestamp-opts
                     :output-fn timbre/default-output-fn

                     :appenders {:base-sysout-appender {:enabled? true
                                                        :async? false
                                                        :min-level nil
                                                        :rate-limit [[1 250] [10 5000]]
                                                        :output-fn :inherit
                                                        :fn default-sysout-appender}}})


