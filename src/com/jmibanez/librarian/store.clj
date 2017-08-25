(ns com.jmibanez.librarian.store
  (:require [taoensso.timbre :as timbre]
            [taoensso.tufte :as tufte]
            [mount.core :refer [defstate]]
            [clj-uuid :as uuid]
            [clojure.core.cache :as cache]
            [clojure.core.async :refer [chan close! go thread
                                        >!! >! <!! <!
                                        mult sliding-buffer]]
            [overtone.at-at :as at-at]
            [digest]
            [camel-snake-kebab.core :refer [->kebab-case]]
            [camel-snake-kebab.extras :refer [transform-keys]]
            [schema.core :as s]

            [clojure.java.jdbc :as jdbc]
            [clojure.data :as data]
            [cheshire.core :as json]
            [cheshire.generate :refer [add-encoder encode-str]]
            [hugsql.core :as hugsql]

            [com.jmibanez.librarian
             [core-schema :as core-schema]
             [config :as config]
             [util :refer [defcacheable]]])
  (:import org.postgresql.util.PGobject
           java.util.Date))


(timbre/refer-timbre)

(def root-type #uuid "81ed782c-fd86-4141-81ec-a8640aa3188b")
(def root-type-name "root")
(def schema-type #uuid "143246e3-c0b7-59e3-ae17-29b99ff0d5ac")
(def schema-type-name "schema")
(def schema-state-type #uuid "a304501d-bef5-5612-8670-67512a56aa1c")
(def schema-state-name "schema-state")

(def DocumentState s/Keyword)

(def Id s/Uuid)

;; Contexts are UUIDs
(def Context (s/maybe s/Uuid))

(s/defrecord Document [id                 :- Id
                       name               :- (s/maybe s/Str)
                       type               :- (s/maybe Id)
                       context            :- (s/maybe Context)
                       state              :- DocumentState
                       version            :- (s/maybe s/Str)
                       document           :- s/Any
                       date-created       :- (s/maybe s/Inst)
                       date-last-modified :- (s/maybe s/Inst)])


(def TransactionState (s/enum :started
                              :dirty
                              :committed
                              :cancelled
                              :conflict))

(s/defrecord Transaction [id             :- Id
                          context        :- Context
                          timeout        :- s/Int
                          state          :- TransactionState
                          last-operation :- s/Inst])

(defstate ^:dynamic *reaper-task-pool*
  :start (at-at/mk-pool)
  :stop  (at-at/stop-and-reset-pool!
          *reaper-task-pool* :strategy :stop))

(declare reschedule-reapers)
(defstate ^:dynamic *reaper*
  :start (reschedule-reapers))


(declare start-gc)
(defstate gc-reaper
  :start (start-gc))

(def StoreEventType (s/enum :transaction-start
                            :transaction-commit
                            :transaction-cancel
                            :transaction-conflict
                            :document-write))
(s/defschema StoreEvent {:event       StoreEventType
                         :context     Context
                         :payload     s/Any
                         (s/optional-key :transaction) (s/maybe Id)})

(defstate ^:dynamic *_events-channel*
  :start (-> config/event-buffer-size
             (sliding-buffer)
             (chan))
  :stop  (close! *_events-channel*))

(defstate ^:dynamic *events*
  :start (mult *_events-channel*))

(hugsql/def-db-fns "sql/core.sql")

(declare doc-row->Document)
(declare stub-row->Transaction)

(def default-transaction-timeout 5000)


(declare schedule-transaction-reaper!)
(declare create-db-transaction-row!)
(s/defn start-transaction! :- Transaction
  ([context    :- Context]
   (start-transaction! context default-transaction-timeout))

  ([context    :- Context
    timeout    :- s/Int]
   (let [transaction-id (uuid/v4)
         transaction    (create-db-transaction-row! context
                                                    transaction-id
                                                    timeout)]
     (schedule-transaction-reaper! transaction)
     (>!! *_events-channel* {:event   :transaction-start
                             :context context
                             :payload transaction})
     transaction)))

(declare cancel-transaction-reaper!)
(declare cas-transaction-state!)
(s/defn commit-transaction! :- Transaction
  [transaction :- Transaction]
  (tufte/p
   ::commit-transaction!
   (let [transaction-id  (:id transaction)
         param           {:transaction-id transaction-id}

         conflict (assoc transaction :state :conflict)
         committed (assoc transaction :state :committed)]

     (>!! *_events-channel* {:event   :transaction-commit
                             :context (:context transaction)
                             :payload transaction})

     (jdbc/with-db-transaction [c config/*datasource*]

       ;; Verify if we should be able to proceed
       (let [{:keys [applicable_num
                     total_num]}
             (select-applicable-count-in-transaction c param)]

         (if-not (= applicable_num
                    total_num)
           ;; Conflict due to concurrent transaction
           (do
             (cas-transaction-state! transaction :conflict
                                     identity)
             (debug "Conflict:" transaction-id
                    "applicable " applicable_num "!= total " total_num)
             conflict)

           (if-not (cas-transaction-state! transaction :committed
                                           #(contains? #{:started :dirty} %))
             ;; State unexpected; yield conflict
             conflict

             ;; Success case
             (do
               (cancel-transaction-reaper! committed)
               (commit-transaction-details! c param)
               committed))))))))

(declare cancel-transaction-reaper!)
(declare reap-transaction!)
(s/defn cancel-transaction! :- Transaction
  [transaction :- Transaction]
  (let [transaction-id  (:id transaction)
        new-transaction (assoc transaction :state :cancelled)]
    (>!! *_events-channel* {:event   :transaction-cancel
                            :context (:context transaction)
                            :payload transaction})
    (cancel-transaction-reaper! transaction)
    (reap-transaction! transaction)
    new-transaction))


(defmacro with-transaction [[transaction-sym context] & body]
  `(let [~transaction-sym (start-transaction! ~context)]
     (try
       ~@body
       (catch Exception e
         (cancel-transaction! ~transaction-sym))
       (finally
         (commit-transaction! ~transaction-sym)))))


(declare ensure-doc-header)
(declare current-doc-version)
(declare next-doc-version)
(declare cancel-transaction-reaper!)
(s/defn write-document! :- Document
  [transaction :- Transaction
   document    :- Document]
  (tufte/p
   ::write-document!
   (jdbc/with-db-transaction [c config/*datasource*]
     (if (cas-transaction-state! transaction :dirty
                                 #(contains? #{:started :dirty} %))
       (let [header (ensure-doc-header c document)
             doc (:document document)
             prev-version (current-doc-version c transaction document)
             doc-version (digest/sha-256 (json/generate-string doc))
             doc-version-row (insert-document-version!
                              c {:id       (:id document)
                                 :document doc
                                 :previous prev-version
                                 :version  doc-version})
             document (assoc document :version doc-version)]
         (bind-document-version-to-tx! c {:transaction-id   (:id transaction)
                                          :document-id      (:id document)
                                          :version          doc-version})

         (>!! *_events-channel* {:event       :document-write
                                 :context     (:context transaction)
                                 :payload     document
                                 :transaction (:id transaction)})
         document)

       ;; FIXME: Raise exception: Invalid transaction state
       nil))))

(s/defn set-document-state! :- Document
  [transaction :- Transaction
   document    :- Document
   new-state   :- DocumentState]
  (jdbc/with-db-transaction [c config/*datasource*]
    (bind-document-state-update-to-tx! c {:transaction-id   (:id transaction)
                                          :document-id      (:id document)
                                          :version          (:version document)
                                          :state            new-state})
    (assoc document :state new-state)))

(s/defn get-document-by-id :- (s/maybe Document)
  [context :- Context
   id      :- Id]

  (jdbc/with-db-transaction [c config/*datasource*
                             {:read-only? true}]
    (if-let [doc-row
             (select-recent-document-by-id c {:context context
                                              :id id})]
      (doc-row->Document doc-row)
      (debug "Unknown document or document could not be retrieved:" id))))

(s/defn get-document-by-name :- (s/maybe Document)
  [context :- Context
   type    :- Id
   name    :- s/Str]

  (jdbc/with-db-transaction [c config/*datasource*
                             {:read-only? true}]
    (if-let [doc-row
             (select-recent-document-by-name c {:context context
                                                :type    type
                                                :name    name})]
      (doc-row->Document doc-row)
      (debug "Unknown document or document could not be retrieved (name):" name))))

(s/defn get-document-version :- (s/maybe Document)
  [context :- Context
   id      :- Id
   version :- s/Str]

  (jdbc/with-db-transaction [c config/*datasource*
                             {:read-only? true}]
    (if-let [doc-row
             (select-document-by-id-and-version c {:context context
                                                   :id id
                                                   :version version})]
      (doc-row->Document doc-row)
      nil)))

(s/defn get-all-document-versions :- [Document]
  [context :- Context
   id      :- Id]
  (jdbc/with-db-transaction [c config/*datasource*
                             {:read-only? true}]
    (map doc-row->Document
         (select-versions-of-document-by-id c {:context context
                                               :id id}))))
(defprotocol Executable
  (execute-this [this conn params]))

(s/defn exec :- s/Any
  [executable :- Executable
   params]
  (jdbc/with-db-transaction [c config/*datasource*]
    (tufte/p
     ::exec
     (execute-this executable c params))))



(defn doc-row->Document
  [doc-row]
  (let [document (:document doc-row)]
    (strict-map->Document
     (-> (transform-keys ->kebab-case doc-row)
         (update :state keyword)
         (assoc :document document)))))

(defn stub-row->Transaction
  [stub-row]
  (strict-map->Transaction
   (update (transform-keys ->kebab-case stub-row)
           :state keyword)))

(defn value-to-json-pgobject [value]
  (doto (PGobject.)
    (.setType "jsonb")
      (.setValue (json/generate-string value))))


(def ^:private keyword-chars
  #"^[A-Za-z][A-Za-z0-9\.\*\+!\-_'\?]*$")
(defn- safe-keyword [k]
  (if (re-find keyword-chars k)
    ;; Pass-through strings that are valid keywords
    (keyword k)

    k))

(extend-protocol jdbc/ISQLValue
  clojure.lang.Keyword
  (sql-value [value] (name value))

  clojure.lang.IPersistentMap
  (sql-value [value] (value-to-json-pgobject value))

  clojure.lang.IPersistentVector
  (sql-value [value] (value-to-json-pgobject value)))

(extend-protocol jdbc/IResultSetReadColumn
  PGobject
  (result-set-read-column [pgobj metadata idx]
    (let [type  (.getType pgobj)
          value (.getValue pgobj)]
      (case type
        "json" (json/parse-string value safe-keyword)
        "jsonb" (json/parse-string value safe-keyword)
        :else value))))


(add-encoder java.util.UUID encode-str)

(defn create-db-transaction-row! [context transaction-id timeout]
  (jdbc/with-db-transaction [c config/*datasource*]
    (stub-row->Transaction
     (insert-transaction-row! c {:id transaction-id
                                 :context context
                                 :timeout timeout
                                 :state   :started}))))

(defcacheable get-document-header [c doc-id]
  (cache/lu-cache-factory {})
  (select-document-header c {:id doc-id}))

(defn ensure-doc-header [c document]
  (if-let [doc-header (get-document-header c (:id document))]
    document
    (do
      (insert-document-header! c document)
      document)))

(defn current-doc-version [c transaction document]
  (let [transaction-id (:id transaction)
        doc-id (:id document)]
    (:version
     (select-current-version-for-document c {:id doc-id
                                             :transaction-id transaction-id}))))

(defn next-doc-version [c transaction document]
  (let [transaction-id (:id transaction)
        doc-id (:id document)]
    (:version
     (select-next-version-for-document c {:id doc-id
                                          :transaction-id transaction-id}))))

(defn cas-transaction-state! [transaction
                              new-state
                              check-fn]
  (jdbc/with-db-transaction [c config/*datasource*
                             {:isolation :read-committed}]

    (if (check-fn (-> (select-transaction-stub c transaction)
                      (stub-row->Transaction)
                      (:state)))

      (do
        (update-transaction-stub-state! c {:id (:id transaction)
                                           :state new-state})
        true)

      nil)))

;; Transaction reaper

(defn reschedule-reapers []
  (let [reaper (agent {})]

    (jdbc/with-db-transaction [c config/*datasource*]
      (let [open-transactions (->> (select-open-transaction-stubs c)
                                   (map stub-row->Transaction))]
        (doseq [transaction open-transactions]
          (schedule-transaction-reaper! transaction reaper))))

    reaper))

(declare clear-transaction-items!)
(defn reap-transaction! [transaction]
  (info "Reap: " transaction)
  (if (cas-transaction-state! transaction :cancelled
                              #(contains? #{:started :dirty} %))
    (do
      (clear-transaction-items! transaction)
      (send *reaper* dissoc (:id transaction)))
    (warn "Tried to reap transaction in the wrong state, not doing anything")))


(defn schedule-transaction-reaper!
  ([transaction]
   (schedule-transaction-reaper! transaction *reaper*))
  ([transaction reaper]
   (let [timeout-in-secs (:timeout transaction 600)
         timeout-in-ms (* 1000 timeout-in-secs)
         scheduled-task-fn (at-at/after timeout-in-ms
                                        #(reap-transaction! transaction)
                                        *reaper-task-pool*)]
     (send reaper assoc (:id transaction)
           scheduled-task-fn))))

(defn cancel-transaction-reaper! [transaction]
  (if-let [scheduled-task-fn (get @*reaper* (:id transaction))]
    (at-at/stop scheduled-task-fn)
    (send *reaper* dissoc (:id transaction))))

(defn clear-transaction-items! [transaction]
  (jdbc/with-db-transaction [c config/*datasource*
                             {:isolation :read-committed}]
    (let [current-state (-> (select-transaction-stub c transaction)
                            (stub-row->Transaction)
                            (:state))]

      (assert (= current-state :cancelled)
              (str "Tried to clear a transaction that hasn't been cancelled: "
                   transaction))

      (clear-transaction-documents! c transaction)
      (clear-transaction-state-updates! c transaction))))


(defn gc-unreferenced-documents []
  (debug "Document GC start")
  (jdbc/with-db-transaction [c config/*datasource*]
    (spy :debug (clear-unreferenced-documents! c))))

(defn start-gc []
  (at-at/every config/store-gc-period
               gc-unreferenced-documents
               *reaper-task-pool*))
