(ns com.jmibanez.librarian.store
  (:require [taoensso.timbre :as timbre]
            [taoensso.tufte :as tufte]
            [mount.core :refer [defstate]]
            [clj-uuid :as uuid]
            [clojure.core.cache :as cache]
            [clojure.core.async :refer [chan close! go thread
                                        >!! >! <!! <!
                                        sliding-buffer]]
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
                              :cancelled))

(s/defrecord Transaction [id             :- Id
                          context        :- Context
                          timeout        :- s/Int
                          state          :- TransactionState
                          last-operation :- s/Inst])

(defstate ^:dynamic *reaper-task-pool*
  :start (at-at/mk-pool))

(def reaper (agent {}))

(def store-state (ref {}))

(hugsql/def-db-fns "sql/core.sql")

(declare doc-row->Document)
(declare stub-row->Transaction)

(def default-transaction-timeout 5000)


(declare schedule-transaction-reaper)
(declare create-db-transaction-row!)
(s/defn start-transaction! :- Transaction
  ([context-id :- Context]
   (start-transaction! context-id default-transaction-timeout))

  ([context-id :- Context
    timeout    :- s/Int]
   (let [transaction-id (uuid/v4)
         transaction    (create-db-transaction-row! context-id
                                                    transaction-id
                                                    timeout)]
     ;; Update transaction state
     (dosync
      (alter store-state assoc transaction-id {:transaction transaction
                                               :context context-id
                                               :documents   (ref [])}))
     transaction)))

(declare cas-transaction-state!)
(s/defn commit-transaction! :- Transaction
  [transaction :- Transaction]
  (tufte/p
   ::commit-transaction!
   (dosync
    (let [transaction-id  (:id transaction)
          new-transaction (assoc transaction :state :committed)
          tx-docs         (get-in @store-state [transaction-id :documents])]
      (alter store-state assoc-in [transaction-id :transaction] new-transaction)
      (ref-set tx-docs [])
      new-transaction))))

(declare cancel-transaction-reaper!)
(declare reap-transaction!)
(s/defn cancel-transaction! :- Transaction
  [transaction :- Transaction]
  (dosync
   (cancel-transaction-reaper! transaction)
   (let [transaction-id  (:id transaction)
         new-transaction (assoc transaction :state :cancelled)
         tx-docs         (get-in @store-state [transaction-id :documents])]
     (alter store-state assoc-in [transaction-id :transaction] new-transaction)
     (ref-set tx-docs [])
     new-transaction)))


(defmacro with-transaction [[transaction-sym context-id] & body]
  `(let [~transaction-sym (start-transaction! ~context-id)]
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
   (let [document
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

               document)

             ;; FIXME: Raise exception: Invalid transaction state
             nil))]
     (dosync
      (let [transaction-id (:id transaction)
            tx-docs        (get-in @store-state [transaction-id :documents])]
        (alter tx-docs conj document)))

     document)))

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
      (warn "Unknown document or document could not be retrieved:" id))))

(s/defn get-document-by-name :- (s/maybe Document)
  [context :- Context
   name    :- s/Str]

  (jdbc/with-db-transaction [c config/*datasource*
                             {:read-only? true}]
    (if-let [doc-row
             (select-recent-document-by-name c {:context context
                                                :name name})]
      (doc-row->Document doc-row)
      (warn "Unknown document or document could not be retrieved (name):" name))))

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
  (strict-map->Document
   (update (transform-keys ->kebab-case doc-row)
           :state keyword)))

(defn stub-row->Transaction
  [stub-row]
  (strict-map->Transaction
   (update (transform-keys ->kebab-case stub-row)
           :state keyword)))

(defn value-to-json-pgobject [value]
  (doto (PGobject.)
    (.setType "jsonb")
      (.setValue (json/generate-string value))))


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
        "json" (json/parse-string value true)
        "jsonb" (json/parse-string value true)
        :else value))))


(add-encoder java.util.UUID encode-str)

(defn create-db-transaction-row! [context-id transaction-id timeout]
  (jdbc/with-db-transaction [c config/*datasource*]
    (stub-row->Transaction
     (insert-transaction-row! c {:id transaction-id
                                 :context context-id
                                 :timeout timeout
                                 :state   :started}))))

(defn- document-writer [_ document-list-ref
                        prev-doc-list new-doc-list]
  (let [[_ new-documents _] (data/diff prev-doc-list
                                       new-doc-list)]
    (debug "Should write new documents here:"
           (vec (filter #(not (nil? %)) new-documents)))))

(defn- transaction-watcher [_ store-state-ref
                            prev-store-state new-store-state]
  (let [[_ transactions _] (data/diff prev-store-state
                                      new-store-state)]
    (doseq [[tx-id tx-state-diff] transactions]
      (if-not (nil? tx-state-diff)
        (let [{transaction :transaction
               context-id  :context
               tx-docs     :documents} (get new-store-state tx-id)]
          (condp = (:state transaction)
            :committed
            (jdbc/with-db-transaction [c config/*datasource*]
              (if (cas-transaction-state! (spy :debug transaction) :committed
                                          #(contains? #{:started :dirty} %))
                (commit-transaction-details! c {:transaction-id tx-id}))
              (remove-watch tx-docs ::doc-writer)
              transaction)

            :cancelled
            (do
              (reap-transaction! transaction)
              (remove-watch tx-docs ::doc-writer))

            :started
            (add-watch tx-docs ::doc-writer document-writer)

            nil))))))
(add-watch store-state ::transaction-watcher transaction-watcher)


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
(declare clear-transaction-items!)
(defn reap-transaction! [transaction]
  (info "Reap: " transaction)
  (if (cas-transaction-state! transaction :cancelled
                              #(contains? #{:started :dirty} %))
    (do
      (clear-transaction-items! transaction)
      (send reaper dissoc (:id transaction)))
    (warn "Tried to reap transaction in the wrong state, not doing anything")))


(defn schedule-transaction-reaper! [transaction]
  (let [timeout-in-secs (:timeout transaction 600)
        timeout-in-ms (* 1000 timeout-in-secs)
        scheduled-task-fn (at-at/after timeout-in-ms
                                       #(reap-transaction! transaction)
                                       *reaper-task-pool*)]
    (send reaper assoc (:id transaction)
          scheduled-task-fn)))

(defn cancel-transaction-reaper! [transaction]
  (if-let [scheduled-task-fn (get @reaper (:id transaction))]
    (at-at/stop scheduled-task-fn)
    (send reaper dissoc (:id transaction))))

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

