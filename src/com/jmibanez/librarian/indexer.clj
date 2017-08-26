(ns com.jmibanez.librarian.indexer
  (:require [clojure.string :as str]
            [clojure.walk :as walk]
            [taoensso.timbre :as timbre]
            [taoensso.tufte :as tufte]
            [schema.core :as s]
            [mount.core :refer [defstate]]

            [overtone.at-at :as at-at]

            [clojure.java.jdbc :as jdbc]
            [clojure.data :as data]
            [cheshire.core :as json]
            [hugsql.core :as hugsql]

            [clojure.core.async :refer [chan close! go
                                        go-loop thread
                                        >!! >! <!! <!! <!
                                        alts! tap untap
                                        sliding-buffer]]
            [com.jmibanez.librarian
             [config :as config]
             [core-schema :as c]
             [store :as store]])
  (:import [com.jmibanez.librarian.store Document]))


(timbre/refer-timbre)
(hugsql/def-db-fns "sql/indexer.sql")

(declare start-indexer!
         stop-indexer!)

(defstate ^:dynamic *indexer-pool*
  :start (at-at/mk-pool)
  :stop  (at-at/stop-and-reset-pool!
          *indexer-pool* :strategy :stop))


(defstate indexer-chan
  :start (start-indexer!)
  :stop  (stop-indexer! indexer-chan))

(declare scan-for-reindexing)
(defstate ^:dynamic *indexer-queue*
  :start (scan-for-reindexing)
  :stop  (dosync
          (ref-set *indexer-queue*
                   clojure.lang.PersistentQueue/EMPTY)))

(declare reload-state-from-db)
(defstate ^:dynamic *indexer-state*
  :start (reload-state-from-db)
  :stop  nil)

(declare create-document-index
         run-indexer)


(defn write-index-row! [c idx-row]
  (tufte/p
   ::write-index-row
   (ensure-paths! c idx-row)
   (ensure-values! c idx-row)
   (insert-indexes! c idx-row)))

(defn create-index-for-document [doc]
  (if-not (nil? doc)
    (tufte/p
     ::create-index-for-document
     (jdbc/with-db-transaction [c config/*datasource*]
       (doseq [idx-row (create-document-index doc)]
         (write-index-row! c idx-row))))))

(defn create-index-for-documents [doc-set]
  (tufte/p
   ::create-index-for-documents
   (jdbc/with-db-transaction [c config/*datasource*]
     (let [idx-rows (apply concat
                           (map create-document-index doc-set))]
       (doseq [idx-row idx-rows]
         (write-index-row! c idx-row))))))

(defn take-from-queue! []
  (dosync
   (let [head (peek @*indexer-queue*)
         tail (alter *indexer-queue* pop)]
     head)))

(defn run-indexer []
  (tufte/p
   ::indexer
   (debug "Indexer has awoken...")
   (loop [i 0]
     (if-not (nil? (peek @*indexer-queue*))
       (do
         (create-index-for-document (take-from-queue!))
         (recur (inc i)))

       (when (> i 0)
         (info "Indexed" i "documents"))))))


(defstate indexer-alive
  :start true
  :stop  false)


(defn queue-for-indexing [transaction-id doc]
  (dosync
   (alter *indexer-queue* conj doc)
   (alter *indexer-state*
          update transaction-id conj
          [(:id doc) (:version doc)])
   true))


(defn clear-indexer-state [transaction-id]
  (dosync
   (alter *indexer-state*
          dissoc transaction-id)))

(declare unindex-transaction-documents!)
(defn dispatch-store-event [ev]
  (if-not (nil? ev)
    (let [{:keys [event context payload transaction]} ev]
      (debug "Event!" ev)
      (condp = event
        :document-write     (queue-for-indexing transaction
                                                payload)
        :transaction-commit (clear-indexer-state (:id payload))
        :transaction-cancel (unindex-transaction-documents!
                             payload)
        true))))


(defn start-indexer! []
  (let [store-events (chan)]
    (tap store/*events* store-events)
    (go-loop [ev (<! store-events)]
      (if (dispatch-store-event ev)
        (recur (<! store-events))))

    (doseq [i (range config/indexer-threads)]
      (at-at/every config/indexer-period
                   run-indexer
                   *indexer-pool*
                   :desc (str "Indexer #" i)
                   :initial-delay (+ config/indexer-period
                                     (* i 500))))

    (info "Indexer started.")
    store-events))

(defn stop-indexer! [indexer-chan]
  (untap store/*events* indexer-chan))

;; (def index-type #uuid "1092c705-e1f8-4260-b6db-50e46d136ce5")
;; (def index-type-name "index")

(s/defschema Index {:document_id c/Id
                    :version     s/Str
                    :path        s/Str
                    :value       s/Any})
(s/defschema IndexList #{Index})

(declare flatten-path
         index-row)
(s/defn create-document-index :- IndexList
  [doc :- Document]

  (let [doc-id   (:id doc)
        version  (:version doc)
        document (:document doc)]
    (debug "document to index=>" document)
    (if-not (nil? document)
      (spy :debug
           (->> document
                (flatten-path [])
                (map (index-row doc-id version))
                (set)))

      (do
        (warn "Cannot create index for unknown or unretrievable document")
        []))))

(defn- to-indexed-seqs [coll]
  (if (map? coll)
    coll
    (map (fn [b c]
           [["[*]" b] c])
         (range)
         coll)))

(defn- flatten-path [path step]
  (if (coll? step)
    (->> step
         to-indexed-seqs
         (map (fn [[k v]] (flatten-path (conj path k) v)))
         (into {}))
    [path step]))

(defn path-key->json-path [key-path]
  (str "$" (str/join "" (for [key key-path]
                          (if (vector? key)
                            (first key)
                            (str "." (name key)))))))

(defn index-row [doc-id version]
  (fn [[k v]]
    {:document_id  doc-id
     :version      version
     :path         (path-key->json-path k)
     :value        (json/generate-string v)}))


(defn clear-documents-from-queue [q docs]
  (let [doc-set (set docs)
        filtered-q (remove #(contains? doc-set
                                       [(:id %) (:version %)])
                           q)]

    (apply conj clojure.lang.PersistentQueue/EMPTY filtered-q)))


(defn unindex-transaction-documents!
  [transaction]

  (jdbc/with-db-transaction [c config/*datasource*]
    (info "Cancelling on-going index of documents in" transaction)

    (let [transaction-id (:id transaction)
          documents-in-transaction (get @*indexer-state* transaction-id)]

      ;; First, grovel through the existing indexer queue to remove all
      ;; documents related to this transaction
      (dosync
       (let []
         (alter *indexer-queue*
                clear-documents-from-queue documents-in-transaction)
         (alter *indexer-state* dissoc transaction-id)))

      ;; Next, clear out the indexes for those documents
      (info "Docs to invalidate" (count documents-in-transaction))
      (doseq [[doc-id version] documents-in-transaction]
        (spy :debug (invalidate-index-for-document-and-version! c {:id doc-id
                                                                   :version version})))))

  true)

(defn scan-for-reindexing []
  (let [indexer-queue (ref clojure.lang.PersistentQueue/EMPTY)]

    (jdbc/with-db-transaction [c config/*datasource*]
      (let [unindexed (->> (select-unindexed-documents c)
                           (map store/doc-row->Document))]
        (info "Reindexing" (count unindexed) "documents")
        (dosync (alter indexer-queue
                       (fn [q rest]
                         (apply conj q rest))
                       unindexed))))
    indexer-queue))

(defn reload-state-from-db []
  (let [indexer-state (ref {})]

    (jdbc/with-db-transaction [c config/*datasource*]
      (let [open-documents (->> (select-documents-for-open-transactions c)
                                (map (juxt :transaction_id
                                           :document_id
                                           :version)))]
        (dosync
         (doseq [[transaction-id document-id version] open-documents]
           (alter indexer-state
                  update transaction-id conj
                  [document-id version])))))

    indexer-state))
