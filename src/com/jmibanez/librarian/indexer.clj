(ns com.jmibanez.librarian.indexer
  (:require [clojure.string :as str]
            [clojure.walk :as walk]
            [taoensso.timbre :as timbre]
            [taoensso.tufte :as tufte]
            [schema.core :as s]
            [mount.core :refer [defstate]]

            [clojure.java.jdbc :as jdbc]
            [clojure.data :as data]
            [cheshire.core :as json]
            [hugsql.core :as hugsql]

            [clojure.core.async :refer [chan close! go
                                        go-loop thread
                                        >!! >! <!! <!! <!
                                        alts!
                                        sliding-buffer]]
            [com.jmibanez.librarian
             [config :as config]
             [store :as store]])
  (:import [com.jmibanez.librarian.store Document]))


(timbre/refer-timbre)
(hugsql/def-db-fns "sql/indexer.sql")

(declare start-indexer!
         stop-indexer!)
(defstate indexer
  :start (start-indexer!)
  :stop  (stop-indexer!))


(declare create-document-index
         run-indexer)

(defn add-document-to-index [state-map context transaction document]
  (let [tx-id     (:id transaction)
        state-key [context tx-id]
        new-state (if (nil? (get state-map state-key))
                    (assoc state-map state-key [document])
                    (update state-map state-key conj document))]


    (if (= (count (get state-map state-key))
           config/indexer-batch-size)
      ;; Flush work queue
      (send-off indexer
                run-indexer context transaction))

    new-state))


(defn create-index-for-documents [doc-set]
  (tufte/p
   ::create-index-for-documents
   (let [idx-list (apply concat
                         (spy :debug (map create-document-index
                                          doc-set)))]

     (jdbc/with-db-transaction [c config/*datasource*]
       (debug "idx-list->" (vec idx-list))
       (doseq [idx-row idx-list]
         (debug "idx=>" idx-row)
         (ensure-paths! c idx-row)
         (ensure-values! c idx-row)
         (insert-indexes! c idx-row))))))

(defn run-indexer [state-map context transaction]
  (let [tx-id     (:id transaction)
        state-key [context tx-id]]

    (if-let [doc-set (get state-map state-key)]
      (do
        (if (<= (count doc-set)
                config/indexer-batch-size)
          (spy :debug (create-index-for-documents doc-set))

          ;; Split into batches
          (tufte/p
           ::index-batch
           (doseq [batch (partition config/indexer-batch-size
                                    doc-set)]
             (info "Indexing batch of " (count batch))
             (spy :debug (create-index-for-documents batch)))))

        (dissoc state-map state-key))

      ;; Bail with warning if transaction not found in state
      (do
        (warn "Transaction" tx-id "not found in indexer state; ignoring")
        state-map))))


(defstate indexer-alive
  :start true
  :stop  false)

(defn add-index-job [[context transaction] doc-list-ref
                     prev-doc-list new-doc-list]
  (let [[old-docs new-docs all-docs] (data/diff prev-doc-list
                                                new-doc-list)]
    (doseq [document (filter #(not (nil? %)) new-docs)]
      (if-not (nil? document)
        (send-off indexer
                  add-document-to-index context transaction document)))))

(defn indexer-watcher [_ store-state-ref
                       prev-store-state new-store-state]
  (let [[_ transactions _] (data/diff prev-store-state
                                      new-store-state)]
    (doseq [[tx-id tx-state-diff] transactions]
      (if-not (nil? tx-state-diff)
        (let [{transaction :transaction
               context     :context
               tx-docs     :documents} (get new-store-state tx-id)]
          (condp = (:state transaction)
            :committed
            (do
              (send-off indexer
                        run-indexer context transaction)
              (remove-watch tx-docs [context transaction]))

            :cancelled
            (remove-watch tx-docs [context transaction])

            :started
            (do
              (send-off indexer assoc [context tx-id] [])
              (add-watch tx-docs [context transaction] add-index-job))

            nil))))))

(defn start-indexer! []
  (add-watch store/store-state ::indexer indexer-watcher)
  (agent {}))

(defn stop-indexer! []
  (remove-watch store/store-state ::indexer))

;; (def index-type #uuid "1092c705-e1f8-4260-b6db-50e46d136ce5")
;; (def index-type-name "index")

(s/defschema Index {:document_id store/Id
                    :version     s/Str
                    :path        s/Str
                    :value       s/Any})
(s/defschema IndexList [Index])

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
                (sort #(compare (first %1)
                                (first %2)))))

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

