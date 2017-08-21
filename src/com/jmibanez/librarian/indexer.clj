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
                                        alts! tap untap
                                        sliding-buffer]]
            [com.jmibanez.librarian
             [config :as config]
             [store :as store]])
  (:import [com.jmibanez.librarian.store Document]))


(timbre/refer-timbre)
(hugsql/def-db-fns "sql/indexer.sql")

(declare start-indexer!
         stop-indexer!)

(defstate indexer-chan
  :start (start-indexer!)
  :stop  (stop-indexer! indexer-chan))

(defstate indexer
  :start (agent [])
  :stop  nil)

(declare create-document-index
         run-indexer)

(defn add-document-to-index [index-queue doc]
  (if (= (count index-queue)
         config/indexer-batch-size)
    ;; Flush work queue
    (send-off indexer
              run-indexer))

  (spy :debug (conj index-queue doc)))


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

(defn run-indexer [index-queue]
  (info "Indexer has awoken...")
  (if (<= (count index-queue)
          config/indexer-batch-size)
    (spy :debug (create-index-for-documents index-queue))

    ;; Split into batches
    (tufte/p
     ::index-batch
     (doseq [batch (partition config/indexer-batch-size
                              index-queue)]
       (info "Indexing batch of " (count batch))
       (spy :debug (create-index-for-documents batch)))))
  [])


(defstate indexer-alive
  :start true
  :stop  false)

(defn dispatch-store-event [ev]
  (let [{:keys [event context payload]} ev]
    (debug "Event!" ev)
    (case event
      :document-write
      (do
        (send-off indexer add-document-to-index payload)
        true)

      :transaction-start
      true

      :transaction-commit
      (do
        (send-off indexer run-indexer)
        true)

      :transaction-cancel
      ;; FIXME: Remove index entries
      true


      nil
      false)))

(defn start-indexer! []
  (let [store-events (chan)]
    (tap store/*events* store-events)
    (go-loop [ev (<! store-events)]
      (if (dispatch-store-event ev)
        (recur (<! store-events))))

    (info "Indexer started.")
    store-events))

(defn stop-indexer! [indexer-chan]
  (untap store/*events* indexer-chan))

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

