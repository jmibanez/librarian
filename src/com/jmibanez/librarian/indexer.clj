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

(defstate indexer-queue
  :start (ref clojure.lang.PersistentQueue/EMPTY)
  :stop  nil)
(defstate indexer
  :start (agent {})
  :stop  nil)

(declare create-document-index
         run-indexer)

(defn add-document-to-index [indexer-queue doc]
  (if (= (count indexer-queue)
         config/indexer-batch-size)
    ;; Flush work queue
    (send-off indexer
              run-indexer))

  (conj indexer-queue doc))


(defn create-index-for-documents [doc-set]
  (tufte/p
   ::create-index-for-documents
   (let [idx-list (apply concat
                         (spy :debug (map create-document-index
                                          doc-set)))]

     (jdbc/with-db-transaction [c config/*datasource*]
       (doseq [idx-row idx-list]
         (debug "idx=>" idx-row)
         (ensure-paths! c idx-row)
         (ensure-values! c idx-row)
         (insert-indexes! c idx-row))))))


(defn take-from-queue! []
  (dosync
   (let [head (peek @indexer-queue)
         tail (alter indexer-queue pop)]
     head)))

(defn run-indexer [indexer]
  (tufte/p
   ::indexer
   (info "Indexer has awoken...")
   (loop [doc (take-from-queue!)]

     (if-not (nil? doc)
       (jdbc/with-db-transaction [c config/*datasource*]
         (doseq [idx-row (create-document-index doc)]
           (tufte/p
            ::write-index-row
            (ensure-paths! c idx-row)
            (ensure-values! c idx-row)
            (insert-indexes! c idx-row)))))

     (if-not (nil? (peek @indexer-queue))
       (recur (take-from-queue!)))))

  {})


(defstate indexer-alive
  :start true
  :stop  false)

(defn dispatch-store-event [ev]
  (if-not (nil? ev)
    (let [{:keys [event context payload]} ev]
      (debug "Event!" ev)
      (condp = event
        :document-write     (dosync
                             (alter indexer-queue
                                    add-document-to-index payload)
                             true)
        :transaction-commit (do
                              (send-off indexer run-indexer)
                              true)
        true))))

(defn start-indexer! []
  (let [store-events (chan)]
    (tap store/*events* store-events)
    (go-loop [ev (<! store-events)]
      (if (dispatch-store-event ev)
        (recur (<! store-events))))

    (at-at/every config/indexer-period
                 (fn []
                   (send-off indexer run-indexer))
                 *indexer-pool*
                 :initial-delay config/indexer-period)
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

