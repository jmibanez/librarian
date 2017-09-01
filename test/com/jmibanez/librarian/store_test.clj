(ns com.jmibanez.librarian.store-test
  (:require [clj-uuid :as uuid]
            [expectations :refer :all]
            [com.jmibanez.librarian.store :as sut]
            [com.jmibanez.librarian.seeds :as seeds]))

;; Document fetches

;; Should be able to fetch root documents by ID
(expect {:id sut/root-type
         :type sut/root-type
         :name sut/root-type-name
         :state :committed
         :context nil
         :document {:name "Root Document" :system "librarian1"}}
        (in (sut/get-document-by-id nil sut/root-type)))
(expect "d7dab10e905c25d3f11f9349f29f63eb632c71486a99fd092db2a0e8b9bba341"
        (-> (sut/get-document-by-id nil sut/root-type)
            (sut/version)))

(expect {:id sut/schema-type
         :type sut/root-type
         :name sut/schema-type-name
         :state :committed
         :context nil
         :document {:definition "::any"}}
        (in (sut/get-document-by-id nil sut/schema-type)))
(expect "b23c5525b96367b9264546da6ebfcc3abba121d26621faea6e4e592e2e600400"
        (-> (sut/get-document-by-id nil sut/schema-type)
            (sut/version)))

;; Should be able to fetch root documents by name
(expect {:id sut/root-type
         :type sut/root-type
         :name sut/root-type-name
         :state :committed
         :context nil
         :document {:name "Root Document" :system "librarian1"}}
        (in (sut/get-document-by-name nil sut/root-type sut/root-type-name)))
(expect "d7dab10e905c25d3f11f9349f29f63eb632c71486a99fd092db2a0e8b9bba341"
        (-> (sut/get-document-by-name nil sut/root-type sut/root-type-name)
            (sut/version)))

(expect {:id sut/schema-type
         :type sut/root-type
         :name sut/schema-type-name
         :state :committed
         :context nil
         :document {:definition "::any"}}
        (in (sut/get-document-by-name nil sut/root-type sut/schema-type-name)))
(expect "b23c5525b96367b9264546da6ebfcc3abba121d26621faea6e4e592e2e600400"
        (-> (sut/get-document-by-name nil sut/root-type sut/schema-type-name)
            (sut/version)))

;; Should yield nil document for v0/null ID
(expect nil (sut/get-document-by-id nil uuid/+null+))

;; Should yield exception for nil name or ID
(expect clojure.lang.ExceptionInfo (sut/get-document-by-id nil nil))
(expect clojure.lang.ExceptionInfo (sut/get-document-by-name nil nil nil))
(expect clojure.lang.ExceptionInfo (sut/get-document-by-name nil sut/root-type nil))


;; Should fetch seeded document
(expect {:id seeds/test-doc-id}
        (in (sut/get-document-by-id seeds/test-context
                                    seeds/test-doc-id)))
(expect {:context seeds/test-context}
        (in (sut/get-document-by-id seeds/test-context
                                    seeds/test-doc-id)))
(expect {:name seeds/test-doc-name}
        (in (sut/get-document-by-id seeds/test-context
                                    seeds/test-doc-id)))
(expect {:type sut/schema-type}
        (in (sut/get-document-by-id seeds/test-context
                                    seeds/test-doc-id)))
(expect seeds/test-doc-root-version
        (-> (sut/get-document-by-id seeds/test-context
                                    seeds/test-doc-id)
            (sut/version)))
(expect {:document {:name "::string"}}
        (in (sut/get-document-by-id seeds/test-context
                                    seeds/test-doc-id)))


;; Should fetch different docs by the same name, different type
(expect {:document {:name "::string"}}
        (in (sut/get-document-by-name seeds/test-context
                                      seeds/test-type
                                      seeds/test-doc-name)))
(expect {:document {:name "other"}}
        (in (sut/get-document-by-name seeds/test-context
                                      seeds/test-other-type
                                      seeds/test-doc-name)))

;; Document writes via storage transaction
(defmacro with-storage-transaction
  [[transaction-sym context-id] & body]
  `(let [~transaction-sym (sut/start-transaction! ~context-id)]
     ~@body))


;; Should return newly written document with next version
(expect "31f6d5f6a551d0449d9038388ae635227ac3385792d9633820fa281d69577eda"
        (with-storage-transaction
          [tx seeds/test-context]
          (let [doc-record (sut/get-document-by-id seeds/test-context
                                                   seeds/test-doc-id)
                new-doc (assoc-in doc-record [:document :age]
                                  "::integer")]
            (-> (sut/write-document! tx new-doc)
                (sut/version)))))

;; Existing document should have old version if transaction not
;; committed
(expect seeds/test-doc-root-version
        (with-storage-transaction
          [tx seeds/test-context]
          (let [doc-record (sut/get-document-by-id seeds/test-context
                                                   seeds/test-doc-id)
                new-doc (assoc-in doc-record [:document :age]
                                  "::integer")]
            (sut/write-document! tx new-doc)
            (-> (sut/get-document-by-id seeds/test-context
                                        seeds/test-doc-id)
                (sut/version)))))

;; Newly written document should have new field
(expect {:age "::integer"}
        (in (:document (with-storage-transaction
                         [tx seeds/test-context]
                         (let [doc-record (sut/get-document-by-id seeds/test-context
                                                                  seeds/test-doc-id)
                               new-doc (assoc-in doc-record [:document :age]
                                                 "::integer")]
                           (sut/write-document! tx new-doc))))))

;; Committing the storage transaction should persist document changes,
;; including new version
(expect "31f6d5f6a551d0449d9038388ae635227ac3385792d9633820fa281d69577eda"
        (with-storage-transaction
          [tx seeds/test-context]
          (let [doc-record (sut/get-document-by-id seeds/test-context
                                                   seeds/test-doc-id)
                new-doc (assoc-in doc-record [:document :age]
                                  "::integer")]
            (sut/write-document! tx new-doc)
            (sut/commit-transaction! tx)
            (-> (sut/get-document-by-id seeds/test-context
                                        seeds/test-doc-id)
                (sut/version)))))

;; Should return new state for document
(expect {:state :opened}
        (in (with-storage-transaction
              [tx seeds/test-context]
              (let [doc-record (-> (sut/get-document-by-id seeds/test-context
                                                           seeds/test-doc-id)
                                   (assoc :state :opened))]
                (sut/write-document! tx doc-record)))))

;; Document state should be persisted
(expect {:state :opened}
        (in (with-storage-transaction
              [tx seeds/test-context]
              (let [doc-record (sut/get-document-by-id seeds/test-context
                                                       seeds/test-doc-id)
                    new-doc (-> doc-record
                                (assoc-in [:document :age]
                                          "::integer")
                                (assoc :state :opened))
                    updated-doc-record (sut/write-document! tx new-doc)]

                (sut/commit-transaction! tx)
                (sut/get-document-by-id seeds/test-context
                                        seeds/test-doc-id)))))
(expect "01d877fa590fb2c3fccbb3ffcb87f7b19935d3d8a4d41d4067950f184402250b"
        (with-storage-transaction
          [tx seeds/test-context]
          (let [doc-record (sut/get-document-by-id seeds/test-context
                                                   seeds/test-doc-id)
                new-doc (-> doc-record
                            (assoc-in [:document :age]
                                      "::integer")
                            (assoc :state :opened))
                updated-doc-record (sut/write-document! tx new-doc)]

            (sut/commit-transaction! tx)
            (-> (sut/get-document-by-id seeds/test-context
                                        seeds/test-doc-id)
                (sut/version)))))

;; Should be able to fetch older document version
(expect {:name "::string"}
        (with-storage-transaction
          [tx seeds/test-context]
          (let [doc-record (sut/get-document-by-id seeds/test-context
                                                   seeds/test-doc-id)
                new-doc (-> doc-record
                            (assoc-in [:document :age]
                                      "::integer")
                            (assoc :state :opened))
                updated-doc-record (sut/write-document! tx new-doc)]

            (sut/commit-transaction! tx)
            (sut/get-document-by-id seeds/test-context
                                    seeds/test-doc-id)
            (:document (sut/get-document-version seeds/test-context
                                                 seeds/test-doc-id
                                                 seeds/test-doc-root-version)))))
(expect {:name "::string" :age "::integer"}
        (with-storage-transaction
          [tx seeds/test-context]
          (let [doc-record (sut/get-document-by-id seeds/test-context
                                                   seeds/test-doc-id)
                new-doc (-> doc-record
                            (assoc-in [:document :age]
                                      "::integer")
                            (assoc :state :opened))
                updated-doc-record (sut/write-document! tx new-doc)]

            (sut/commit-transaction! tx)
            (sut/get-document-by-id seeds/test-context
                                    seeds/test-doc-id)
            (:document (sut/get-document-version seeds/test-context
                                                 seeds/test-doc-id
                                                 (sut/version updated-doc-record))))))

;; Fetching non-existent version of a document should return nil
(expect nil
        (with-storage-transaction
          [tx seeds/test-context]
          (let [doc-record (sut/get-document-by-id seeds/test-context
                                                   seeds/test-doc-id)
                new-doc (-> doc-record
                            (assoc-in [:document :age]
                                      "::integer")
                            (assoc :state :opened))
                updated-doc-record (sut/write-document! tx new-doc)]

            (sut/commit-transaction! tx)
            (sut/get-document-by-id seeds/test-context
                                    seeds/test-doc-id)
            (sut/get-document-version seeds/test-context
                                      seeds/test-doc-id
                                      "XXX"))))


;; Should be ok on first of two concurrent transactions
(expect {:state :committed}
        (in (let [tx1 (sut/start-transaction! seeds/test-context)
                  tx2 (sut/start-transaction! seeds/test-context)]
              (let [doc-record (sut/get-document-by-id seeds/test-context
                                                       seeds/test-doc-id)
                    new-doc1 (assoc-in doc-record [:document :age]
                                       "::integer")
                    new-doc2 (assoc-in doc-record [:document :age]
                                       "::string")]
                (sut/write-document! tx1 new-doc1)
                (sut/write-document! tx2 new-doc2)

                (let [new-tx1
                      (sut/commit-transaction! tx1)]
                  (sut/commit-transaction! tx2)
                  new-tx1)))))

;; Should trigger transaction conflict on second of two concurrent
;; transactions
(expect {:state :conflict}
        (in (let [tx1 (sut/start-transaction! seeds/test-context)
                  tx2 (sut/start-transaction! seeds/test-context)]
              (let [doc-record (sut/get-document-by-id seeds/test-context
                                                       seeds/test-doc-id)
                    new-doc1 (assoc-in doc-record [:document :age]
                                       "::integer")
                    new-doc2 (assoc-in doc-record [:document :age]
                                       "::string")]
                (sut/write-document! tx1 new-doc1)
                (sut/write-document! tx2 new-doc2)

                (sut/commit-transaction! tx1)
                (sut/commit-transaction! tx2)))))

;; Should be able to cancel conflicted transaction
(expect {:state :cancelled}
        (in (let [tx1 (sut/start-transaction! seeds/test-context)
                  tx2 (sut/start-transaction! seeds/test-context)]
              (let [doc-record (sut/get-document-by-id seeds/test-context
                                                       seeds/test-doc-id)
                    new-doc1 (assoc-in doc-record [:document :age]
                                       "::integer")
                    new-doc2 (assoc-in doc-record [:document :age]
                                       "::string")]
                (sut/write-document! tx1 new-doc1)
                (sut/write-document! tx2 new-doc2)

                (sut/commit-transaction! tx1)
                (sut/commit-transaction! tx2)

                (sut/cancel-transaction! tx2)))))


;; Multiple writes to same document in a single transaction should be
;; fine; coalesced to latest version
(expect {:state :committed}
        (in (with-storage-transaction
              [tx seeds/test-context]
              (let [doc-record (sut/get-document-by-id seeds/test-context
                                                       seeds/test-doc-id)
                    new-doc1 (sut/write-document! tx
                                                  (assoc-in doc-record [:document :age]
                                                            "::integer"))
                    new-doc2 (sut/write-document! tx
                                                  (assoc-in new-doc1 [:document :height]
                                                            "::integer"))]
                (sut/commit-transaction! tx)))))

(expect "26d74f660af7c4c2c8e79e2c406980e8c407986def55399978a4c95d15f3e094"
        (with-storage-transaction
          [tx seeds/test-context]
          (let [doc-record (sut/get-document-by-id seeds/test-context
                                                   seeds/test-doc-id)
                new-doc1 (sut/write-document! tx
                                              (assoc-in doc-record [:document :age]
                                                        "::integer"))
                new-doc2 (sut/write-document! tx
                                              (assoc-in new-doc1 [:document :height]
                                                        "::integer"))]
            (sut/commit-transaction! tx)
            (-> (sut/get-document-by-id seeds/test-context
                                        seeds/test-doc-id)
                (sut/version)))))

(expect {:age    "::integer"
         :height "::integer"}
        (in (with-storage-transaction
              [tx seeds/test-context]
              (let [doc-record (sut/get-document-by-id seeds/test-context
                                                       seeds/test-doc-id)
                    new-doc1 (sut/write-document! tx
                                                  (assoc-in doc-record [:document :age]
                                                            "::integer"))
                    new-doc2 (sut/write-document! tx
                                                  (assoc-in new-doc1 [:document :height]
                                                            "::integer"))]
                (sut/commit-transaction! tx)
                (-> (sut/get-document-by-id seeds/test-context
                                            seeds/test-doc-id)
                    :document)))))



;; Check scope of key transforms
(expect {:name_is_snake_case 1234}
        (in (let [new-doc-id (uuid/v4)]
              (with-storage-transaction
                [tx seeds/test-context]
                (->> {:id             new-doc-id
                      :name           "test-new-doc"
                      :type           sut/root-type
                      :context        seeds/test-context
                      :state          :new
                      :document       {"name_is_snake_case" 1234}}
                     (sut/map->Document)
                     (sut/write-document! tx))
                (sut/commit-transaction! tx))

              (-> (sut/get-document-by-id seeds/test-context new-doc-id)
                  :document))))

;; Preserve dict keys that can't be coerced to keywords
(expect {"name with spaces" 1234}
        (in (let [new-doc-id (uuid/v4)]
              (with-storage-transaction
                [tx seeds/test-context]
                (->> {:id             new-doc-id
                      :name           "test-new-doc"
                      :type           sut/root-type
                      :context        seeds/test-context
                      :state          :new
                      :document       {"name with spaces" 1234}}
                     (sut/map->Document)
                     (sut/write-document! tx))
                (sut/commit-transaction! tx))

              (-> (sut/get-document-by-id seeds/test-context new-doc-id)
                  :document))))

