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
         :version "a20190aeb466d75a2cb2b44c51a37a103e0303dc0d24d4c5a9e573062f5d2d66"
         :document {:name "Root Document" :system "librarian1"}}
        (in (sut/get-document-by-id nil sut/root-type)))

(expect {:id sut/schema-type
         :type sut/root-type
         :name sut/schema-type-name
         :state :committed
         :context nil
         :version "477ef70b9f4f23aa113dd01ce7301bdb1a6575a9ea180c16e1090a74be2604a0"
         :document {:definition "::any"}}
        (in (sut/get-document-by-id nil sut/schema-type)))

;; Should be able to fetch root documents by name
(expect {:id sut/root-type
         :type sut/root-type
         :name sut/root-type-name
         :state :committed
         :context nil
         :version "a20190aeb466d75a2cb2b44c51a37a103e0303dc0d24d4c5a9e573062f5d2d66"
         :document {:name "Root Document" :system "librarian1"}}
        (in (sut/get-document-by-name nil sut/root-type sut/root-type-name)))

(expect {:id sut/schema-type
         :type sut/root-type
         :name sut/schema-type-name
         :state :committed
         :context nil
         :version "477ef70b9f4f23aa113dd01ce7301bdb1a6575a9ea180c16e1090a74be2604a0"
         :document {:definition "::any"}}
        (in (sut/get-document-by-name nil sut/root-type sut/schema-type-name)))

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
(expect {:version seeds/test-doc-root-version}
        (in (sut/get-document-by-id seeds/test-context
                                    seeds/test-doc-id)))
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
(expect {:version "d4e3dcd997ecef2b734eead7bf8a55a48c5302af7e90a41c3610bd34bbe0526e"}
        (in (with-storage-transaction
              [tx seeds/test-context]
              (let [doc-record (sut/get-document-by-id seeds/test-context
                                                       seeds/test-doc-id)
                    new-doc (assoc-in doc-record [:document :age]
                                      "::integer")]
                (sut/write-document! tx new-doc)))))

;; Existing document should have old version if transaction not
;; committed
(expect {:version seeds/test-doc-root-version}
        (in (with-storage-transaction
              [tx seeds/test-context]
              (let [doc-record (sut/get-document-by-id seeds/test-context
                                                       seeds/test-doc-id)
                    new-doc (assoc-in doc-record [:document :age]
                                      "::integer")]
                (sut/write-document! tx new-doc)
                (sut/get-document-by-id seeds/test-context
                                        seeds/test-doc-id)))))

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
(expect {:version "d4e3dcd997ecef2b734eead7bf8a55a48c5302af7e90a41c3610bd34bbe0526e"}
        (in (with-storage-transaction
              [tx seeds/test-context]
              (let [doc-record (sut/get-document-by-id seeds/test-context
                                                       seeds/test-doc-id)
                    new-doc (assoc-in doc-record [:document :age]
                                      "::integer")]
                (sut/write-document! tx new-doc)
                (sut/commit-transaction! tx)
                (sut/get-document-by-id seeds/test-context
                                        seeds/test-doc-id)))))

;; Should return new state for document
(expect {:state :opened}
        (in (with-storage-transaction
              [tx seeds/test-context]
              (let [doc-record (-> (sut/get-document-by-id seeds/test-context
                                                           seeds/test-doc-id)
                                   (assoc :state :opened))]
                (sut/write-document! tx doc-record)))))

;; Document state should be persisted
(expect {:state :opened
         :version "ab873da32ed04d03c31f7cb7fb7d27d8ff0f178475daed57233c34ba4421e332"}
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
                                                 (:version updated-doc-record))))))

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

