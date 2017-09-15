(ns com.jmibanez.librarian.query-test
  (:require [expectations :refer :all]
            [clj-uuid :as uuid]
            [com.jmibanez.librarian
             [indexer :as idx]
             [query :as sut]
             [seeds :as seeds]
             [store :as store]]))


(def test-doc-type #uuid "549cfe8b-d950-4030-9f7c-ba8aafc2fac7")
(def test-doc-type-schema {:defintion {:id   "::integer"
                                       :name "::string"
                                       :members [{:name "::string"
                                                  :age  "::integer"}]}})
(def test-doc-type-doc (store/map->Document
                        {:id         test-doc-type
                         :name       "test-doc-type"
                         :type       store/schema-type
                         :context    seeds/test-context
                         :state      :created
                         :document   test-doc-type-schema}))

(def query-docset
  (map store/map->Document
       [{:id         (uuid/v4)
         :name       "Test Document 1"
         :type       test-doc-type
         :context    seeds/test-context
         :state      :created
         :document   {:id       1
                      :name     "Some Document"
                      :members  [{:name "Foo"
                                  :age  42}
                                 {:name "Test User"
                                  :age  24}]}}
        {:id         (uuid/v4)
         :name       "Test Document 2"
         :type       test-doc-type
         :context    seeds/test-context
         :state      :created
         :document   {:id       2
                      :name     "Another Document"
                      :members  [{:name "Test User"
                                  :age  24}]}}
        {:id         (uuid/v4)
         :name       "Test Document 3"
         :type       test-doc-type
         :context    seeds/test-context
         :state      :created
         :document   {:id       3
                      :name     "Document Three"
                      :members  [{:name "Foo"
                                  :age  20}
                                 {:name "Bar"
                                  :age  25}]}}]))


(defmacro with-document-set
  [[context docset] & body]
  `(let [tx# (store/start-transaction! ~context)
         test-doc-type-doc# (store/map->Document
                             {:id         test-doc-type
                              :name       "test-doc-type"
                              :type       store/schema-type
                              :context    ~context
                              :state      :created
                              :document   test-doc-type-schema})]
     (try
       (store/write-document! tx# test-doc-type-doc#)
       (idx/create-index-for-documents!
        (for [doc# ~docset]
          (store/write-document! tx# doc#)))
       (finally
         (store/commit-transaction! tx#)))
     (do
       ~@body)))


(defmacro with-document-set-in-transaction
  [[context docset] & body]
  `(let [tx# (store/start-transaction! ~context)]
     (try
       (do
         (store/write-document! tx# test-doc-type-doc)
         (idx/create-index-for-documents!
          (for [doc# ~docset]
            (store/write-document! tx# doc#)))
         ~@body)
       (finally
         (store/commit-transaction! tx#)))))





(expect
 {:total  2}
 (in (with-document-set-in-transaction
       [seeds/test-context
        query-docset]
       (let [q (sut/parse-query-document
                {:query {:name   "test-query"
                         :type   test-doc-type
                         :sort   nil
                         :rule   {:any_of [{:match "$..age"
                                            :with  42}
                                           {:match "$..name"
                                            :with  "Test User"}]}}})]
         (store/exec q {:context   seeds/test-context
                        :page      0
                        :page-size 100})))))

(expect
 {:total  1}
 (in (with-document-set-in-transaction
       [seeds/test-context
        query-docset]
       (let [q (sut/parse-query-document
                {:query {:name   "test-query"
                         :type   test-doc-type
                         :sort   nil
                         :rule   {:all_of [{:match "$..age"
                                            :with  42}
                                           {:match "$..name"
                                            :with  "Test User"}]}}})]
         (store/exec q {:context   seeds/test-context
                        :page      0
                        :page-size 100})))))


(expect
 {:total  2}
 (in (with-document-set [seeds/test-context
                         query-docset]
       (let [q (sut/parse-query-document
                {:query {:name   "test-query"
                         :type   test-doc-type
                         :sort   nil
                         :rule   {:any_of [{:match "$..age"
                                            :with  42}
                                           {:match "$..name"
                                            :with  "Test User"}]}}})]
         (store/exec q {:context   seeds/test-context
                        :page      0
                        :page-size 100})))))

(expect
 {:total  1}
 (in (with-document-set [seeds/test-context
                         query-docset]
       (let [q (sut/parse-query-document
                {:query {:name   "test-query"
                         :type   test-doc-type
                         :sort   nil
                         :rule   {:all_of [{:match "$..age"
                                            :with  42}
                                           {:match "$..name"
                                            :with  "Test User"}]}}})]
         (store/exec q {:context   seeds/test-context
                        :page      0
                        :page-size 100})))))

