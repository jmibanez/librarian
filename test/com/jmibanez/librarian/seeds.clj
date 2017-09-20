(ns com.jmibanez.librarian.seeds
  (:require [clojure.java.jdbc :as jdbc]
            [com.jmibanez.librarian.config :as config]
            [com.jmibanez.librarian.store :as store]
            [com.jmibanez.librarian.store-init :refer [do-init]]))

(def test-type #uuid "143246e3-c0b7-59e3-ae17-29b99ff0d5ac")
(def test-other-type #uuid "13068b05-bc96-4ed6-9a5f-d49936da40da")

(def test-context #uuid "008dc16e-f7d9-45ce-a043-e482919561c1")
(def test-doc-id #uuid "34a79262-f324-4911-ac33-8bb62f020d09")
(def test-doc-name "Foo")
(def test-doc-root-version "d22b30f48fe55d0633be7761aadb7b5e631830644dc9efaa3b74fd55d943c34e")

(def test-type-id #uuid "60053ded-32e9-48ee-b6b1-d546b3c071b3")
(def test-type-name "TestDoc")

(def test-type-ref-id #uuid "1ececa2b-0bae-478f-80f4-a291deaf2186")
(def test-type-ref-name "TestDocWithRef")

(def test-doc-id-same-name #uuid "e649ce0d-801d-414c-bdc1-9605ff7090d1")

(def test-doc
  {:id        test-doc-id
   :name      test-doc-name
   :type      test-type
   :context   test-context
   :state     :posted
   :document  {:name "::string"}})

(def test-doc-same-name
  {:id        test-doc-id-same-name
   :name      test-doc-name
   :type      test-other-type
   :context   test-context
   :state     :posted
   :document  {:name "other"}})

(def test-type-doc
  {:id        test-type-id
   :name      test-type-name
   :type      store/schema-type
   :context   test-context
   :state     :posted
   :document  {:definition {:id   "::string"
                            :name "::string"
                            :inner [{:name  "::string"
                                     :value "::integer"}]}}})
(def test-type-ref-doc
  {:id        test-type-ref-id
   :name      test-type-ref-name
   :type      store/schema-type
   :context   test-context
   :state     :posted
   :document  {:definition {:id       "::string"
                            :name     "::string"
                            :referred "TestDoc"}}})


(def documents [test-doc test-doc-same-name test-type-doc test-type-ref-doc])

(defn seed-test-documents! []
  (let [tx (store/start-transaction! test-context)]
    (try
      (doseq [doc documents]
        (->> doc
             (store/map->Document)
             (store/write-document! tx)))
      (finally
        (store/commit-transaction! tx)))))

(defn populate-db []
  (do-init)
  (seed-test-documents!))

(defn fixture [test]
  (jdbc/with-db-transaction [c config/*datasource*]
    (jdbc/db-set-rollback-only! c)
    (with-redefs [config/*datasource* c]
      (populate-db)
      (test))))
