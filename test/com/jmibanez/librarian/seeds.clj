(ns com.jmibanez.librarian.seeds
  (:require [clojure.java.jdbc :as jdbc]
            [com.jmibanez.librarian.config :as config]
            [com.jmibanez.librarian.query :as q]
            [com.jmibanez.librarian.store :as store]
            [com.jmibanez.librarian.store-init :refer [do-init]]))

(def test-type #uuid "ed0e8b73-b8f3-414a-945b-35ee84a98127")
(def test-other-type #uuid "13068b05-bc96-4ed6-9a5f-d49936da40da")

(def test-context #uuid "008dc16e-f7d9-45ce-a043-e482919561c1")
(def test-doc-id #uuid "34a79262-f324-4911-ac33-8bb62f020d09")
(def test-doc-name "Foo")
(def test-doc-root-version "0isw9I_lXQYzvndhqtt7XmMYMGRNye-qO3T9VdlDw04")

(def test-type-id #uuid "60053ded-32e9-48ee-b6b1-d546b3c071b3")
(def test-type-name "TestDoc")

(def test-type-ref-id #uuid "1ececa2b-0bae-478f-80f4-a291deaf2186")
(def test-type-ref-name "TestDocWithRef")

(def test-recursive-type-id #uuid "a0626679-d2a7-4434-a96c-8c4acd75eb47")
(def test-recursive-type-name "TestRecursive")

(def test-doc-id-same-name #uuid "e649ce0d-801d-414c-bdc1-9605ff7090d1")

(def test-query-id #uuid "961882b5-6d56-4126-ac3e-75f6e1b8c9ea")


(def test-doc-type-schema
  {:id        test-type
   :name      "test-type"
   :type      store/schema-type
   :context   test-context
   :state     :posted
   :document  {:definition "::any"}})

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


(def test-recursive-type-doc
  {:id        test-recursive-type-id
   :name      test-recursive-type-name
   :type      store/schema-type
   :context   test-context
   :state     :posted
   :document  {:definition {:id       "::string"
                            :name     "::string"
                            :next     ["maybe" ["recursive:" test-recursive-type-name]]}}})

(def test-query-doc
  {:id        test-query-id
   :name      "test-query"
   :type      q/query-type
   :context   test-context
   :state     :posted
   :document  {:query {:name "test-query"
                       :type test-type
                       :rule nil
                       :sort "name"}}})

(def documents [test-doc-type-schema test-doc test-doc-same-name
                test-type-doc test-type-ref-doc
                test-recursive-type-doc test-query-doc])

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
