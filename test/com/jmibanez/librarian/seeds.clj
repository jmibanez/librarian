(ns com.jmibanez.librarian.seeds
  (:require [hugsql.core :as hugsql]
            [clojure.java.jdbc :as jdbc]
            [com.jmibanez.librarian.config :as config]
            [com.jmibanez.librarian.bootstrap :refer [do-bootstrap]]))

(hugsql/def-db-fns "sql/base-seed.sql")


(def test-context #uuid "008dc16e-f7d9-45ce-a043-e482919561c1")
(def test-doc-id #uuid "34a79262-f324-4911-ac33-8bb62f020d09")
(def test-doc-name "Foo")
(def test-doc-root-version "ea06354b4d2594116a3ab44a0033bf4b643715a2")

(def test-type-id #uuid "60053ded-32e9-48ee-b6b1-d546b3c071b3")
(def test-type-name "TestDoc")

(def test-type-ref-id #uuid "1ececa2b-0bae-478f-80f4-a291deaf2186")
(def test-type-ref-name "TestDocWithRef")

(defn populate-db [conn]
  (do-bootstrap)
  (seed-test-document-headers! conn)
  (seed-test-documents! conn))

(defn clear-db [conn]
  (clear-test-documents! conn)
  (clear-test-document-headers! conn))

(defn fixture [test]
  (jdbc/with-db-transaction [c config/*datasource*]
    (jdbc/db-set-rollback-only! c)
    (with-redefs [config/*datasource* c]
      (populate-db c)
      (test))))
