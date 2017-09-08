(ns com.jmibanez.librarian.handler-test
  (:require [expectations :refer :all]
            [clojure.instant :refer [read-instant-date]]
            [ring.mock.request :as mock]
            [cheshire.core :refer [parse-string generate-string]]
            [schema-generators.generators :as g]
            [clojure.test.check.generators :as tcg]
            [clj-uuid :as uuid]
            [taoensso.timbre :as timbre
             :refer [spy debug trace get-env log-env]]
            [schema.coerce :as coerce]
            [com.jmibanez.librarian
             [handler :refer [app]]
             [seeds :as seeds]
             [store :as store]
             [test-helpers :refer [GET DELETE POST PUT
                                   parse-body
                                   serialize-body]]])
  (:import [java.util Date]
           [com.jmibanez.librarian.store
            Document
            Transaction]))



(def context-headers {:x-librarian-context seeds/test-context})

(defn transaction-headers [tx]
  (assoc context-headers
         :x-librarian-transaction-id (:id tx)))

(def datetime-regex #"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{2,3})?Z")
(defn- datetime-matcher [schema]
  (when (= Date schema)
    (coerce/safe
     (fn [x]
       (if (and (string? x) (re-matches datetime-regex x))
         (read-instant-date x)
         x)))))

(defn coerce-to-document [doc]
  (let [coerce->Document
        (coerce/coercer Document
                        (coerce/first-matcher [datetime-matcher
                                               coerce/json-coercion-matcher]))]
    (-> doc
        (store/map->Document)
        (coerce->Document))))


;; -------------------------------------
;; Get document by ID
(expect seeds/test-doc
        (in (-> (GET (str "/api/doc/"
                          seeds/test-type
                          "/id/"
                          seeds/test-doc-id)
                    :headers context-headers)
                parse-body
                coerce-to-document)))

(expect seeds/test-doc-same-name
        (in (-> (GET (str "/api/doc/"
                          seeds/test-other-type
                          "/id/"
                          seeds/test-doc-id-same-name)
                    :headers context-headers)
                parse-body
                coerce-to-document)))

(expect seeds/test-type-doc
        (in (-> (GET (str "/api/doc/"
                          store/schema-type
                          "/id/"
                          seeds/test-type-id)
                    :headers context-headers)
                parse-body
                coerce-to-document)))

(expect seeds/test-type-ref-doc
        (in (-> (GET (str "/api/doc/"
                          store/schema-type
                          "/id/"
                          seeds/test-type-ref-id)
                    :headers context-headers)
                parse-body
                coerce-to-document)))

;; -------------------------------------
;; Should 404 on wrong document types
(expect 404
        (-> (GET (str "/api/doc/"
                      seeds/test-type-ref-id
                      "/id/"
                      seeds/test-doc-id)
                :headers context-headers)
            :status))

(expect 404
        (-> (GET (str "/api/doc/"
                      seeds/test-type-id
                      "/id/"
                      seeds/test-doc-id-same-name)
                :headers context-headers)
            :status))

(expect 404
        (-> (GET (str "/api/doc/"
                      store/root-type
                      "/id/"
                      seeds/test-type-id)
                :headers context-headers)
            :status))

(expect 404
        (-> (GET (str "/api/doc/"
                      store/root-type
                      "/id/"
                      seeds/test-type-ref-id)
                :headers context-headers)
            :status))

;; -------------------------------------
;; Get document by name: Redirect to ID
(expect 303
        (-> (GET (str "/api/doc/"
                         seeds/test-type
                         "/name/"
                         seeds/test-doc-name)
                   :headers context-headers)
            :status))
(expect {"Location" (str "/api/doc/" seeds/test-type "/id/" seeds/test-doc-id)}
        (in (-> (GET (str "/api/doc/"
                          seeds/test-type
                          "/name/"
                          seeds/test-doc-name)
                    :headers context-headers)
                :headers)))


(expect 303
        (-> (GET (str "/api/doc/"
                      seeds/test-other-type
                      "/name/"
                      seeds/test-doc-name)
                :headers context-headers)
            :status))
(expect {"Location" (str "/api/doc/" seeds/test-other-type
                         "/id/" seeds/test-doc-id-same-name)}
        (in (-> (GET (str "/api/doc/"
                          seeds/test-other-type
                          "/name/"
                          seeds/test-doc-name)
                    :headers context-headers)
                :headers)))
(expect 303
        (-> (GET (str "/api/doc/"
                      store/schema-type
                      "/name/"
                      seeds/test-type-name)
                :headers context-headers)
            :status))
(expect {"Location" (str "/api/doc/" store/schema-type
                         "/id/" seeds/test-type-id)}
        (in (-> (GET (str "/api/doc/"
                          store/schema-type
                          "/name/"
                          seeds/test-type-name)
                    :headers context-headers)
                :headers)))

(expect 303
        (-> (GET (str "/api/doc/"
                      store/schema-type
                      "/name/"
                      seeds/test-type-ref-name)
                :headers context-headers)
            :status))
(expect {"Location" (str "/api/doc/" store/schema-type
                         "/id/" seeds/test-type-ref-id)}
        (in (-> (GET (str "/api/doc/"
                          store/schema-type
                          "/name/"
                          seeds/test-type-ref-name)
                    :headers context-headers)
                :headers)))

;; -------------------------------------
;; Transactions

;; Be able to create a transaction

(defn coerce-to-transaction [tx]
  (let [coerce->Transaction
        (coerce/coercer Transaction
                        (coerce/first-matcher [datetime-matcher
                                               coerce/json-coercion-matcher]))]
    (-> tx
        (store/map->Transaction)
        (coerce->Transaction))))

(expect 200 (-> (POST "/api/tx/"
                    :headers context-headers)
                :status))
(expect {:context seeds/test-context}
        (in (-> (POST "/api/tx/"
                    :headers context-headers)
                parse-body
                coerce-to-transaction)))
(expect {:timeout 5000}
        (in (-> (POST "/api/tx/"
                    :headers context-headers)
                parse-body
                coerce-to-transaction)))
(expect {:state :started}
        (in (-> (POST "/api/tx/"
                    :headers context-headers)
                parse-body
                coerce-to-transaction)))


(defmacro with-transaction [[[tx-header-sym tx-id-sym] context-headers] & body]
  `(let [tx# (-> (POST "/api/tx"
                     :headers context-headers)
                 parse-body
                 coerce-to-transaction)
         ~tx-id-sym (:id tx#)
         ~tx-header-sym (merge context-headers
                               {:x-librarian-transaction-id ~tx-id-sym})]
     ~@body))

;; Be able to get a document within a transaction
(expect seeds/test-doc
        (in (with-transaction [[tx-headers _] context-headers]
              (-> (GET (str "/api/doc/"
                            seeds/test-type
                            "/id/"
                            seeds/test-doc-id)
                      :headers tx-headers)
                  parse-body
                  coerce-to-document))))

;; Update existing document
(let [updated-test-doc (assoc-in seeds/test-doc
                                 [:document :age] "::integer")]
  (expect updated-test-doc
          (in (with-transaction [[tx-headers _] context-headers]
                (-> (POST (str "/api/doc/"
                               seeds/test-type
                               "/id/"
                               seeds/test-doc-id)
                        :body {:state :posted
                               :document (:document updated-test-doc)}
                        :headers tx-headers)
                    parse-body
                    coerce-to-document)))))

;; Create new document
(let [new-doc {:id        (uuid/v4)
               :name      "My Document"
               :type      seeds/test-type-id
               :context   seeds/test-context
               :state     :new
               :document  {:id    "My Document"
                           :name  "Name"
                           :inner [{:name  "One"
                                    :value 1}
                                   {:name  "Magic"
                                    :value 42}]}}]
  (expect new-doc
          (in (with-transaction [[tx-headers _] context-headers]
                (-> (POST (str "/api/doc/"
                               (:type new-doc)
                               "/id/"
                               (:id new-doc))
                        :body {:name     (:name new-doc)
                               :state    :new
                               :document (:document new-doc)}
                        :headers tx-headers)
                    parse-body
                    coerce-to-document)))))

;; Document writes should be validated against type
(let [new-doc {:id        (uuid/v4)
               :name      "My Document"
               :type      seeds/test-type-id
               :context   seeds/test-context
               :state     :new
               :document  {:id    "My Document"
                           :name  "Name"
                           :inner [{:name  "One"
                                    :value 1}
                                   {:name  "Magic"
                                    :value "42"}]}}]
  (expect 400
          (with-transaction [[tx-headers _] context-headers]
            (-> (POST (str "/api/doc/"
                           (:type new-doc)
                           "/id/"
                           (:id new-doc))
                    :body {:name     (:name new-doc)
                           :state    :new
                           :document (:document new-doc)}
                    :headers tx-headers)
                :status)))

  (expect {:message "Invalid document"}
          (in (with-transaction [[tx-headers _] context-headers]
                (-> (POST (str "/api/doc/"
                               (:type new-doc)
                               "/id/"
                               (:id new-doc))
                        :body {:name     (:name new-doc)
                               :state    :new
                               :document (:document new-doc)}
                        :headers tx-headers)
                    parse-body)))))

;; Updated document should only be visible from within transaction
(let [updated-test-doc (assoc-in seeds/test-doc
                                 [:document :age] "::integer")]
  (expect updated-test-doc
          (in (with-transaction [[tx-headers _] context-headers]
                (POST (str "/api/doc/"
                           seeds/test-type
                           "/id/"
                           seeds/test-doc-id)
                    :body {:state :posted
                           :document (:document updated-test-doc)}
                    :headers tx-headers)

                (-> (GET (str "/api/doc/"
                              seeds/test-type
                              "/id/"
                              seeds/test-doc-id)
                        :headers tx-headers)
                    parse-body
                    coerce-to-document))))

  (expect seeds/test-doc
          (in (with-transaction [[tx-headers _] context-headers]
                (POST (str "/api/doc/"
                           seeds/test-type
                           "/id/"
                           seeds/test-doc-id)
                    :body {:state :posted
                           :document (:document updated-test-doc)}
                    :headers tx-headers)

                (-> (GET (str "/api/doc/"
                              seeds/test-type
                              "/id/"
                              seeds/test-doc-id)
                        :headers context-headers)
                    parse-body
                    coerce-to-document)))))


;; New document should only be visible from within the transaction
(let [new-doc {:id        (uuid/v4)
               :name      "My Document"
               :type      seeds/test-type-id
               :context   seeds/test-context
               :state     :new
               :document  {:id    "My Document"
                           :name  "Name"
                           :inner [{:name  "One"
                                    :value 1}
                                   {:name  "Magic"
                                    :value 42}]}}]
  (expect new-doc
          (in (with-transaction [[tx-headers _] context-headers]
                (POST (str "/api/doc/"
                           (:type new-doc)
                           "/id/"
                           (:id new-doc))
                    :body {:name     (:name new-doc)
                           :state    :new
                           :document (:document new-doc)}
                    :headers tx-headers)

                (-> (GET (str "/api/doc/"
                              (:type new-doc)
                              "/id/"
                              (:id new-doc))
                        :headers tx-headers)
                    parse-body
                    coerce-to-document))))

  (expect 404
          (with-transaction [[tx-headers _] context-headers]
            (POST (str "/api/doc/"
                       (:type new-doc)
                       "/id/"
                       (:id new-doc))
                :body {:name     (:name new-doc)
                       :state    :new
                       :document (:document new-doc)}
                :headers tx-headers)

            (-> (GET (str "/api/doc/"
                          (:type new-doc)
                          "/id/"
                          (:id new-doc))
                    :headers context-headers)
                :status))))

;; Updated document should be visible after tx commit
(let [updated-test-doc (assoc-in seeds/test-doc
                                 [:document :age] "::integer")]
  (expect updated-test-doc
          (in (with-transaction [[tx-headers tx-id] context-headers]
                (POST (str "/api/doc/"
                           seeds/test-type
                           "/id/"
                           seeds/test-doc-id)
                    :body {:state :posted
                           :document (:document updated-test-doc)}
                    :headers tx-headers)

                (POST (str "/api/tx/" tx-id)
                    :headers context-headers)

                (-> (GET (str "/api/doc/"
                              seeds/test-type
                              "/id/"
                              seeds/test-doc-id)
                        :headers context-headers)
                    parse-body
                    coerce-to-document)))))


;; New document should be visible after tx commit
(let [new-doc {:id        (uuid/v4)
               :name      "My Document"
               :type      seeds/test-type-id
               :context   seeds/test-context
               :state     :new
               :document  {:id    "My Document"
                           :name  "Name"
                           :inner [{:name  "One"
                                    :value 1}
                                   {:name  "Magic"
                                    :value 42}]}}]
  (expect new-doc
          (in (with-transaction [[tx-headers tx-id] context-headers]
                (POST (str "/api/doc/"
                           (:type new-doc)
                           "/id/"
                           (:id new-doc))
                    :body {:name     (:name new-doc)
                           :state    :new
                           :document (:document new-doc)}
                    :headers tx-headers)

                (POST (str "/api/tx/" tx-id)
                    :headers context-headers)

                (-> (GET (str "/api/doc/"
                              (:type new-doc)
                              "/id/"
                              (:id new-doc))
                        :headers context-headers)
                    parse-body
                    coerce-to-document)))))

;; Updated document should revert after tx cancellation
(let [updated-test-doc (assoc-in seeds/test-doc
                                 [:document :age] "::integer")]
  (expect seeds/test-doc
          (in (with-transaction [[tx-headers tx-id] context-headers]
                (POST (str "/api/doc/"
                           seeds/test-type
                           "/id/"
                           seeds/test-doc-id)
                    :body {:state :posted
                           :document (:document updated-test-doc)}
                    :headers tx-headers)

                (DELETE (str "/api/tx/" tx-id)
                    :headers context-headers)

                (-> (GET (str "/api/doc/"
                              seeds/test-type
                              "/id/"
                              seeds/test-doc-id)
                        :headers context-headers)
                    parse-body
                    coerce-to-document)))))


;; New document should not be "gone" after tx cancellation
(let [new-doc {:id        (uuid/v4)
               :name      "My Document"
               :type      seeds/test-type-id
               :context   seeds/test-context
               :state     :new
               :document  {:id    "My Document"
                           :name  "Name"
                           :inner [{:name  "One"
                                    :value 1}
                                   {:name  "Magic"
                                    :value 42}]}}]
  (expect 404
          (with-transaction [[tx-headers tx-id] context-headers]
            (POST (str "/api/doc/"
                       (:type new-doc)
                       "/id/"
                       (:id new-doc))
                :body {:name     (:name new-doc)
                       :state    :new
                       :document (:document new-doc)}
                :headers tx-headers)

            (DELETE (str "/api/tx/" tx-id)
                :headers context-headers)

            (-> (GET (str "/api/doc/"
                          (:type new-doc)
                          "/id/"
                          (:id new-doc))
                    :headers context-headers)
                :status))))
