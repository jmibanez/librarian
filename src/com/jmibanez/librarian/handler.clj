(ns com.jmibanez.librarian.handler
  (:require [clojure.string :as string]
            [compojure.route :as route]
            [compojure.api.sweet :refer :all]

            [ring.util.http-response :refer :all]

            [mount.core :as mount]
            [clojure.java.jdbc :as jdbc]
            [hugsql.core :as hugsql]

            [clj-uuid :as uuid]

            [taoensso.timbre :as timbre]

            [schema.core :as s]
            [com.jmibanez.librarian
             [config :as config]
             [core-schema :as c]
             [query :as q]
             [store :as store]
             [types :as t]])
  (:import [com.jmibanez.librarian.store
            Document
            Transaction]
           [com.jmibanez.librarian.query
            Query
            QueryPage]))

(timbre/refer-timbre)


(s/defschema ApiDocument {(s/optional-key :name)   (s/maybe s/Str)
                          :state                   store/DocumentState
                          :document                s/Any})

(s/defschema TransactionOptions {(s/optional-key :timeout) (s/maybe s/Int)})


(defapi librarian-api
  {:swagger
   {:ui "/api"
    :spec "/api/swagger.json"
    :data {:info {:title "LibrarianAPI"
                  :description "Librarian Document Store"}
           :tags [{:name "document" :description "Document API"}
                  {:name "transaction" :description "Transaction API"}
                  {:name "query", :description "View API"}]}}}

  (context "/api" []

    :header-params [x-librarian-context  :- c/Context]

    (context "/doc/:type_id" [type_id]
      :path-params [type_id :- c/Id]

      (GET "/" []
        (ok {:endpoints ["/id/:document_id"
                         "/name/:document_name"]}))


      (context "/id/:document_id" [document_id]
        :path-params [document_id :- c/Id]

        (GET "/" []
          :operationId "getDocumentById"
          :header-params [{x-librarian-transaction-id :- c/Id nil}]
          :return      (s/maybe Document)
          :tags        ["document"]

          (let [doc (if-not (nil? x-librarian-transaction-id)
                      (-> (store/get-transaction x-librarian-context
                                                 x-librarian-transaction-id)
                          (store/get-document-by-id-in-transaction document_id))

                      (store/get-document-by-id x-librarian-context
                                                document_id))]
            (if (and (not (nil? doc))
                     (= (:type doc) type_id))
              (ok doc)

              (not-found {:message "Unknown document ID"}))))

        (POST "/" []
          :operationId   "writeDocument"
          :header-params [x-librarian-transaction-id :- c/Id]
          :body          [doc ApiDocument]
          :return        (s/maybe Document)
          :tags          ["document"]

          (if (or (nil? x-librarian-transaction-id)
                  (= uuid/null x-librarian-transaction-id))
            (unauthorized "Implicit transaction not allowed")

            (let [tx (store/get-transaction x-librarian-context
                                            x-librarian-transaction-id)
                  state    (:state    doc)
                  document (:document doc)
                  name     (:name     doc)
                  doc  (or (store/get-document-by-id-in-transaction tx document_id)
                           {:name    name
                            :context x-librarian-context
                            :type    type_id
                            :id      document_id})]

              (if-not (= (:type doc) type_id)
                (bad-request {:message "Invalid type"})

                (t/with-type-context x-librarian-context
                  (let [doc (->> (assoc doc
                                        :document document
                                        :state state)
                                 (store/map->Document))]
                    (if-let [err (:error (t/validate-and-coerce-document
                                          x-librarian-context doc))]
                      (bad-request {:message "Invalid document"
                                    :details err})

                      (ok (store/write-document! tx doc))))))))))

      (GET "/name/:document_name" [document_name]
        :operationId "getDocumentByTypeAndName"
        :header-params [{x-librarian-transaction-id :- c/Id nil}]
        :path-params [document_name :- s/Str]
        :tags        ["document"]

        (if-let [doc (if-not (nil? x-librarian-transaction-id)
                       (-> (store/get-transaction x-librarian-context
                                                  x-librarian-transaction-id)
                           (store/get-document-by-name-in-transaction type_id
                                                                      document_name))

                       (store/get-document-by-name x-librarian-context
                                                   type_id document_name))]
          (see-other (str "/api/doc/" type_id "/id/" (:id doc)))

          (not-found {:message "Unknown document type/name"}))))

    (context "/q" []
      (GET "/:query_id" [query_id]
        :operationId "query"
        :path-params [query_id :- c/Id]
        :query-params [{page       :- s/Int 0}
                       {page_size  :- s/Int 10}
                       :as params]
        :return      (s/maybe QueryPage)
        :tags        ["document" "query"]

        (t/with-type-context x-librarian-context
          (if-let [q (q/get-query-document-by-id x-librarian-context
                                                 query_id)]
            (ok
             (-> (q/parse-query-document q)
                 (store/exec (merge (dissoc params :page :page_size)
                                    {:page      page
                                     :page-size page_size
                                     :context   x-librarian-context}))
                 (dissoc :q)))

            (not-found {:message "Query not found"})))))

    (context "/tx" []

      (POST "/" []
        :operationId "startTransaction"
        :body        [tx-opts (s/maybe TransactionOptions)]
        :return      Transaction
        :tags        ["transaction"]

        (ok (let [{:keys [timeout]} tx-opts]
              (if-not (nil? timeout)
                (store/start-transaction! x-librarian-context timeout)
                (store/start-transaction! x-librarian-context)))))

      (GET "/:transaction_id" [transaction_id]
        :operationId "getTransactionStub"
        :path-params [transaction_id :- c/Id]
        :return      (s/maybe Transaction)
        :tags        ["transaction"]

        (if-let [tx (store/get-transaction x-librarian-context
                                           transaction_id)]
          (ok tx)

          (not-found {:message "Unknown transaction"})))

      (DELETE "/:transaction_id" [transaction_id]
        :operationId "cancelTransaction"
        :path-params [transaction_id :- c/Id]
        :return      (s/maybe Transaction)
        :tags        ["transaction"]

        (if-let [tx (-> (store/get-transaction x-librarian-context
                                               transaction_id)
                        (store/cancel-transaction!))]
          (ok tx)

          (not-found {:message "Couldn't cancel transaction: Unknown transaction"})))

      (POST "/:transaction_id" [transaction_id]
        :operationId "commitTransaction"
        :path-params [transaction_id :- c/Id]
        :return      (s/maybe Transaction)
        :tags        ["transaction"]

        (if-let [tx (-> (store/get-transaction x-librarian-context
                                               transaction_id)
                        (store/commit-transaction!))]
          (condp = (:state tx)
            :committed (ok tx)
            :conflict  (bad-request tx)

            (method-not-allowed tx))

          (not-found {:message "Couldn't commit transaction: Unknown transaction"}))))))


(defn canonicalize-uri [uri]
  (string/replace uri #"//" "/"))

(defn wrap-canonicalize-uri [handler]
  (fn [request]
    (handler (update request
                     :uri canonicalize-uri))))

(def app
  (wrap-canonicalize-uri librarian-api))
