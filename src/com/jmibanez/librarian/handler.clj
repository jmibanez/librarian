(ns com.jmibanez.librarian.handler
  (:require [clojure.string :as string]
            [compojure.route :as route]
            [compojure.api.sweet :refer :all]

            [ring.util.http-response :refer :all]

            [mount.core :as mount]
            [clojure.java.jdbc :as jdbc]
            [hugsql.core :as hugsql]

            [taoensso.timbre :as timbre]

            [schema.core :as s]
            [com.jmibanez.librarian.config :as config]))

(timbre/refer-timbre)


(defmacro with-page-params [sort-keys other-params]
  `[{page        :- s/Int 0}
    {page-size   :- s/Int 10}
    {sort-by     :- (s/enum ~@sort-keys) ~sort-default}
    {descending  :- s/Bool false}
    ~@other-params])


(defapi librarian-api
  {:swagger
   {:ui "/"
    :spec "/swagger.json"
    :data {:info {:title "LibrarianAPI"
                  :description "Kagami Librarian Document Store"}
           :tags [{:name "schema" :description "Document Schema API"}
                  {:name "document" :description "Document API"}
                  {:name "view", :description "View API"}
                  {:name "meta", :description "Meta API"}]}}}

  (context "/api" []

    (context "/c/:context-id" [context-id]
      :path-params [context-id :- s/Str]

      (context "/:type-name" [type-name]
        :path-params [type-name :- s/Str]

        (GET "/" []
          :query-params (with-page-params
                          [{view :- views/ViewName}])
          :tags ["view"]

          )

        (context "/.schema" []
          :tags ["schema"]
          )

        (context "/:document-id" [document-id]
          :path-params [document-id :- s/Str]
          :tags ["document"]))

      )
    (GET "/types" []
      :query-params [{page       :- s/Int 0}
                     {page-size  :- s/Int 10}
                     {sort-by    :- (s/enum "tenant_id"
                                            "tenant_tz") "tenant_id"}
                     {descending :- s/Bool false}]
      :return [types/TypeEntity]
      :tags []

      (ok (types/list-types page page-size
                            sort-by descending)))

    (POST "/types" [])

    (context "/documents/:doc-id" [doc-id]
      :path-params [doc-id :- s/Str]

      (GET "/" []
        :return documents/DocumentEntity
        :tags []

        (if-let [result (documents/fetch-document-by-id doc-id)]
          (ok result))))
))

(defn canonicalize-uri [uri]
  (string/replace uri #"//" "/"))

(defn wrap-canonicalize-uri [handler]
  (fn [request]
    (handler (update request
                     :uri canonicalize-uri))))

(def app
  (-> librarian-api
      (wrap-canonicalize-uri)))
