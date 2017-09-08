(ns com.jmibanez.librarian.test-helpers
  (:require [expectations :refer :all]
            [cheshire.core :refer [parse-string generate-string]]
            [ring.mock.request :as mock]))

;; Handler test helpers
(defn parse-body [request]
  (-> request
      :body
      slurp
      (parse-string true)))

(defn serialize-body [request body]
  (->> body
       generate-string
       (mock/body request)))

(defmacro GET [uri-path & {:keys [headers params]}]
  (let [headers (or headers {})]
    `(let [base-req# (mock/request :get ~uri-path ~params)
           with-headers# (reduce #(apply mock/header %1 %2)
                                 base-req# ~headers)]
       (~'app with-headers#))))

(defn- with-body [method uri-path & {:keys [headers params body]}]
  (let [headers (or headers {})]
    `(let [base-req# (-> (mock/request ~method ~uri-path)
                         (mock/content-type "application/json"))
           with-headers# (reduce #(apply mock/header %1 %2)
                                 base-req# ~headers)]
       (-> with-headers#
           (serialize-body ~body)
           ~'app))))

(defmacro DELETE [uri-path & body]
  (cond
    (nil? body) `(~'app (mock/request :delete ~uri-path))
    :else        (apply (partial with-body :delete uri-path)
                        body)))

(defmacro POST [uri-path & rest]
  (apply with-body :post uri-path rest))

(defmacro PUT [uri-path & rest]
  (apply with-body :put uri-path rest))

