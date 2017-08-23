(ns com.jmibanez.librarian.query
  (:require [taoensso.timbre :as timbre]
            [clojure.walk :refer [postwalk]]
            [clojure.string :as str]
            [clojure.java.jdbc :as jdbc]
            [cheshire.core :as json]
            [clj-uuid :as uuid]
            [schema.core :as s]
            [hugsql.core :as hugsql]
            [hugsql.adapter :as hugsql-adapter]
            [honeysql.core :as honeysql]
            [honeysql.helpers :as sqlgen]
            [honeysql.format :as sqlfmt]
            [honeysql.types :refer [param-name]]

            [camel-snake-kebab.core :refer [->kebab-case
                                            ->snake_case]]

            [json-path]
            [json-path.parser :as path-parser]

            [com.jmibanez.librarian.store :as store]
            [com.jmibanez.librarian.types :as t])
  (:import [com.jmibanez.librarian.store Document Transaction]
           [honeysql.types SqlParam]
           [java.util Date]))


(timbre/refer-timbre)

(def allowed-match-attributes #{:state :date-created :date-last-modified})

(def MatchKeyPath #"^\$\.?(\.(\w+))|(\[\*\])+$")
(def MatchKeyAttribute (str/join "|" (map name allowed-match-attributes)))
(def MatchKey s/Str)
(def MatchVariable #"^([\w-_]+)$")


(def SortCondition #"^(\w+)(\.\w+)*")
(declare Rule)

(s/defschema MatchRule
  {:match MatchKey
   :with (s/conditional :variable {:variable s/Str}
                        :else     s/Any)})

(s/defschema AllOfRule
  {:all_of [(s/recursive #'Rule)]})

(s/defschema AnyOfRule
  {:any_of [(s/recursive #'Rule)]})

(s/defschema Rule
  (s/conditional :all_of AllOfRule
                 :any_of AnyOfRule
                 :else MatchRule))


(s/defschema QueryDocument
  {:query {:name s/Str
           :type s/Uuid
           :rule (s/maybe Rule)
           :sort (s/maybe SortCondition)}})


(def query-type #uuid "c9918f14-2469-4f03-988a-dbbb95fa3afc")
(def query-type-name "query")
(def query-rule-type #uuid "002cd31c-4cc5-4546-b620-d3458283d0d8")
(def query-rule-type-name "QueryRule")


(hugsql/def-db-fns "sql/query.sql")

(s/defn create-query-document! :- Document
  "Construct a storage Document from the given query"
  [context     :- store/Context
   transaction :- Transaction
   query-doc   :- (s/maybe QueryDocument)]

  (->> {:id                 (uuid/v4)
        :name               (:name query-doc)
        :type               query-type
        :context            context
        :state              :created
        :document           query-doc
        :date-created       (new Date)
        :date-last-modified (new Date)}
       (store/strict-map->Document)
       (store/write-document! transaction)))

(s/defn update-query-document! :- Document
  "Update the given query document. Query name cannot be
  changed."
  [context     :- store/Context
   transaction :- Transaction
   query-id    :- store/Id
   query-doc   :- (s/maybe QueryDocument)]

  (if-let [doc (store/get-document-by-id context query-id)]
    (if-not (= query-type
               (:type doc))
      (throw (Exception. "Not a valid query document"))

      (->> (assoc doc :document query-doc)
           (store/write-document! transaction)))

    (create-query-document! context transaction query-doc)))

(s/defn get-query-document-by-id :- QueryDocument
  [context   :- store/Context
   query-id  :- store/Id]
  (if-let [doc (store/get-document-by-id context query-id)]
    (if-not (= query-type
               (:type doc))
      (throw (Exception. "Not a valid query document"))

      (:document doc))

    nil))

(s/defn get-query-document-by-name :- QueryDocument
  [context    :- store/Context
   query-name :- s/Str]
  (if-let [doc (store/get-document-by-name context
                                           query-type
                                           query-name)]
    (if-not (= query-type
               (:type doc))
      (throw (Exception. "Not a valid query document"))

      (:document doc))

    nil))


;; Generators for default queries
(s/defn gen-query-get-documents-of-type :- QueryDocument
  [context     :- store/Context
   transaction :- Transaction
   type        :- s/Uuid
   type-name   :- s/Str
   sort-rule   :- SortCondition]
  (let [query-name (str "get-all-documents:" type-name)]
    (create-query-document! context transaction
                            {:query {:name   query-name
                                     :type   type
                                     :rule   nil
                                     :sort   sort-rule}})))

;; Default system queries
(def sys-query-get-root-documents
  {:query {:name  "system-get-all-documents:root"
           :type  store/root-type
           :rule  nil
           :sort  nil}})
(def sys-query-get-schema-documents
  {:query {:name  "system-get-all-documents:schemas"
           :type  store/schema-type
           :rule  nil
           :sort  nil}})



(defprotocol Pageable
  (next-page-exec [this])
  (prev-page-exec [this]))


(declare query-reexec)
(s/defrecord QueryPage
    [page        :- s/Int
     page-size   :- s/Int
     total       :- s/Int
     results     :- [Document]]

  Pageable
  (next-page-exec [this]
    (query-reexec (:q this) (inc (:page this)) (:page-size this)))
  (prev-page-exec [this]
    (if-not (zero? (:page this))
      (query-reexec (:q this) (dec (:page this)) (:page-size this))
      (:q this))))

(s/defrecord Query [doc   :- QueryDocument
                    type  :- s/Uuid
                    sort  :- s/Any
                    sql   :- s/Any]

  store/Executable
  (execute-this
    [q conn params]
    "Execute a Query object using the given connection, with the given
    parameters."
    (let [{:keys [page page-size]
           :as   params} params
          type (:type q)
          params (merge params {:type type})
          sort-sql (if-not (nil? (:sort q))
                     (honeysql/format (:sort q)))
          sql (honeysql/format (:sql q)
                               :params params)
          query-params (merge params
                              {:offset       (* page page-size)
                               :count        page-size
                               :sort         sort-sql
                               :query_filter sql})
          result-size (:count (query-document-size conn query-params))
          results (map store/doc-row->Document
                       (query-document-full conn query-params))]

      (-> {:page       page
           :page-size  page-size
           :total      result-size
           :results    results}
          (strict-map->QueryPage)
          (assoc :q q)))))


(def base-document-query-map
  {:select [:h.id]
   :from [[:document_header :h]]
   :join [[:document :d] [:= :h.id :d.id]]})


(def index-query-map
  {:select [:idx.document_id]
   :from   [[:document_index_view :idx]]})

(declare rule->sql-fragment
         ->sort-sql-expression)
(s/defn parse-query-document :- Query
  "Parse a query document into a Query object."
  [query-doc :- QueryDocument]

  (let [root-rule (get-in query-doc [:query :rule])
        type      (get-in query-doc [:query :type])
        sort-key  (get-in query-doc [:query :sort])
        sort-sql  (->sort-sql-expression sort-key)
        root-cond (rule->sql-fragment root-rule)
        sql       (-> base-document-query-map
                      (sqlgen/where root-cond))]
    (strict-map->Query {:doc  query-doc
                        :type type
                        :sort sort-sql
                        :sql  sql})))


(defn- dispatch-rule->sql-fragment
  [rule]
  (sort (keys rule)))

(defmulti rule->sql-fragment #'dispatch-rule->sql-fragment)


;; Default: return empty
(defmethod rule->sql-fragment :default
  [_]
  nil)

(defmethod rule->sql-fragment [:any_of]
  [rule]
  (let [or-rules (map rule->sql-fragment (:any_of rule))]
    (concat [:or] or-rules)))

(defmethod rule->sql-fragment [:all_of]
  [rule]
  (let [and-rules (map rule->sql-fragment (:all_of rule))]
    (concat [:and] and-rules)))

(declare path->query-expression)
(defmethod rule->sql-fragment [:match :with]
  [rule]
  (let [match-key (:match rule)
        match-rhs (:with rule)
        var-ref   (:variable match-rhs)
        rhs       (if-not (nil? var-ref)
                    (honeysql/param (keyword var-ref))
                    match-rhs)]
    (path->query-expression match-key rhs)))


(defn expand-sql-parameter-in-subdoc [subdoc]
  (postwalk (fn [elem]
              (if (instance? SqlParam elem)
                (let [pname (param-name elem)]
                  (if (map? @sqlfmt/*input-params*)
                    (get @sqlfmt/*input-params* pname)
                    (let [x (first @sqlfmt/*input-params*)]
                      (swap! sqlfmt/*input-params* rest)
                      (sqlfmt/add-param pname x))))
                elem))
            subdoc))

(defn subdoc-to-sql [subdoc]
  (-> subdoc
      (expand-sql-parameter-in-subdoc)
      (json/generate-string)))

(defmethod sqlfmt/fn-handler "json_is_subdocument" [_ lhs rhs]
  (let [rhs-subdoc (subdoc-to-sql rhs)]
    (str (sqlfmt/to-sql lhs) " @> '" rhs-subdoc "'")))

(defn valid-document-attribute-for-query? [attr]
  (contains? allowed-match-attributes
             (keyword attr)))


(defn yield-path-construct
  [path-list inner-subdocs]
  (let [inner-outer-path (reverse path-list)]
    (reduce (fn [inner outer]
              (condp #(= (first %2) %1) outer
                :key
                {(keyword (second outer)) inner}

                :child
                inner

                :root
                inner))
            inner-subdocs
            inner-outer-path)))

(defn yield-selector-construct
  [_ inner-subdocs]
  ;; We actually ignore the actual selector
  [inner-subdocs])

(defn walk-path-ops
  [[opcode operands rest] rhs]

  (let [inner-subdocs (if-not (or (nil? rest)
                                  (empty? rest))
                           (walk-path-ops rest rhs)
                           rhs)
        subdocument (case opcode
                      :path
                      (yield-path-construct operands
                                            inner-subdocs)

                      :selector
                      (yield-selector-construct operands
                                                inner-subdocs))]

    subdocument))


(defn path->index-selector
  [path rhs]
  (let [path-subselector (str "%" (subs path 2))
        index-selector [:and [:like :idx.path path-subselector]
                             [:= :idx.value (honeysql/call :jsonb
                                                           (json/generate-string rhs))]]
        selector-expr (-> index-query-map
                          (sqlgen/where index-selector))]
    [:in :h.id selector-expr]))

(defn path->json-subdocument
  [path rhs]

  (let [path-ops (path-parser/parse-path path)
        subdoc   (walk-path-ops path-ops rhs)]
    subdoc))

(defn path->query-expression
  "Convert a possible JSON Path expression to a Postgres query
  expression. If the path is not a valid JSON Path expression, we
  assume it is one of the document attributes."
  [path rhs]

  (if-not (re-find MatchKeyPath path)
    (if-not (valid-document-attribute-for-query? path)
      ;; FIXME: Raise parse exception
      (error "Should raise exception: Not valid attribute")

      [:= (honeysql/raw (->snake_case path)) rhs])

    (if (= (subs path 0 3) "$..")
      ;; Ancestor query
      (path->index-selector path rhs)

      [:json_is_subdocument :document (path->json-subdocument path rhs)])))

(defn ->sort-sql-expression [sort-key]
  (if-not (nil? sort-key)
    (if (contains? allowed-match-attributes (keyword sort-key))
      (honeysql/raw (->snake_case sort-key))

      (let [sort-path (str/split sort-key #"\.")]
        (apply honeysql/call :jsonb_extract_path :d.document
               sort-path)))))

(defrecord QueryWrapper [q page page-size]
  store/Executable
  (execute-this [qw c {:as params}]
    (store/execute-this (:q qw) c
                        (merge {:page-size (:page-size qw)
                                :page      (:page      qw)}
                               (spy :debug params)))))


(defn query-reexec [q page page-size]
  (->QueryWrapper q page page-size))
