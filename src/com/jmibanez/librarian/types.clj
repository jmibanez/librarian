(ns com.jmibanez.librarian.types
  (:require [taoensso.timbre :as timbre]
            [clj-uuid :as uuid]
            [schema.core :as s]
            [com.jmibanez.librarian.core-schema :as core-schema]
            [com.jmibanez.librarian.config :as config]
            [com.jmibanez.librarian.store :as store]))

(timbre/refer-timbre)


(def sequence-keys
  {"one"      s/one
   "maybe"    s/maybe
   "optional" s/optional})


(declare TypeDefinition)
(s/defschema TypeConditional
  [(s/one (s/eq "conditional:") "cond-key")
   [(s/one (s/maybe s/Str) "cond") (s/recursive #'TypeDefinition)]])

(s/defschema TypeSpecialVector
  (s/conditional #(contains? sequence-keys (first %))
                 [(s/one (apply s/enum (keys sequence-keys)) "key") (s/recursive #'TypeDefinition)]

                 #(= "enum:" (first %))
                 [(s/one (s/eq "enum:") "enum-key") s/Str]

                 #(= "conditional:" (first %))
                 TypeConditional

                 ;; #(= "recursive:" (first %))
                 ;; TypeRecursive
))

(s/defschema TypeVector
  (s/conditional #(string? (first %)) TypeSpecialVector
                 :else [(s/recursive #'TypeDefinition)]))


(s/defschema TypeMap
  {s/Keyword (s/recursive #'TypeDefinition)})


(s/defschema TypeDefinition
  (s/conditional vector? TypeVector
                 map?    TypeMap
                 :else   (s/maybe s/Str)))

(s/defrecord Type [id         :- core-schema/Id
                   owner      :- core-schema/Context
                   name       :- s/Str
                   definition :- TypeDefinition])


(declare find-type-by-id)
(s/defn get-type-by-id :- Type
  [owning-context :- core-schema/Context
   type-id        :- core-schema/Id]
  (find-type-by-id owning-context type-id))

(declare find-type-by-name)
(s/defn get-type-by-name :- Type
  [owning-context :- core-schema/Context
   type-name      :- s/Str]
  (find-type-by-name owning-context type-name))

(declare write-type-definition)
(s/defn create-type :- Type
  [owning-context :- core-schema/Context
   type-name      :- s/Str
   definition     :- s/Any]
  (let [type-id (uuid/v4)
        type-def (strict-map->Type {:id type-id
                                    :owner owning-context
                                    :name type-name
                                    :definition definition})]
    (write-type-definition type-def)))

(s/defn update-type-definition :- Type
  [owning-context :- core-schema/Context
   type-id        :- core-schema/Id
   new-definition :- s/Any]
  (let [type-def (find-type-by-id owning-context type-id)]
    (write-type-definition
     (assoc type-def :definition new-definition))))


(declare resolve-type-ref)
(s/defn validate-against-type :- s/Any
  [owning-context :- core-schema/Context
   type-name      :- s/Str
   document       :- s/Any]
  (let [schema-def (resolve-type-ref owning-context type-name)]
    (s/validate schema-def document)))



(defn find-type-by-id [owning-context type-id]
  (if-let [type-doc (store/get-document-by-id owning-context type-id)]
    (if-not (= (:type type-doc)
               store/schema-type)
      (throw (Exception. "Not a valid type document"))

      (strict-map->Type (assoc (:document type-doc)
                               :id (:id type-doc)
                               :name (:name type-doc)
                               :owner (:context type-doc))))
    nil))

(defn find-type-by-name [owning-context name]
  (let [type-doc-name (str "type/" name)]
    (if-let [type-doc (store/get-document-by-name owning-context
                                                  store/schema-type
                                                  type-doc-name)]
      (if-not (= (:type type-doc)
                 store/schema-type)
        (throw (Exception. "Not a valid type document"))

        (strict-map->Type (assoc (:document type-doc)
                                 :id (:id type-doc)
                                 :name (:name type-doc)
                                 :owner (:context type-doc))))
      nil)))

(defn write-type-definition [type-def])

(def type-schemas (atom {}))
(def type-schema-references (agent {}))

(def core-schema-types
  {"::string"  s/Str
   "::number"  s/Num
   "::integer" s/Int
   "::boolean" s/Bool
   "::uuid"    s/Uuid
   "::any"     s/Any})

(defn create-type-context [context]
  (swap! type-schemas
         assoc context (agent {})))

(defn destroy-type-context [context]
  (swap! type-schemas
         dissoc context))

(defn create-type-schema-references-for-context [context]
  (send type-schema-references assoc context {}))

(defn destroy-type-schema-references-for-context [context]
  (send type-schema-references dissoc context))

(defn bind-type-in-context [context type-name schema-def]
  (let [type-context (get @type-schemas context)]
    (send type-context assoc type-name schema-def)))

(defn add-type-reference [context type-name
                          referring-type-name]
  (if-let [ref-list (get-in @type-schema-references [context
                                                     type-name])]
    (send ref-list conj referring-type-name)
    (send type-schema-references assoc-in
          [context type-name] (agent []))))

(declare compile-type-definition)
(defn resolve-type-ref [context type-name]
  (let [type-context (get @type-schemas context)]
    (if-let [core-schema-def (get core-schema-types type-name)]
      core-schema-def

      (if-let [schema-def (get @type-context type-name)]
        schema-def
        (compile-type-definition context type-name)))))

(defmulti compile-type-def
  (fn [context this-type-name definition]
    (type definition)))

(defmethod compile-type-def
  String
  [context this-type-name ref-type-name]
  (add-type-reference context ref-type-name this-type-name)
  (resolve-type-ref context ref-type-name))


(defmulti compile-type-sequence
  (fn [context this-type-name
       type-sequence-head type-sequence-rest]
    type-sequence-head))

(defmethod compile-type-sequence
  "enum:"
  [context this-type-name
   _ enum-values]
  (apply s/enum enum-values))

(defmethod compile-type-sequence
  "conditional:"
  [context this-type-name
   _ condition-tuples]
  (let [pairs (for [[key schema] condition-tuples]
                (let [rhs (compile-type-def context this-type-name
                                            schema)
                      lhs (if (nil? key)
                            :else
                            (keyword key))]
                  [lhs rhs]))]
    (apply s/conditional (flatten pairs))))

(defmethod compile-type-sequence
  :default

  [context this-type-name
   type-sequence-head
   type-sequence-rest]

  (let [compiler (partial compile-type-def context
                          this-type-name)]
    (if (contains? sequence-keys type-sequence-head)
      (let [key-fn (get sequence-keys type-sequence-head)]
        (apply key-fn
               (map compiler type-sequence-rest)))

      (mapv compiler (conj type-sequence-head
                           type-sequence-rest)))))

(declare create-conditional)
(defmethod compile-type-def
  clojure.lang.PersistentVector
  [context this-type-name type-sequence]
  (let [compiler (partial compile-type-def context
                          this-type-name)
        first-elem (first type-sequence)]

    (or
     ;; Special case: first elem is a string ...
     (and (= (type first-elem)
             String)
          (cond
            ;; ... is a sequence-key function?
            (contains? sequence-keys first-elem)
            (let [key-fn (get sequence-keys first-elem)]
              (apply key-fn
                     (map compiler
                          (rest type-sequence))))

            ;; ... is an enum declaration?
            ;; FIXME: Ensure rest of sequence is strings
            (= "enum:" first-elem)
            (apply s/enum (rest type-sequence))

            ;; ... is a conditional/one-of rule (depends on key of
            ;; first element)
            (= "conditional:" first-elem)
            (create-conditional context this-type-name (rest type-sequence))

            (= "recursive:" first-elem)
            (apply s/recursive (compile-type-definition context
                                                        this-type-name
                                                        (rest type-sequence)))))

     ;; Default: Just compile each element
     (mapv compiler type-sequence))))

(defmethod compile-type-def
  clojure.lang.APersistentMap
  [context this-type-name definition]
  (into {} (for [[k v] definition]
             [(keyword k) ;; ensure keys are keywords
              (compile-type-def context this-type-name v)])))
(prefer-method compile-type-def
               clojure.lang.APersistentMap
               clojure.lang.ISeq)


(defn invalidate-type-refs [context ref-list]
  (for [ref ref-list]
    (compile-type-definition context ref))
  [])

(defn invalidate-type [context type-name]
  (let [type-context (get @type-schemas context)]
    (send type-context dissoc type-name)))

(defn compile-type-definition [context type-name]
  (let [type-def (find-type-by-name context type-name)
        schema-def (compile-type-def context
                                     type-name
                                     (:definition type-def))]
    (bind-type-in-context context type-name schema-def)
    (if-let [ref-list (get-in @type-schema-references [context
                                                       type-name])]
      (send ref-list invalidate-type-refs))
    schema-def))


