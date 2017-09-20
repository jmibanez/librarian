(ns com.jmibanez.librarian.store-init
  "System init. Provides functions for ensuring core documents and types
  exist in the backing database."

  (:require [clj-uuid :as uuid]
            [com.jmibanez.librarian
             [query :as q]
             [store :as store]]))


(def ^:dynamic *init-transaction* (atom nil))


(defn create-system-doc
  ([id name doc]
   (create-system-doc id name store/root-type doc))
  ([id name type doc]
   {:id                  id
    :name                name
    :context             nil
    :type                type
    :state               :committed
    :document            doc}))

(defmacro ensure-system-document
  ([id name doc]
   `(ensure-system-document ~id ~name store/root-type ~doc))
  ([id name type doc]
   `(when (nil? (store/get-document-by-id nil ~id))
      (->> (create-system-doc ~id ~name ~type ~doc)
           (store/map->Document)
           (store/write-document! @*init-transaction*)))))

(defmacro ensure-system-schema [id name schema-def]
  `(when (nil? (store/get-document-by-id nil ~id))
     (->> (create-system-doc ~id ~name store/schema-type ~schema-def)
          (store/map->Document)
          (store/write-document! @*init-transaction*))))

(defn init-root-type []
  (ensure-system-document store/root-type
                          store/root-type-name
                          {:name "Root Document" :system "librarian1"}))


(defn init-schemas []

  ;; XXX This may change in the future to self-describe schema
  (ensure-system-document store/schema-type
                          store/schema-type-name
                          {:definition "::any"})

  (ensure-system-schema store/schema-state-type
                        store/schema-state-name
                        {:definition ["enum:" "open" "committed" "deprecated"]})


  ;; Query schemas
  (ensure-system-schema q/query-rule-type
                        q/query-rule-type-name
                        {:definition ["conditional:"
                                      ["all_of" {"all_of" [["recursive:" "QueryRule"]]}]
                                      ["any_of" {"any_of" [["recursive:" "QueryRule"]]}]
                                      ["match"  {"match"  "::string"
                                                 "with"   ["conditional:"
                                                           ["variable" "::string"]
                                                           [nil        "::any"]]}]]})
  (ensure-system-schema q/query-type
                        q/query-type-name
                        {:definition {:query {:name "::string"
                                              :type "::uuid"
                                              :rule "QueryRule"
                                              :sort ["maybe" "SortCondition"]}}}))

(defn do-init []
  (if-not (compare-and-set! *init-transaction*
                            nil (store/start-transaction! (uuid/null)))
    (throw (Exception. "Previous init transaction already initiated.")))

  (init-root-type)

  (init-schemas)

  (store/commit-transaction! @*init-transaction*)
  (compare-and-set! *init-transaction*
                    @*init-transaction* nil))
