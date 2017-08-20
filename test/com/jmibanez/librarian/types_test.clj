(ns com.jmibanez.librarian.types-test
  (:require [expectations :refer :all]

            [schema.core :as s]
            [com.jmibanez.librarian.types :as sut]
            [com.jmibanez.librarian.seeds :as seeds]))


(defmacro in-type-context
  [context & body]
  `(do
     (sut/create-type-context ~context)
     (sut/create-type-schema-references-for-context ~context)
     (try
       (do
         ~@body)
       (finally
         (do
           (sut/destroy-type-schema-references-for-context ~context)
           (sut/destroy-type-context ~context))))))





;; Expect basic JSON definition translates to its equivalent Schema def
(expect {:id s/Str
         :name s/Str
         :inner [{:name s/Str
                  :value s/Int}]}
        (in-type-context seeds/test-context
         (sut/compile-type-definition seeds/test-context seeds/test-type-name)))

;; Expect JSON definition with type reference compiles properly
(expect {:id s/Str
         :name s/Str
         :referred {:id s/Str
                    :name s/Str
                    :inner [{:name s/Str
                             :value s/Int}]}}
        (in-type-context seeds/test-context
         (sut/compile-type-definition seeds/test-context seeds/test-type-ref-name)))
