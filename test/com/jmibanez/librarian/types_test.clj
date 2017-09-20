(ns com.jmibanez.librarian.types-test
  (:require [expectations :refer :all]

            [schema.core :as s]
            [com.jmibanez.librarian.types :as sut]
            [com.jmibanez.librarian.seeds :as seeds]))



;; Expect basic JSON definition translates to its equivalent Schema def
(expect {:id s/Str
         :name s/Str
         :inner [{:name s/Str
                  :value s/Int}]}
        (sut/with-type-context seeds/test-context
          (sut/compile-type-definition seeds/test-context
                                       seeds/test-type-name)))

;; Expect JSON definition with type reference compiles properly
(expect {:id s/Str
         :name s/Str
         :referred {:id s/Str
                    :name s/Str
                    :inner [{:name s/Str
                             :value s/Int}]}}
        (sut/with-type-context seeds/test-context
          (sut/compile-type-definition seeds/test-context
                                       seeds/test-type-ref-name)))

;; Fix handling of recursive types: Should parse and handle recursive
;; types
(def recursive-type-base {:id   s/Str
                          :name s/Str})

(expect recursive-type-base
        (in (sut/with-type-context seeds/test-context
              (sut/compile-type-definition seeds/test-context
                                           seeds/test-recursive-type-name))))

;; Should be able to validate a recursive type
(def test-recursive-type-value
  {:id    "foo"
   :name  "bar"
   :next  {:id    "baz"
           :name  "quux"
           :next  nil}})
(expect test-recursive-type-value
        (sut/with-type-context seeds/test-context
          (sut/validate-and-coerce-to-type seeds/test-context
                                           seeds/test-recursive-type-name
                                           test-recursive-type-value)))
