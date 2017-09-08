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
