(ns com.jmibanez.librarian.core-schema
  (:require [schema.core :as s]))

(def Id s/Uuid)

;; Contexts are UUIDs
(def Context (s/maybe s/Uuid))
