(ns expectations-options
  (:require [taoensso.timbre :as timbre]
            [com.jmibanez.librarian
             [indexer :as idx]
             [store :as store]
             [migrations :refer [do-migrate]]
             [seeds :as seed]]
            [schema.core :as s]
            [mount.core :as mount]))

(timbre/refer-timbre)

(defn boot-mounts
  {:expectations-options :before-run}
  []
  (mount/start-without #'idx/*indexer-pool*
                       #'idx/indexer-chan
                       #'store/gc-reaper)
  (do-migrate))

(defn test-context
  {:expectations-options :in-context}
  [test]
  (do
    (s/set-fn-validation! true)
    (try
      (seed/fixture test)
      (finally (s/set-fn-validation! false)))))
