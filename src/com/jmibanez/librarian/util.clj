(ns com.jmibanez.librarian.util
  (:require [clojure.core.cache :as cache]
            [taoensso.timbre :as timbre]))

(timbre/refer-timbre)

(defn cache-result [[c cache-key] result]
  (dosync
   (if (cache/has? @c cache-key)
     (swap! c cache/hit cache-key)
     (swap! c cache/miss cache-key result))
   (cache/lookup @c cache-key)))


(defmacro defcacheable [name args cache-factory & body]
  (let [cache-name (gensym name)]
    `(do
       (def ~cache-name (atom ~cache-factory))
       (defn ~name ~args
         (cache-result [~cache-name ~args]
                       ~@body)))))
