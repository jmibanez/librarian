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

(defmulti canonical-representation
  (fn [o]
    (type o)))

(defmethod canonical-representation
  :default
  [o] o)

(defmethod canonical-representation
  clojure.lang.PersistentVector
  [v]
  (for [e v]
    (canonical-representation e)))

(defmethod canonical-representation
  clojure.lang.APersistentMap
  [o]
  (into (sorted-map)
        (for [[k v] o]
          [k (canonical-representation v)])))
