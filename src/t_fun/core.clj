(ns t-fun.core
  (:require [datomic.ion :as ion]
            [datomic.ion.cast :as cast]
            [clojure.string :as string]))

(def stage
  (memoize
   (fn stage* []
     (keyword (or (get (System/getenv) "STAGE")
                  (get (ion/get-env) :env))))))

(defn echo
  [{:keys [input] :as params}]
  (string/join "\n" [(ion/get-app-info)
                     (ion/get-env)
                     (pr-str (or input params))]))

