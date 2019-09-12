(ns t-fun.core
  (:require [datomic.ion :as ion]
            [datomic.ion.cast :as cast]
            [clojure.string :as string]))

(def stage (future (keyword (or (get (System/getenv) "STAGE")
                                (get (ion/get-env) :env)
                                "development"))))

(defn echo
  [{:keys [input] :as params}]
  (string/join "\n" [(ion/get-app-info)
                     (ion/get-env)
                     (pr-str (or input params))]))

