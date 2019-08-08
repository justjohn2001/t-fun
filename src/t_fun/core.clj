(ns t-fun.core
  (:require [datomic.ion :as ion]
            [datomic.ion.cast :as cast]
            [t-fun.infrastructure :as i]
            [clojure.string :as string]))

(def stage (future (keyword (or (get (System/getenv) "STAGE")
                                (get (ion/get-env) :env)
                                "development"))))

(defn echo
  [{:keys [input] :as params}]
  (string/join "\n" [@i/stack-error
                     (pr-str (or input params))]))

