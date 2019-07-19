(ns transactor-fns.core
  (:require [datomic.ion :as ion]
            [datomic.ion.cast :as cast]))

(defn echo
  [{:keys [input] :as params}]
  (or input params))
