(ns t-fun.core
  (:require [datomic.ion :as ion]
            [datomic.ion.cast :as cast]
            [t-fun.infrastructure :as i]
            [clojure.string :as string]))

(defn echo
  [{:keys [input] :as params}]
  (string/join "\n" [@i/stack
                     (pr-str (or input params))]))
