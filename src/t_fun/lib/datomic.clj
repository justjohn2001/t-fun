(ns t-fun.lib.datomic
  (:require [datomic.client.api :as d]))

(def default-region "us-east-1")
(def default-db "locations")

(defn datomic-config
  ([system-name] (datomic-config system-name default-region))
  ([system-name region]
   {:server-type :ion
    :region region
    :system system-name
    :endpoint (format "http://entry.%s.%s.datomic.net:8182/" system-name region)}))

(defn get-conn
  ([stage] (get-conn stage default-db))
  ([stage db-name]
   (-> stage
       name
       datomic-config
       d/client
       (d/connect {:db-name db-name}))))
