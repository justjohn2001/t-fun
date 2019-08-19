(ns t-fun.lib.cast
  (:require [datomic.ion.cast :as cast]
            [clojure.tools.logging :as log]))

(defn alert [m]
  (try (cast/alert m)
       (catch Exception e
         (prn "ALERT - " m))))

(defn event [m]
  (try (cast/event m)
       (catch Exception e
         (prn "EVENT - " m))))

(defn dev [m]
  (try (cast/dev m)
       (catch Exception e
         (prn "DEV - " m))))
