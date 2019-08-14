(ns t-fun.lib.cast
  (:require [datomic.ion.cast :as cast]))

(defn alert [m]
  (try (cast/alert m)
       (catch Exception e
         (prn "ALERT - " m))))

(defn event [m]
  (try (cast/event m)
       (catch Exception e
         (prn "EVENT - " m))))

