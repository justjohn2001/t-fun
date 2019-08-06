(ns t-fun.infrastructure
  (:import java.util.UUID))

(def topology {:version "1.1"})

(def stack (future (str (UUID/randomUUID) "\n" (pr-str topology))))
