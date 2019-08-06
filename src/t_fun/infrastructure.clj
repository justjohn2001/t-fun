(ns t-fun.infrastructure
  (:import java.util.UUID))

(def topology {})

(def stack (future (str (UUID/randomUUID) "\n" (pr-str topology))))
