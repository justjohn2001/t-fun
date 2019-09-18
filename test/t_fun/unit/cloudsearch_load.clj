(ns t-fun.unit.cloudsearch-load
  (:require [t-fun.cloudsearch-load :refer :all]
            [midje.sweet :as t :refer [=> contains anything just]]
            [datomic.ion :as ion]
            [datomic.client.api :as d]))

(t/fact "datomic-config"
        (datomic-config :test) => map?)

(t/fact "make-resource-name"
        (make-resource-name "test") => "app-group-infrastructure-test"
        (provided
         (ion/get-app-info) => {:app-name "app" :deployment-group "group"}))

(t/fact "expand-n-gram"
        (expand-n-gram "test") => #{"t" "te" "tes" "test"})

(t/facts
 (t/fact "make-starts-with"
         (make-starts-with "Te$+  Runs ") => "te runs")
 (t/fact "make-starts-with including a regex"
         (make-starts-with "+est  rUn$" #"\s") => ["est" "run"]))

(def datomic-result {:rk.place/id "abc123"
                     :iata/airport-code "CHO"
                     :rk.place/display-name "Charlottesville, Virginia, USA"
                     :rk.location/hotel-count 23
                     :rk.geo/latitude -78.3
                     :rk.geo/longitude 38.2
                     :rk.place/name "Charlottesville"
                     :rk.place/type "city"
                     :alt-id "abc-123-alt"
                     :rk.place/region {:rk.region/code "VA"
                                       :rk.region/name "Virginia"}
                     :rk.place/country {:rk.country/code "USA"
                                        :rk.country/name "USA"}})

(t/facts
 (t/fact "datomic->aws"
         (datomic->aws datomic-result) => (contains {:type "add"
                                                     :id "abc-123-alt"
                                                     :fields (contains {:tid "abc123"
                                                                        :full_name "Charlottesville, Virginia, USA"
                                                                        :full_name_starts_with (contains ["c" "charlottesville virginia usa"])
                                                                        :full_name_starts_with_anywhere (contains ["charlottesville" "virginia" "usa"])
                                                                        :hotel_count 23
                                                                        :latlng "-78.300000,38.200000"
                                                                        :name "Charlottesville"
                                                                        :place_type "city"
                                                                        :is_primary "false"
                                                                        :airport_code "CHO"
                                                                        :country_code "USA"
                                                                        :country_name "USA"
                                                                        :region_code "VA"
                                                                        :region "Virginia"})}))
 (t/fact "datomic->aws minimal"
         (datomic->aws (dissoc datomic-result
                               :alt-id :iata/airport-code :rk.place/region :rk.place/country))
         => (contains {:type "add"
                       :id "abc123"
                       :fields (contains {:tid "abc123"
                                          :full_name "Charlottesville, Virginia, USA"
                                          :full_name_starts_with (contains ["c" "charlottesville virginia usa"])
                                          :full_name_starts_with_anywhere (contains ["charlottesville" "virginia" "usa"])
                                          :hotel_count 23
                                          :latlng "-78.300000,38.200000"
                                          :name "Charlottesville"
                                          :place_type "city"
                                          :is_primary "true"})}))
 (t/fact "datomic->aws with no hotel count results in delete"
         (datomic->aws (dissoc datomic-result :rk.location/hotel-count :alt-id))
         => {:type "delete"
             :id "abc123"}
         (datomic->aws (assoc datomic-result :rk.location/hotel-count 0))
         => {:type "delete"
             :id "abc-123-alt"}))

(t/fact "make-alt-location replaces display-name and adds alt-id"
        (make-alt-location (dissoc datomic-result :alt-id))
        => (contains {:alt-id "abc123-region_code"
                      :rk.place/display-name "Charlottesville, VA, USA"}))

(t/fact "datomic-transactions is lazy"
        (take 1 (datomic-transactions ..conn.. 0)) => (t/one-of [1])
        (provided (datomic.client.api/tx-range ..conn.. anything) => [1])
        (take 5 (datomic-transactions ..conn.. 0)) => (t/five-of [1])
        (provided (datomic.client.api/tx-range ..conn.. anything) => [1] :times 5))

(t/fact "take-while* includes the first false element"
        (sequence (take-while* even?) [2 4 5 6 7]) => [2 4 5]
        (take-while* even? [2 4 5 6 7]) => [2 4 5])

(t/facts "expand-counties-and-regions"
         (t/fact "type :place not expanded"
                 (expand-countries-and-regions ..conn.. [[:place 123 1 1] [:place 124 1 2]])
                 => [[123 1] [124 2]])
         (t/fact "type :country expands"
                 (expand-countries-and-regions ..conn.. [[:country 123 1 1]])
                 => [[456 1] [457 1]]
                 (provided (datomic.client.api/db ..conn..) => ..db..
                           (datomic.client.api/as-of ..db.. anything) => ..asof..
                           (datomic.client.api/q anything anything anything) => [[456 1] [457 1]]))
         (t/fact "type :region expands"
                 (expand-countries-and-regions ..conn.. [[:region 123 1 1]])
                 => [[456 1] [457 1]]
                 (provided (datomic.client.api/db ..conn..) => ..db..
                           (datomic.client.api/as-of ..db.. anything) => ..asof..
                           (datomic.client.api/q anything anything anything) => [[456 1] [457 1]])))

(t/facts "entity-reducer"
         (t/fact "0 arity returns default map"
                 ((entity-reducer ..conn.. ..sqsclient.. "url" 1)) => map?)
         (t/fact "2 arity accumulates pending"
                 ((entity-reducer ..conn.. ..sqsclient.. "url" 1) {:pending {} :max-t 1} [123 6])
                 => {:pending {123 6} :max-t 6})
         (t/fact "2 arity accumulates t-values for entities"
                 ((entity-reducer ..conn.. ..sqsclient.. "url" 1) {:pending {123 3} :max-t 3} [123 6])
                 => {:pending {123 6} :max-t 6})
         ; TODO - test 1 arity version. But it has many side effects.
         )

(t/facts "walk-transaction"
         (t/fact "runs"
                 (walk-transactions ..conn.. 10 0) => {}
                 (provided (cognitect.aws.client.api/client {:api :sqs}) => ..cs-client..
                           (get-queue-url ..cs-client.. anything) => ..url..
                           (get-attribute-ids ..conn..) => [{:db/id 1 :db/ident :rk.place/id}
                                                            {:db/id 2 :db/identy :rk.place/name}]
                           (entity-reducer ..conn.. ..cs-client.. ..url.. anything)
                           => (fn ([] {:pending {} :t-val 0})
                                ([acc] acc)
                                ([acc [e t-val]]
                                 (cond-> acc
                                   e (update-in [:pending e] (fnil max 0) t-val)
                                   t-val (update :max-t max t-val))))
                           (datomic-transactions ..conn.. anything)
                           => [{:t 10
                                :data [{:e 1234
                                        :a 1
                                        :v "val1"
                                        :tx 9999
                                        :added true
                                        }]}])))
