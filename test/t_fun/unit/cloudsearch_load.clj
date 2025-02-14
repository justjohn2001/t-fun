(ns t-fun.unit.cloudsearch-load
  (:require [t-fun.cloudsearch-load :refer :all :as cs-load]
            [t-fun.lib.datomic :as t-d]
            [midje.sweet :as t :refer [=> contains anything just]]
            [datomic.ion :as ion]))

(t/fact "datomic-config"
        (t-d/datomic-config :test) => map?)

(t/fact "make-resource-name"
        (make-resource-name "test") => "app-group-infrastructure-test"
        (provided
         (ion/get-app-info) => {:app-name "app" :deployment-group "group"}))

(def datomic-result {:place/id "abc123"
                     :iata/airport-code "CHO"
                     :place/display-name "Charlottesville, Virginia, USA"
                     :location/hotel-count 23
                     :geo/latitude -78.3
                     :geo/longitude 38.2
                     :place/name "Charlottesville"
                     :place/type "city"
                     :place/region {:region/code "VA"
                                    :region/name "Virginia"}
                     :place/country {:country/code "USA"
                                     :country/name "USA"}})

(t/facts
 (t/fact "datomic->aws"
         (datomic->aws datomic-result) => (contains {:type "add"
                                                     :id "abc123"
                                                     :fields (contains {:id "abc123"
                                                                        :full_name "Charlottesville, Virginia, USA"
                                                                        :hotel_count 23
                                                                        :latlng "-78.300000,38.200000"
                                                                        :name "Charlottesville"
                                                                        :place_type "city"
                                                                        :airport_code "CHO"
                                                                        :country_code "USA"
                                                                        :country_name "USA"
                                                                        :region_code "VA"
                                                                        :region "Virginia"})}))
 (t/fact "datomic->aws minimal"
         (datomic->aws (dissoc datomic-result
                               :iata/airport-code :place/region :place/country))
         => (contains {:type "add"
                       :id "abc123"
                       :fields (contains {:id "abc123"
                                          :full_name "Charlottesville, Virginia, USA"
                                          :hotel_count 23
                                          :latlng "-78.300000,38.200000"
                                          :name "Charlottesville"
                                          :place_type "city"})}))
 (t/fact "datomic->aws with no hotel count results in delete"
         (datomic->aws (dissoc datomic-result :location/hotel-count))
         => {:type "delete"
             :id "abc123"}
         (datomic->aws (assoc datomic-result :location/hotel-count 0))
         => {:type "delete"
             :id "abc123"}))

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
                 (expand-countries-and-regions ..conn.. [{:type :place :e 123 :tx 1 :t-id 1}
                                                         {:type :place :e 124 :tx 1 :t-id 2}])
                 => [[123 1] [124 2]])
         (t/fact "type :country expands"
                 (expand-countries-and-regions ..conn.. [{:type :country :e 123 :t-id 1 :tx 1}])
                 => [[456 1] [457 1]]
                 (provided (datomic.client.api/db ..conn..) => ..db..
                           (datomic.client.api/as-of ..db.. anything) => ..asof..
                           (datomic.client.api/q anything anything anything) => [[456 1] [457 1]]))
         (t/fact "type :region expands"
                 (expand-countries-and-regions ..conn.. [{:type :region :e 123 :t-id 1 :tx 1}])
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

(defn fake-reducer
  ([] {:pending {} :max-t 0})
  ([result] result)
  ([result [e t-val]]
   (cond-> result
     e (update-in [:pending e] (fnil max 0) t-val)
     t-val (update :max-t (fnil max 0) t-val))))

(t/facts "walk-transaction"
         (t/fact "runs"
                 (walk-transactions ..conn.. ..sqs-client.. 10 0) => {:max-t 10 :pending {1234 10}}
                 (provided (datomic-transactions ..conn.. anything)
                           => [[{:t 10
                                 :data [{:e 1234 :a 1 :v "val1" :tx 9999 :added true}]}]]))
         (t/fact "accumulates mmultiple"
                 (walk-transactions ..conn.. ..sqs-client.. 10 0) => {:max-t 10 :pending {1234 10 1235 10}}
                 (provided (datomic-transactions ..conn.. anything)
                           => [[{:t 10
                                 :data [{:e 1234 :a 1 :v "val1" :tx 9999 :added true}
                                        {:e 1235 :a 2 :v "val2" :tx 9999 :added true}]}]]))
         (t/fact "ignores undesired attributes"
                 (walk-transactions ..conn.. ..sqs-client.. 10 0) => {:max-t 10 :pending {1234 10 1235 10}}
                 (provided (entity-reducer ..conn.. ..sqs-client.. ..url.. anything) => fake-reducer
                           (datomic-transactions ..conn.. anything)
                           => [[{:t 10
                                 :data [{:e 1234 :a 1 :v "val1" :tx 9999 :added true}
                                        {:e 1235 :a 2 :v "val2" :tx 9999 :added true}
                                        {:e 1236 :a 3 :v "Attribute not desired" :tx 9999 :added true}]}]]))
         (t/fact "aggregates over multiple transactions"
                 (walk-transactions ..conn.. ..sqs-client.. 10 0) => {:max-t 11 :pending {1234 10 1235 11}}
                 (provided (datomic-transactions ..conn.. anything)
                           => [[{:t 10
                                 :data [{:e 1234 :a 1 :v "val1" :tx 9999 :added true}]}
                                {:t 11
                                 :data [{:e 1235 :a 2 :v "val2" :tx 1000 :added true}]}]]))
         (t/against-background (get-queue-url ..sqs-client.. anything) => ..url..
                               (get-attribute-ids ..conn..) => [{:db/id 1 :db/ident :place/id}
                                                                {:db/id 2 :db/identy :place/name}]
                               (entity-reducer ..conn.. ..sqs-client.. ..url.. anything) => fake-reducer))

(t/facts "queue-updates"
         (t/fact "runs"
                 (queue-updates {:input "{}"}) => #"Read through transaction"
                 (provided (datomic.ion.cast/event anything) => nil))
         (t/fact "input overrides defaults and get-tx-param"
                 (queue-updates {:input "{:start-tx 100 :timeout 10}"})
                 => #"transaction 100"
                 (provided (walk-transactions ..conn.. anything 100 10) => {:max-t 100 :sent 1 :deleted 0}
                           (datomic.ion.cast/event anything) => nil))
         (t/fact "exception is propagated out"
                 (queue-updates {:input "{}"}) => (t/throws Exception)
                 (provided (walk-transactions ..conn.. anything anything anything) t/=throws=> (Exception.)
                           (datomic.ion.cast/alert anything) => nil))

         (t/against-background (t-fun.core/stage) => :test
                               (datomic.client.api/client anything) => ..client..
                               (datomic.client.api/connect ..client.. anything) => ..conn..
                               (get-tx-param ..conn..) => {:param/int-value 10}
                               (walk-transactions ..conn.. anything 11 anything) => {:max-t 11 :sent 1 :deleted 0}
                               (set-tx-param ..conn.. anything) => nil))

(t/facts "upload-docs"
         (t/fact "runs and logs the count"
                 (upload-docs ..client.. [1 2 3 4])
                 => {:test "1"})
         (t/against-background (cognitect.aws.client.api/invoke ..client.. anything) => {"test" "1"}
                               (datomic.ion.cast/event (contains {::cs-load/count anything})) => nil))

(t/facts "load-locations-to-cloudsearch"
         (t/fact "processes deletes"
                 (load-locations-to-cloudsearch {:input "{\"Records\":[{\"body\": \"{:op :delete :ids [1, 2]}\"}]}"})
                 => "({:status \"success\"})")
         (t/fact "processes updates"
                 (load-locations-to-cloudsearch {:input "{\"Records\":[{\"body\": \"{:op :update :ids [1, 2]}\"}]}"})
                 => "({:status \"success\"})"
                 (provided (datomic.client.api/db ..client..) => ..db..
                           (location-details ..db.. anything) => [{:place/id "1234"} {:place/id "2345"}]))
         (t/fact "unknown ops are thrown"
                 (load-locations-to-cloudsearch {:input "{\"Records\":[{\"body\": \"{:op :unknown :ids [1, 2]}\"}]}"})
                 => (t/throws Exception)
                 (provided (datomic.ion.cast/alert anything) => nil))

         (t/against-background (t-fun.core/stage) => :test
                               (datomic.client.api/client anything) => ..client..
                               (datomic.client.api/connect anything anything) => ..client..
                               (datomic.ion.cast/event anything) => (t/two-of nil)
                               (upload-docs anything anything) => {:status "success"}))
