(ns t-fun.unit.cloudsearch-load
  (:require [t-fun.cloudsearch-load :refer :all]
            [midje.sweet :as t :refer [=> contains anything just]]
            [datomic.ion :as ion]))

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
