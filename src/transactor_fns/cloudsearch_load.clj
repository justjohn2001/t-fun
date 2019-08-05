(ns transactor-fns.cloudsearch-load
  (:require [cheshire.core :as json]
            [clojure.core.async :as a]
            [clojure.set :as set]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [cognitect.aws.client.api :as aws]
            [datomic.client.api :as d]
            [datomic.client.api.async :as da]

            [clojure.walk :as walk]
            [datomic.ion.cast :as cast]
            [datomic.ion :as ion])
  (:import (java.time Instant)
           (java.io ByteArrayInputStream)))

(def stage (delay (keyword (or (get (System/getenv) "STAGE")
                               (get (ion/get-env) :env)
                               "test"))))

(defn datomic-config
  [stage region]
  (let [stage-str (name stage)]
    {:server-type :ion
     :region region
     :system (format "datomic-cloud-%s" stage-str)
     :endpoint (format "http://entry.datomic-cloud-%s.us-east-1.datomic.net:8182/" stage-str)}))

(def attribute->entity
  {:rk.location/hotel-count :place
   :rk.place/id :place
   :rk.place/region :place
   :rk.place/name :place
   :rk.place/type :place
   :rk.place/country :place
   :iata/airport-code :place
   :rk.place/display-name :place
   :rk.geo/lat :place
   :rk.geo/lng :place

   :rk.country/code :country
   :rk.country/name :country

   :rk.region/name :region
   :rk.region/code :region})

(defn expand-n-gram
  ([s] (expand-n-gram (sorted-set) [(first s)] (rest s)))
  ([result prefix suffix]
   (if (seq suffix)
     (recur (conj result (apply str prefix))
            (conj prefix (first suffix))
            (rest suffix))
     (conj result (apply str prefix)))))

;;; Mirror in apij.models.location
(defn make-starts-with
  [s & [split-re]]
  (-> s
      string/lower-case
      (string/replace #"[^a-z0-9 ]" " ")
      (string/replace #"\s+" " ")
      (string/replace #"^\s|\s$" "")
      (cond->
          split-re (string/split split-re))))

(defn datomic->aws
  [{:keys [rk.place/id iata/airport-code
           rk.place/display-name rk.location/hotel-count rk.geo/latitude rk.geo/longitude
           rk.place/name rk.place/type
           is_primary alt-id]
    {region-code :rk.region/code region-name :rk.region/name} :rk.place/region
    {country-code :rk.country/code country-name :rk.country/name} :rk.place/country
    :as m}]
  {:type "add"
   :id (string/replace (or alt-id id) " " "-")
   :fields (cond-> {:tid id
                    :country_code country-code
                    :country_name country-name
                    :full_name display-name
                    :full_name_starts_with (-> display-name
                                               make-starts-with
                                               expand-n-gram)
                    :full_name_starts_with_anywhere (-> display-name
                                                        (make-starts-with #" ")
                                                        (->> (reduce (fn [coll i]
                                                                       (apply conj coll (expand-n-gram i)))
                                                                     (sorted-set))))

                    :hotel_count (or hotel-count 0)
                    :latlng (format "%.6f,%.6f" latitude longitude)
                    :name name
                    :place_type type
                    :is_primary ((fnil identity true) is_primary)}
             airport-code (assoc :airport_code airport-code)
             region-code (assoc :region_code region-code)
             region-name (assoc :region region-name))})

(defn- make-alt-location
  [{{:keys [rk.region/code rk.region/name]} :rk.place/region :as loc}]
  (-> loc
      (assoc :alt-id (str (:rk.place/id loc) "-region_code"))
      (update :rk.place/display-name
              #(string/replace %
                               (format ", %s, " (:name loc))
                               (format ", %s, " (:code loc))))))

(defn get-attribute-ids
  [dt-conn]
  (map first
       (d/q '[:find (pull ?e [:db/id :db/ident])
              :in $ [?ident ...]
              :where [?e :db/ident ?ident]]
            (d/db dt-conn)
            (keys attribute->entity))))

(def mem-attribute-ids (memoize get-attribute-ids))

(def dt-conn (-> (datomic-config @stage "us-east-1")
                 d/client
                 (d/connect {:db-name "rk"})))

(defn retrieve-transaction
  [dt-conn start]
  (let [start-time (Instant/now)
        location-attributes (into #{} (map :db/id (mem-attribute-ids dt-conn)))
        it (d/tx-range dt-conn {:start start
                                :end (inc start)
                                :timeout 120000})]
    (when (seq it)
      (sort-by :added
               (transduce (comp (map :tx-data)
                                cat
                                (filter #(location-attributes
                                          (:a %))))
                          conj
                          []
                          it)))))

(defn walk-transactions
  ([dt-conn start] (walk-transactions dt-conn start 645))
  ([dt-conn start end]
   (loop [i start
          updates {}]
     (cast/event {:msg (format "looking at tx %d" i)})
     (if (and end (> i end))
       {:last-tx (dec i) :updates updates}
       (let [id->ident (into {}
                             (map (juxt :db/id :db/ident)
                                  (mem-attribute-ids dt-conn)))
             result (try (retrieve-transaction dt-conn i)
                         (catch Exception e
                           [e]))]
         (cond
           (seq result) (let [updates' (reduce (fn [m {:keys [e a] :as v}]
                                                 (update m (-> a
                                                               id->ident
                                                               attribute->entity)
                                                         #((fnil conj (sorted-set)) % e)))
                                               updates
                                               result)]
                          (recur (inc i) updates'))
           (nil? result) {:last-tx (dec i) :updates updates}
           :else (do
                   (when (zero? (mod i 100))
                     (prn i))
                   (recur (inc i) updates))))))))

(defn all-updates
  [dt-conn start]
  (let [{{:keys [country region place] :as updates} :updates :as results} (walk-transactions dt-conn start)
        db (d/db dt-conn)
        country-places (set (map first (d/q '{:find [?e]
                                              :in [$ [?country ...]]
                                              :where [[?e :rk.place/country ?country]]}
                                            db
                                            (or (seq country) []))))
        region-places (set (map first (d/q '[:find ?e
                                             :in $ [?region ...]
                                             :where [?e :rk.place/region ?region]]
                                           db
                                           (or (seq region) []))))]
    (assoc results :updates (set/union place country-places region-places))))

(defn queue-updates
  [dt-conn stage start]
  (let [sqs-client (aws/client {:api :sqs})
        queue-url (aws/invoke sqs-client {:op :ListQueues
                                          :request {:QueueNamePrefix (format "cloudsearch-locations-%s" stage)}})])
  (let [cs-client (aws/client {:api :cloudsearch})
        domain-name (format "locations-%s" (name stage))
        domain-status-list (aws/invoke cs-client {:op :DescribeDomains :request {:DomainNames [domain-name]}})
        domain-status (get-in domain-status-list [:DomainStatusList 0])]
    (cond
      (:Processing domain-status) (log/warn "Domain is currently processing")
      (:RequiresIndexDocuments domain-status) (log/warn "Domain requires indexing")
      :else (let [endpoint (get-in domain-status [:DocService :Endpoint])
                  doc-client (aws/client {:api :cloudsearchdomain :endpoint-override endpoint})
                  {:keys [updates last-tx]} (all-updates dt-conn start)
                  db (d/db dt-conn)])
      )))

(defn upload-docs
  [client rows]
  (log/info "Starting batch of" (count rows))
  (let [docs (-> rows
                 (json/generate-string {:escape-non-ascii true})
                 .getBytes)
        {:keys [status adds] :as result} (walk/keywordize-keys
                                          (aws/invoke client {:op :UploadDocuments
                                                              :request {:contentType "application/json"
                                                                        :documents docs}}))]
    (if (= status "error")
      (throw (ex-info "Error sending batch" {:batch rows
                                             :result result}))
      result)))

#_(doseq [batch (partition-all 1000 updates)]
     (log/infof "Processing batch of %d updates" (count batch))
     (let [deletes (into {}
                         (comp (map first)
                               (map #(let [id (string/replace % " " "-")] [id {:type "delete" :id id}])))
                         (d/q '[:find ?id
                                :in $ [?e ...]
                                :where [?e :rk.place/id ?id]]
                              (d/history db)
                              batch))
           location-data (map first (d/q '[:find (pull ?id [:db/id
                                                            :rk.place/id
                                                            :iata/airport-code
                                                            :rk.place/display-name
                                                            :rk.location/hotel-count
                                                            :rk.geo/latitude
                                                            :rk.geo/longitude
                                                            :rk.place/name
                                                            :rk.place/type

                                                            {:rk.place/country [:rk.country/code
                                                                                :rk.country/name]}
                                                            {:rk.place/region [:rk.region/name
                                                                               :rk.region/code]}
                                                            ])
                                           :in $ [?id ...]
                                           :where [?id :rk.place/id _]]
                                         db
                                         batch))]
       (-> deletes
           (merge (into {}
                        (comp
                         (mapcat #(cond-> [%]
                                    (get-in % [:rk.place/region :rk.region/code])
                                    (conj (make-alt-location %))))
                         (map (juxt #(or (:alt-id %) (:rk.place/id %)) datomic->aws)))
                        location-data))
           vals
           (->> (upload-docs doc-client)))))
