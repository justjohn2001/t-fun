(ns t-fun.cloudsearch-load
  (:require [cheshire.core :as json]
            [clojure.edn :as edn]
            [clojure.set :as set]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [cognitect.aws.client.api :as aws]
            [datomic.client.api :as d]
            [datomic.client.api.async :as da]

            [clojure.walk :as walk]
            [datomic.ion.cast :as cast]
            [datomic.ion :as ion]
            [t-fun.core :as core]
            [t-fun.infrastructure :as inf])
  (:import (java.time Instant)
           (java.io ByteArrayInputStream)))

(def region "us-east-1")

(defn datomic-config
  [stage]
  (let [stage-str (name stage)]
    {:server-type :ion
     :region region
     :system (format "datomic-cloud-%s" stage-str)
     :endpoint (format "http://dc-%s-compute-main.c0pt3r.local:8182/" stage-str)}))

(defn make-resource-name
  [s]
  (let [{:keys [app-name deployment-group]} (ion/get-app-info)]
    (format "%s-%s-infrastructure-%s"
            app-name
            deployment-group
            s)))

(def attribute->entity
  {nil :place
   :rk.location/hotel-count :place
   :rk.place/id :place
   :rk.place/region :place
   :rk.place/name :place
   :rk.place/type :place
   :rk.place/country :place
   :iata/airport-code :place
   :rk.place/display-name :place
   :rk.geo/latitude :place
   :rk.geo/longitude :place

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

;;; A version of this function also exists in apij.models.location to build queries
;;; against the fields. Please keep both in sync.
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
           alt-id]
    {region-code :rk.region/code region-name :rk.region/name} :rk.place/region
    {country-code :rk.country/code country-name :rk.country/name} :rk.place/country
    :as m}]
  (if (or (nil? hotel-count) (zero? hotel-count))
    {:type "delete"
     :id (string/replace (or alt-id id) " " "-")}
    {:type "add"
     :id (string/replace (or alt-id id) " " "-")
     :fields (cond-> {:tid id
                      :full_name display-name
                      :full_name_starts_with (-> display-name
                                                 make-starts-with
                                                 expand-n-gram)
                      :full_name_starts_with_anywhere (-> display-name
                                                          (make-starts-with #" ")
                                                          (->> (reduce (fn [coll i]
                                                                         (apply conj coll (expand-n-gram i)))
                                                                       (sorted-set))))

                      :hotel_count hotel-count
                      :latlng (format "%.6f,%.6f" latitude longitude)
                      :name name
                      :place_type type
                      :is_primary (nil? alt-id)}
               airport-code (assoc :airport_code airport-code)
               country-code (assoc :country_code country-code)
               country-name (assoc :country_name country-name)
               region-code (assoc :region_code region-code)
               region-name (assoc :region region-name))}))

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

(defn datomic-transactions
  [dt-conn tx-id]
  (cast/dev {:msg "Loading tx"
             ::tx-id tx-id})
  (let [tx (d/tx-range dt-conn
                       {:start tx-id
                        :end (inc tx-id)})]
    (when (seq tx)
      (cons (seq tx) (lazy-seq (datomic-transactions dt-conn (inc tx-id)))))))


(defn take-while*
  "Like take-while but includes the first element where pred evaluates to false."
  {:added "1.0"
   :static true}
  ([pred]
   (fn [rf]
     (fn
       ([] (rf))
       ([result] (rf result))
       ([result input]
        (let [new-result (rf result input)]
          (if (pred input)
            new-result
            (reduced new-result)))))))
  ([pred coll]
   (lazy-seq
    (when-let [s (seq coll)]
      (let [f (first s)]
        (if (pred f)
          (cons f (take-while pred (rest s)))
          f))))))

(defn find-entities
  [attr-to-find datoms]
  (let [wanted-entities (filter #(contains? attr-to-find (:a %)) datoms)]
    (if (seq wanted-entities)
      (sort-by :added wanted-entities)
      [{}])))

(defn expand-countries-and-regions
  [dt-conn [[entity-type _ tx] :as l]]
  (let [e-t (mapv (juxt second #(nth % 3)) l)]
    (case entity-type
      :place e-t
      :country (d/q '[:find ?e ?t-id
                      :in $ [[?country-e ?t-id]]
                      :where [?e :rk.place/country ?country-e]]
                    (-> dt-conn d/db (d/as-of tx))
                    e-t)
      :region (d/q '[:find ?e ?t-id
                     :in $ [[?region-e ?t-id]]
                     :where [?e :rk.place/region ?region-e]]
                   (-> dt-conn d/db (d/as-of tx))
                   e-t))))

(defn sqs-send
  [sqs-client sqs-url op s]
  (cast/event {:msg "Sending batch to sqs"
               ::sqs-url sqs-url})
  (aws/invoke sqs-client {:op :SendMessage
                          :request {:QueueUrl sqs-url
                                    :MessageBody (format "{:op %s :ids %s}" op s)}}))

(defn entity-reducer
  [dt-conn sqs-client sqs-url start-tx]
  (fn entity-reducer*
    ([] {:pending {} :max-t start-tx})
    ([{:keys [pending max-t]}]
     (merge {:max-t max-t}
            (when (not-empty pending)
              (let [id-map (into {}
                                 (d/q '[:find ?e ?id
                                        :in $ [?e ...]
                                        :where [?e :rk.place/id ?id]]
                                      (d/db dt-conn)
                                      (keys pending)))
                    updates (vals id-map)
                    deletes (apply dissoc pending (keys id-map))
                    send-result (into []
                                      (comp (partition-all 1000)
                                            (map pr-str)
                                            (map (partial sqs-send sqs-client sqs-url :update)))
                                      updates)
                    delete-result (into []
                                        (comp (mapcat (fn [[tx v]]
                                                        (d/q '[:find ?id
                                                               :in $ [?e ...]
                                                               :where [?e :rk.place/id ?id]]
                                                             (-> dt-conn d/db (d/as-of (dec tx)))
                                                             v)))
                                              (map pr-str)
                                              (map (partial sqs-send sqs-client sqs-url :delete)))
                                        (group-by deletes (keys deletes)))]
                {:sent (count updates) :send-results send-result
                 :deleted (count deletes) :delete-results delete-result}))))
    ([acc [e t-val]]
     (cond-> acc
       e (update-in [:pending e] (fnil max 0) t-val)
       t-val (update :max-t max t-val)))))

(defn type-and-tx [e]
  [(first e) (nth e 2)])

(defn walk-transactions
  [dt-conn start-tx timeout]
  (let [sqs-client (aws/client {:api :sqs})
        queue-name (inf/make-cloudsearch-load-queue-name)
        {sqs-url :QueueUrl :as sqs-url-response} (aws/invoke sqs-client
                                                             {:op :GetQueueUrl
                                                              :request {:QueueName queue-name}})
        stop-time (+ (System/currentTimeMillis) timeout)
        attribute-ids (get-attribute-ids dt-conn)
        id->ident (into {} (map (juxt :db/id :db/ident)
                                attribute-ids))
        location-attributes (into #{} (map :db/id attribute-ids))]
    (if (nil? sqs-url)
      (cast/alert {:msg "Error getting sqs-url"
                   ::queue-name queue-name
                   ::response sqs-url-response
                   ::section "walk-transaction"})
      (transduce (comp (take-while* (fn [_] (< (System/currentTimeMillis) stop-time)))
                       cat
                       (map (juxt :t #(find-entities location-attributes (:data %))))
                       (mapcat (fn [[t-id d]]
                                 (sort-by type-and-tx
                                          (map (fn [{:keys [e a tx]}]
                                                 (vector (-> a id->ident attribute->entity)
                                                         e tx t-id))
                                               d))))
                       (partition-by type-and-tx)
                       (mapcat (partial expand-countries-and-regions dt-conn)))
                 (entity-reducer dt-conn sqs-client sqs-url start-tx)
                 (datomic-transactions dt-conn start-tx)))))

(def tx-param-name "Last tx-id queued to cloudsearch-locations")

(defn queue-updates
  [{:keys [input]}]
  (try
    (let [options (try (edn/read-string input) (catch Exception e {}))
          dt-conn (-> (datomic-config @core/stage)
                      d/client
                      (d/connect {:db-name "rk"}))
          {e :db/id last-tx :rk.param/int-value
           :or {last-tx 0}
           :as r} (ffirst (d/q '[:find (pull ?e [:db/id :rk.param/int-value])
                                 :in $ ?name
                                 :where [?e :rk.param/name ?name]]
                               (d/db dt-conn)
                               tx-param-name))
          {:keys [max-t sent deleted]
           :or {sent 0 deleted 0}} (walk-transactions dt-conn
                                                      (or (:start-tx options) (inc last-tx))
                                                      (or (:timeout options) 120000))]
      (d/transact dt-conn
                  {:tx-data [{:db/id "param"
                              :rk.param/name tx-param-name
                              :rk.param/int-value max-t}]})
      (let [msg (format "Read through transaction %d. Sent %d updates, %d deletes." max-t sent deleted)]
        (cast/event {:msg msg
                     ::section "QUEUE-UPDATES"
                     ::app "t-fun"})
        msg))
    (catch Exception e
      (cast/alert {:msg "Exception running queue-updates"
                   ::exception e})
      (throw e))))

(defn upload-docs
  [client rows]
  (when (seq rows)
    (cast/event {:msg (format "Uploading to cloudsearch batch of %d" (count rows))
                 ::section "LOAD-LOCATIONS"})
    (let [docs (-> rows
                   (json/generate-string {:escape-non-ascii true})
                   .getBytes)
          result (walk/keywordize-keys
                  (aws/invoke client {:op :UploadDocuments
                                      :request {:contentType "application/json"
                                                :documents docs}}))]
      (if (= (:status result) "error")
        (throw (ex-info "Error sending batch" {:batch rows
                                               :result result}))
        result))))

(defn location-details
  [db ids]
  (map first (d/q '[:find (pull ?e [:db/id
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
                    :where [?e :rk.place/id ?id]]
                  db
                  ids)))

(def locations-doc-client
  (delay (let [cs-client (aws/client {:api :cloudsearch})
               domain-name (format "locations-%s" (name @core/stage))
               domain-status-list (aws/invoke cs-client {:op :DescribeDomains :request {:DomainNames [domain-name]}})
               endpoint (get-in domain-status-list [:DomainStatusList 0 :DocService :Endpoint])]
           (cast/event {:msg "cloudsearch client details"
                        ::section "LOAD-LOCATIONS"
                        ::details {:domain-name domain-name
                                   :domain-status-list domain-status-list
                                   :endpoint endpoint}})
           (aws/client {:api :cloudsearchdomain :endpoint-override endpoint}))))


(defn load-locations-to-cloudsearch
  [{:keys [input]}]
  (try
    (let [config (datomic-config @core/stage)
          dt-conn (-> config
                      d/client
                      (d/connect {:db-name "rk"}))
          records (-> input
                      (json/parse-string true)
                      :Records)
          doc-client @locations-doc-client
          result (pr-str
                  (sequence (map (fn [record]
                                   (let [{:keys [op ids] :as request} (-> record :body edn/read-string)]
                                     (case op
                                       :delete (do (cast/event {:msg (format "processing batch of %d deletes" (count ids))
                                                                ::section "LOAD-LOCATIONS"
                                                                ::deletes ids})
                                                   (upload-docs doc-client
                                                                (sequence (comp (mapcat #(vector % (str % "-region_code")))
                                                                                (map #(hash-map :type "delete" :id %)))
                                                                          ids)))
                                       :update (do (cast/event {:msg (format "processing batch of %d updates" (count ids))
                                                                ::section "LOAD-LOCATIONS"
                                                                ::updates ids})
                                                   (let [location-data (location-details (d/db dt-conn) ids)]
                                                     (->> location-data
                                                          (into {}
                                                                (comp
                                                                 (mapcat #(cond-> [%]
                                                                            (get-in % [:rk.place/region :rk.region/code]) (conj (make-alt-location %))))
                                                                 (map (juxt #(or (:alt-id %) (:rk.place/id %)) datomic->aws))))
                                                          vals
                                                          (upload-docs doc-client))))
                                       (throw (ex-info (format "Unknown operation - %s" op) {:request request}))))))
                            records))]
      (cast/event {:msg "load-location-to-cloudsearch done"
                   ::section "LOAD-LOCATIONS"
                   ::result result})
      result)
    (catch Exception e
      (cast/alert {:msg "Exception in load-locations-to-cloudsearch"
                   ::exception e})
      (throw e))))
