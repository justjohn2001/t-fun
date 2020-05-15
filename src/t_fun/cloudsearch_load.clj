(ns t-fun.cloudsearch-load
  (:require [cheshire.core :as json]
            [clojure.edn :as edn]
            [clojure.string :as string]
            [clojure.walk :as walk]
            [cognitect.aws.client.api :as aws]
            [datomic.client.api :as d]
            [datomic.ion :as ion]
            [datomic.ion.cast :as cast]
            [t-fun.core :as core]
            [t-fun.infrastructure :as inf]
            [t-fun.lib.datomic :as lib.d]))

(defn make-resource-name
  [s]
  (let [{:keys [app-name deployment-group]} (ion/get-app-info)]
    (format "%s-%s-infrastructure-%s"
            app-name
            deployment-group
            s)))

(def attribute->entity
  {nil :place
   :location/hotel-count :place
   :place/id :place
   :place/region :place
   :place/name :place
   :place/type :place
   :place/country :place
   :iata/airport-code :place
   :place/display-name :place
   :geo/latitude :place
   :geo/longitude :place

   :country/code :country
   :country/name :country

   :region/name :region
   :region/code :region})

(defn datomic->aws
  [{:keys [place/id iata/airport-code
           place/display-name location/hotel-count geo/latitude geo/longitude
           place/name place/type]
    {region-code :region/code region-name :region/name} :place/region
    {country-code :country/code country-name :country/name} :place/country
    :as m}]
  (if (or (nil? hotel-count) (zero? hotel-count))
    {:type "delete"
     :id (string/replace id " " "-")}
    {:type "add"
     :id (string/replace id " " "-")
     :fields (cond-> {:id id
                      :full_name display-name
                      :hotel_count hotel-count
                      :latlng (format "%.6f,%.6f" latitude longitude)
                      :name name
                      :place_type type}
               airport-code (assoc :airport_code airport-code)
               country-code (assoc :country_code country-code)
               country-name (assoc :country_name country-name)
               region-code (assoc :region_code region-code)
               region-name (assoc :region region-name))}))

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
  (let [tx (d/tx-range dt-conn
                       {:start tx-id
                        :end (inc tx-id)})]
    (when (seq tx)
      (cast/dev {:msg "Reading tx" ::tx-id tx-id})
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
          (cons f (take-while* pred (rest s)))
          [f]))))))

(defn- find-entities
  [attr-to-find datoms]
  (let [wanted-entities (filter #(contains? attr-to-find (:a %)) datoms)]
    (if (seq wanted-entities)
      (sort-by :added wanted-entities)
      [{}])))

(defn expand-countries-and-regions
  [dt-conn [{:keys [type tx]} :as l]] ;; this assumes that all the items in the list have the same entity-type and tx value
  (let [e-t (mapv (juxt :e :t-id) l)]
    (case type
      :place e-t
      :country (d/q '[:find ?e ?t-id
                      :in $ [[?country-e ?t-id]]
                      :where [?e :place/country ?country-e]]
                    (-> dt-conn d/db (d/as-of tx))
                    e-t)
      :region (d/q '[:find ?e ?t-id
                     :in $ [[?region-e ?t-id]]
                     :where [?e :place/region ?region-e]]
                   (-> dt-conn d/db (d/as-of tx))
                   e-t))))

(defn- sqs-send
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
              (let [changed-ids (map first
                                     (d/q '[:find ?id
                                            :in $ [?e ...]
                                            :where [?e :place/id ?id]]
                                          (d/history (d/db dt-conn))
                                          (keys pending)))
                    send-result (into []
                                      (comp (partition-all 1000)
                                            (map pr-str)
                                            (map (partial sqs-send sqs-client sqs-url :update)))
                                      changed-ids)]
                {:sent (count changed-ids) :send-results send-result}))))
    ([acc [e t-val]]
     (cond-> acc
       e (update-in [:pending e] (fnil max 0) t-val)
       t-val (update :max-t (fnil max 0) t-val)))))

(def tx-and-type
  (juxt :tx :type))

(defn get-queue-url
  [client queue-name]
  (let [response (aws/invoke client
                             {:op :GetQueueUrl
                              :request {:QueueName queue-name}})]
    (when-not (:QueueUrl response)
      (cast/alert {:msg "Error gett-ing sqs-url"
                   ::get-queue-url-response response})
      (throw (ex-info "Unable to get queue url" {::queue-name queue-name
                                                 ::response response})))
    (:QueueUrl response)))

(defn walk-transactions
  [dt-conn sqs-client start-tx timeout]
  (let [queue-name (inf/make-cloudsearch-load-queue-name)
        sqs-url (get-queue-url sqs-client queue-name)
        stop-time (+ (System/currentTimeMillis) timeout)
        attribute-ids (get-attribute-ids dt-conn)
        id->ident (into {} (map (juxt :db/id :db/ident)
                                attribute-ids))
        location-attributes (into #{} (keys id->ident))]
    (transduce (comp (take-while* (fn [_] (< (System/currentTimeMillis) stop-time)))
                     cat
                     (map (juxt :t #(find-entities location-attributes (:data %))))
                     (mapcat (fn [[t-id d]]
                               (sort-by tx-and-type
                                        (map (fn [{:keys [e a tx]}]
                                               {:type (-> a id->ident attribute->entity)
                                                :e e
                                                :tx tx
                                                :t-id t-id})
                                             d))))
                     (partition-by tx-and-type)
                     (mapcat (partial expand-countries-and-regions dt-conn)))
               (entity-reducer dt-conn sqs-client sqs-url start-tx)
               (datomic-transactions dt-conn start-tx))))

(def tx-param-name "Last tx-id queued to cloudsearch-locations")

(defn get-tx-param
  [dt-conn]
  (ffirst (d/q '[:find (pull ?e [:db/id :param/int-value])
                 :in $ ?name
                 :where [?e :param/name ?name]]
               (d/db dt-conn)
               tx-param-name)))

(defn set-tx-param
  [dt-conn max-t]
  (d/transact dt-conn
              {:tx-data [{:db/id "param"
                          :param/name tx-param-name
                          :param/int-value max-t}]}))

(def sqs-client
  (delay (aws/client {:api :sqs})))

(defn queue-updates
  [{:keys [input]}]
  (try
    (let [options (try (edn/read-string input) (catch Exception _ {}))
          dt-conn (lib.d/get-conn (core/stage))
          {last-tx :param/int-value
           :or {last-tx 0}} (get-tx-param dt-conn)
          _ (cast/event {:msg "queue-updates starting" ::input input ::last-tx last-tx})
          {:keys [max-t sent deleted]
           :or {sent 0 deleted 0}
           :as result} (walk-transactions dt-conn @sqs-client
                                          (or (:start-tx options) (inc last-tx))
                                          (or (:timeout options) 240000))]
      (set-tx-param dt-conn max-t)
      (let [msg (format "Read through transaction %d. Sent %d updates, %d deletes." max-t sent deleted)]
        (cast/event {:msg msg
                     ::result result
                     ::app "t-fun"})
        msg))
    (catch Exception e
      (cast/alert {:msg "Exception running queue-updates"
                   ::exception e})
      (throw e))))

(defn upload-docs
  [client rows]
  (when (seq rows)
    (cast/event {:msg "Uploading to cloudsearch batch"
                 ::count (count rows)})
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
                                    :place/id
                                    :iata/airport-code
                                    :place/display-name
                                    :location/hotel-count
                                    :geo/latitude
                                    :geo/longitude
                                    :place/name
                                    :place/type

                                    {:place/country [:country/code
                                                     :country/name]}
                                    {:place/region [:region/name
                                                    :region/code]}
                                    ])
                    :in $ [?id ...]
                    :where [?e :place/id ?id]]
                  db
                  ids)))

(def locations-doc-client
  (delay (let [cs-client (aws/client {:api :cloudsearch})
               domain-name (format "locations-%s" (name (core/stage)))
               domain-status-list (aws/invoke cs-client {:op :DescribeDomains :request {:DomainNames [domain-name]}})
               endpoint (get-in domain-status-list [:DomainStatusList 0 :DocService :Endpoint])]
           (cast/event {:msg "cloudsearch client details"
                        ::details {:domain-name domain-name
                                   :domain-status-list domain-status-list
                                   :endpoint endpoint}})
           (aws/client {:api :cloudsearchdomain
                        :endpoint-override endpoint      ; Deprecated usage. Fix to use {:hostname "<hostname>"}
                        }))))


(defn load-locations-to-cloudsearch
  [{:keys [input]}]
  (try
    (let [dt-conn (lib.d/get-conn (core/stage))
          records (-> input
                      (json/parse-string true)
                      :Records)
          doc-client @locations-doc-client
          result (sequence
                  (map
                   (fn [record]
                     (let [{:keys [op ids] :as request} (-> record :body edn/read-string)]
                       (case op
                         :delete (do (cast/event {:msg (format "processing batch of %d deletes" (count ids))
                                                  ::deletes ids})
                                     (upload-docs doc-client
                                                  (sequence (comp (mapcat #(vector % (str % "-region_code")))
                                                                  (map #(hash-map :type "delete" :id %)))
                                                            ids)))
                         :update (do (cast/event {:msg (format "processing batch of %d updates" (count ids))
                                                  ::updates ids})
                                     (let [location-data (location-details (d/db dt-conn) ids)]
                                       (->> location-data
                                            (into {}
                                                  (comp
                                                   (map (juxt :place/id datomic->aws))))
                                            vals
                                            (upload-docs doc-client))))
                         (throw (ex-info (format "Unknown operation - %s" op) {:request request}))))))
                  records)]
      (cast/event {:msg "load-location-to-cloudsearch done"
                   ::result result})
      (pr-str result))
    (catch Exception e
      (cast/alert {:msg "Exception in load-locations-to-cloudsearch"
                   ::exception e})
      (throw e))))
