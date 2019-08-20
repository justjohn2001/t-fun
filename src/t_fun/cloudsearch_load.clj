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
            [t-fun.lib.cast :as cast]
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

(defn datomic-transactions
  [dt-conn tx-id]
  (cast/dev {:msg (format "Loading tx %d" tx-id)})
  (when-let [tx (d/tx-range dt-conn
                            {:start tx-id
                             :end (inc tx-id)})]
    (cons (seq tx) (lazy-seq (datomic-transactions dt-conn (inc tx-id))))))


(defn take-until
  "Returns a lazy sequence of successive items from coll until the first value where
  (pred item) returns logical true. pred must be free of side-effects.
  Returns a transducer when no collection is provided."
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
  [attr-to-find tx-data]
  (sort-by :added
           (filter #(contains? attr-to-find (:a %)) tx-data)))

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
  (cast/event {:msg (format "Sending batch to sqs to %s" sqs-url)})
  (aws/invoke sqs-client {:op :SendMessage
                          :request {:QueueUrl sqs-url
                                    :MessageBody (format "{:op %s :ids [%s]}" op s)}}))

(defn entity-reducer
  [dt-conn sqs-client sqs-url start-tx]
  (fn entity-reducer*
    ([] {:pending {} :max-t start-tx})
    ([{:keys [pending max-t]}]
     (when (not-empty pending)
       (let [id-map (into {}
                          (d/q '[:find ?e ?id
                                 :in $ [?e ...]
                                 :where [?e :rk.place/id ?id]]
                               (d/db dt-conn) (keys pending)))
             updates (vals id-map)
             deletes (apply dissoc pending (keys id-map))
             send-result (transduce (comp (partition-all 1000)
                                          (map pr-str)
                                          (map (partial sqs-send sqs-client sqs-url :update)))
                                    conj
                                    []
                                    updates)]
                                        ; TODO - send deletes
         {:max-t max-t :sent (count updates) :send-results send-result :deleted (count deletes)})))
    ([acc [e t-val]]
     (-> acc
         (update-in [:pending e] (fnil max 0) t-val)
         (update :max-t max t-val)))))

(defn type-tx [e]
  [(first e) (nth e 2)])

(defn walk-transactions
  [dt-conn start-tx timeout]
  (let [sqs-client (aws/client {:api :sqs})
        sqs-url (:QueueUrl (aws/invoke sqs-client
                                       {:op :GetQueueUrl
                                        :request {:QueueName (format "%s-%s"
                                                                     (inf/make-stack-name)
                                                                     "cloudsearch-load")}}))
        stop-time (+ (System/currentTimeMillis) timeout)
        id->ident (into {} (map (juxt :db/id :db/ident)
                                (mem-attribute-ids dt-conn)))
        location-attributes (into #{} (map :db/id (mem-attribute-ids dt-conn)))]
    (transduce (comp (take-until (fn [_] (< (System/currentTimeMillis) stop-time)))
                     cat
                     (map (juxt :t #(find-entities location-attributes (:data %))))
                     (mapcat (fn [[t-id d]]
                               (sort-by type-tx
                                        (map (fn [{:keys [e a tx]}]
                                               (vector (-> a id->ident attribute->entity)
                                                       e tx t-id))
                                             d))))
                     (partition-by type-tx)
                     (mapcat (partial expand-countries-and-regions dt-conn)))
               (entity-reducer dt-conn sqs-client sqs-url start-tx)
               (datomic-transactions dt-conn start-tx))))

(defn queue-updates
  [i]
  (let [dt-conn (-> (datomic-config @core/stage)
                    d/client
                    (d/connect {:db-name "rk"}))
        [[start-tx e]] [[i nil]] #_(d/q '[:find ?tx ?e
                                          :where
                                          [?e :rk/name "Next txid to load into cloudsearch"]
                                          [?e :rk/tx-id ?tx]]
                                        (d/db dt-conn))
        {:keys [max-t sent deleted]} (walk-transactions dt-conn start-tx 0)]
    #_(d/transact dt-conn
                  {:tx-data [[:db/add e :rk/tx-id (inc max-t)]]})
    (let [msg (format "QUEUE-UPDATES: Read through transaction %d. Sent %d updates, %d deletes." max-t sent deleted)]
      (cast/event {:msg msg})
      msg)))

(defn upload-docs
  [client rows]
  (cast/event {:msg (format "LOAD-LOCATIONS: Starting batch of %d" (count rows))})
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

(defn location-details
  [db batch]
  (map first (d/q '[:find (pull ?id [:db/id
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
                  batch)))

(def locations-doc-client
  (delay (let [cs-client (aws/client {:api :cloudsearch})
               domain-name (format "locations-%s" (name @core/stage))
               domain-status-list (aws/invoke cs-client {:op :DescribeDomains :request {:DomainNames [domain-name]}})
               endpoint (get-in domain-status-list [:DomainStatusList 0 :DocService :Endpoint])]
           (aws/client {:api :cloudsearchdomain :endpoint-override endpoint}))))


(defn load-locations-to-cloudsearch
  [{:keys [input]}]
  (let [config (datomic-config @core/stage)
        dt-conn (-> config
                    d/client
                    (d/connect {:db-name "rk"}))
        {:keys [op ids]} (edn/read-string input)
        doc-client @locations-doc-client]
    (case op
      :delete (do (cast/event {:msg (format "LOAD_LOCATIONS - processing batch of %d deletes" (count ids))})
                  (upload-docs doc-client
                               (sequence (comp (mapcat #(vector % (str % "-region_code")))
                                               (map #(hash-map :type "delete" :id %)))
                                         ids)))
      :update (doseq [batch (partition-all 1000 ids)]
                (cast/event {:msg (format "LOAD-LOCATIONS - Processing batch of %d updates" (count batch))})
                (let [location-data (location-details (d/db dt-conn) batch)]
                  (-> location-data
                      (into {}
                            (comp
                             (mapcat #(cond-> [%]
                                        (get-in % [:rk.place/region :rk.region/code]) (conj (make-alt-location %))))
                             (map (juxt #(or (:alt-id %) (:rk.place/id %)) datomic->aws)))
                            location-data)
                      vals
                      (partial upload-docs doc-client))))))

  )
