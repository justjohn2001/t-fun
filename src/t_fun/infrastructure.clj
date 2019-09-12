(ns t-fun.infrastructure
  (:require
   [cheshire.core :as json]
   [clojure.edn :as edn]
   [clojure.set :as set]
   [clojure.tools.logging :as log]
   [cognitect.aws.client.api :as aws]
   [cognitect.aws.util :as aws.util]
   [crucible.core :as c]
   [crucible.aws.events :as events]
   [crucible.aws.lambda :as lambda]
   [crucible.aws.iam :as iam]
   [crucible.aws.sqs :as sqs]
   [crucible.aws.sqs.queue :as sqs.q]
   [crucible.encoding :as e]
   [datomic.ion :as ion]
   [t-fun.lib.cast :as cast]
   [t-fun.core :as core])
  (:import java.util.UUID))

(def account-id "304062982811")
(def region "us-east-1")

(def default-arn-role (format "arn:aws:iam::%s:role/CfAdministratorAccess" account-id))

(def make-stack-name
  (memoize (fn make-stack-name* []
             (let [{:keys [app-name deployment-group]} (ion/get-app-info)]
               (format "%s-%s-infrastructure" app-name deployment-group)))))

(def make-cloudsearch-load-queue-name
  (memoize (fn []
             (format "%s-%s" (make-stack-name) "cloudsearch-load-queue"))))

(defn five-minute-rule
  [fn-names]
  (events/rule {::events/name (format "%s-5-minutes" (make-stack-name))
                ::events/schedule-expression "rate(5 minutes)"
                ::events/targets (mapv (fn [name]
                                         {::events/id name
                                          ::events/arn (format "arn:aws:lambda:%s:%s:function:%s"
                                                               region account-id name)})
                                       fn-names)}))

(defn make-template
  []
  (let [prefixed-name (fn make-prefixed-name [s] (format "%s-%s" (make-stack-name) s))
        cloudsearch-queue-name (make-cloudsearch-load-queue-name)
        cloudsearch-queue (sqs/queue {::sqs.q/queue-name cloudsearch-queue-name
                                      ::sqs.q/visibility-timeout 300})
        locations-lambda-name (format "%s-cloudsearch-locations" (get (ion/get-app-info) :deployment-group))
        queue-lambda-name (format "%s-%s" (-> (ion/get-app-info) :deployment-group) "queue-updates")
        cs-queue->lambda (lambda/event-source-mapping {::lambda/event-source-arn (c/xref (keyword cloudsearch-queue-name)
                                                                                         :arn)
                                                       ::lambda/function-name locations-lambda-name
                                                       ::lambda/batch-size 1
                                                       ::lambda/enabled true})
        queue-lambda-perms (lambda/permission {::lambda/action "lambda:InvokeFunction"
                                               ::lambda/function-name queue-lambda-name
                                               ::lambda/principal "events.amazonaws.com"})]
    (-> {(keyword cloudsearch-queue-name) cloudsearch-queue
         (-> cloudsearch-queue-name (str "-esm") keyword) cs-queue->lambda

         (-> (prefixed-name "5-minute-rule") keyword)
         (five-minute-rule [queue-lambda-name])

         (-> (prefixed-name "5-minute-rule-perms") keyword) queue-lambda-perms}
        (c/template "Resources to support t-fun")
        e/encode)))

(defn invoke-with-throttle-retry
  [{:keys [args retries sleep]
    :or {retries 5 sleep 1000}
    :as input}]
  (let [response (apply aws/invoke args)]
    (if (and (pos? retries)
             (#{"Throttling"} (get-in response [:ErrorResponse :Error :Code])))
      (do (Thread/sleep sleep)
          (recur (assoc input
                        :retries (dec retries)
                        :sleep (* 2 sleep))))
      response)))

(defn cf-describe
  [cf-client stack-name]
  (invoke-with-throttle-retry {:args [cf-client {:op :DescribeStacks
                                                 :request {:StackName stack-name}}]}))

(defn create-or-update
  [cf-client stack-name template]
  (cast/event {:msg "Creating stack" ::stack stack-name})
  (let [response (cf-describe cf-client stack-name)]
    (cond
      (:Stacks response)
      (do (cast/event {:msg "updating existing stack"
                       ::stack stack-name})
          (invoke-with-throttle-retry {:args [cf-client
                                              {:op :UpdateStack
                                               :request {:StackName stack-name
                                                         :TemplateBody template}}]}))

      (re-find (re-pattern "does not exist") (get-in response [:ErrorResponse :Error :Message]))
      (do (cast/event {:msg "creating new stack"
                       ::stack stack-name})
          (invoke-with-throttle-retry {:args [cf-client
                                              {:op :CreateStack
                                               :request {:StackName stack-name
                                                         :TemplateBody template
                                                         :RoleARN default-arn-role}}]}))

      :else (throw (ex-info "unknown response creating stack" response)))))

(defn wait
  [cf-client stack-name]
  (cast/event {:msg "waiting on stack"
               ::stack stack-name})
  (let [duration (* 10 60 1000)                           ;; 10 minutes
        end-time (+ (System/currentTimeMillis) duration)]
    (loop [interval 1000]
      (let [stack (cf-describe cf-client stack-name)
            status (get-in stack [:Stacks 0 :StackStatus])]
        (cond
          (:ErrorResponse stack) (:ErrorResponse stack)
          (#{"CREATE_COMPLETE" "UPDATE_COMPLETE"} status) nil
          (#{"ROLLBACK_COMPLETE" "UPDATE_ROLLBACK_COMPLETE"} status) :failed
          (> (System/currentTimeMillis) end-time) (format "Timeout waiting. Status %s" status)
          :else (do (Thread/sleep interval)
                    (recur (if (< interval 10000) (* 2 interval) interval))))))))

(defn build-stack
  [cf-client deployment-group]
  (try
    (cast/event {:msg "build-stack"
                 ::deployment-group deployment-group})
    (let [template (make-template)
          create-result (create-or-update cf-client (make-stack-name) template)]
      (if (and (:ErrorResponse create-result)
               (not= (get-in create-result [:ErrorResponse :Error :Message])
                     "No updates are to be performed."))
        create-result
        (when-let [result (wait cf-client (make-stack-name))]
          (format "%s waiting for %s to stabilize" (name result) (make-stack-name)))))
    (catch Exception e
      e)))

(def stack-error
  (future (let [deployment-group (:deployment-group (ion/get-app-info))]
            (if-not deployment-group
              (do (cast/alert {:msg "Unable to determine deployment group"
                               ::app-info (ion/get-app-info)})
                  "Unable to determine deployment group")
              (let [cf-client (aws/client {:api :cloudformation})
                    _ (cast/event {:msg "Stack updates starting"
                                   ::deployment-group deployment-group})
                    result (build-stack cf-client deployment-group)]
                (if result
                  (cast/alert {:msg "Stack build result"
                               ::deployment-group deployment-group
                               ::result result})
                  (cast/event {:msg "Stack created/updated successfully"
                               ::deployment-group deployment-group}))
                result)))))

(defn create-cloudsearch-domain
  [cs-client domain-name]
  (cast/event {:msg "Creating domain. This may take 10 minutes."
               ::domain domain-name})
  (aws/invoke cs-client {:op :CreateDomain :request {:DomainName domain-name}}))

(defn create-cloudsearch-index
  [cs-client domain-name schema]

  (doseq [index-field schema]
    (let [field-response (aws/invoke cs-client
                                     {:op :DefineIndexField
                                      :request {:DomainName domain-name
                                                :IndexField index-field}})
          status (get-in field-response [:IndexField :Status :State])]
      (cast/event {:msg "Creating field."
                   ::domain domain-name
                   ::field (:IndexFieldName index-field)
                   ::result status})
      (case status
        nil (throw (ex-info "Unknown response" {:field index-field :response field-response}))
        "FailedToValidate" (throw (ex-info "Field failed to validate" {:field index-field :response field-response}))
        true))))

(defn- remove-cloudsearch-fields
  [cs-client domain-name fields]
  (cast/alert {:msg "Extra fields found in cloudsearch domain"
               ::domain domain-name
               ::fields (map :IndexFieldName fields)})
  (doseq [{field-name :IndexFieldName} fields]
    (cast/event {:msg "Deleting field" ::domain domain-name ::field field-name})
    (aws/invoke cs-client {:op :DeleteIndexField :request {:DomainName domain-name :IndexFieldName field-name}}))
  (aws/invoke cs-client {:op :IndexDocuments})
  (seq fields))

(defn- fix-schema!?
  "Checks to see if the schema matches locations.edn. If not, the function fixes the schema and returns true to indicate a change was made."
  [cs-client domain-name]
  (cast/event {:msg "Checking whether schema needs fixing" ::domain domain-name})
  (let [base-schema (into #{}
                          (edn/read-string (slurp "resources/cloudsearch/locations.edn")))
        cs-schema (into #{}
                        (map :Options
                             (:IndexFields (aws/invoke cs-client
                                                       {:op :DescribeIndexFields
                                                        :request {:DomainName domain-name}}))))
        missing-fields (set/difference base-schema cs-schema)
        extra-fields (set/difference cs-schema base-schema)]
    (cond
      (= base-schema cs-schema) false
      (seq missing-fields) (create-cloudsearch-index cs-client
                                                     domain-name
                                                     (sort-by :IndexFieldName missing-fields))
      ;; This should only be run if there are no missing fields to be added.
      ;; A field with incorrect properties will show up in both missing and extra,
      ;; so will be dropped if the remove is run after the create.
      (seq extra-fields) (remove-cloudsearch-fields cs-client domain-name extra-fields))))

(defn build-cloudsearch-domain
  [stage]
  (let [cs-client (aws/client {:api :cloudsearch})
        domain-name (format "locations-%s" stage)]
    (loop []
      (Thread/sleep 1000)       ; Pause to let previous step settle.
      (let [domain-status-list (aws/invoke cs-client {:op :DescribeDomains :request {:DomainNames [domain-name]}})
            domain-status (get-in domain-status-list [:DomainStatusList 0])]
        (cond
          (not (:DomainStatusList domain-status-list))
          (do (let [msg {:msg "Error calling DescribeDomains"
                         ::domain domain-name
                         ::result domain-status-list}]
                (cast/alert msg)
                (pr-str msg)))

          (not domain-status)
          (do (cast/event {:msg "Creating Cloudsearh domain" ::domain-name domain-name})
              (create-cloudsearch-domain cs-client domain-name)
              (recur))

          (:Processing domain-status)
          (do
            (cast/event {:msg "Waiting for Cloudsearch domain to finish processing..."
                         ::domain domain-name})
            (Thread/sleep 30000)
            (recur))

          (:RequiresIndexDocuments domain-status)
          (do
            (cast/event {:msg "Indexing Cloudsearch domain documents" ::domain domain-name})
            (aws/invoke cs-client {:op :IndexDocuments :request {:DomainName domain-name}})
            (recur))

          (fix-schema!? cs-client domain-name)
          (do (Thread/sleep 10000)
              (recur))

          :else nil)))))

(def cs-domain-error
  (future (let [stage (name @core/stage)]
            (if-not stage
              (do (cast/alert {:msg "cs-domain-error unable to determine stage"
                               ::app-info (ion/get-env)})
                  "cs-domain-error unable to determine stage")
              (let [
                    result (build-cloudsearch-domain stage)]
                (if result
                  (cast/alert {:msg "Cloudsearch domain build result"
                               ::stage stage
                               ::result result})
                  (cast/event {:msg "Cloudsearch domain created/updated successfully"
                               ::stage stage}))
                result)))))

(defn stack-state
  [{:keys [input] :as params}]
  (pr-str (or @stack-error @cs-domain-error "OK")))

