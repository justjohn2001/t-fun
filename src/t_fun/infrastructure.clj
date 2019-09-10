(ns t-fun.infrastructure
  (:require
   [cheshire.core :as json]
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
   [t-fun.lib.cast :as cast])
  (:import java.util.UUID))

(def account-id "304062982811")
(def region "us-east-1")

(def default-arn-role (format "arn:aws:iam::%s:role/CfAdministratorAccess" account-id))

(def t-fun-query-group-policies
  #_(iam/policy {::iam/policy-name "RoomkeyTfunSQSAccess"
                 ::iam/policy-document {::iam/statement [{::iam/effect "Allow"
                                                          ::iam/action ["sqs:ReceiveMessage" "sqs:GetQueueAttributes"
                                                                        "sqs:DeleteMessage" "sqs:DeleteMessageBatch"
                                                                        "sqs:SendMessage" "sqs:SendMessageBatch"
                                                                        "cloudsearch:document" "cloudsearch:search" "cloudsearch:DescribeDomains"]
                                                          ::iam/resource ["arn:aws:sqs:*:*:t-fun-*"
                                                                          interval (* 10 1000)                              ;; check every 10 seconds
                                                                          "arn:aws:cloudsearch:*:*:domain/locations-*"]}]}})
  [{"PolicyName" "RoomkeyTFunSQSAccess"
    "PolicyDocument" {"Version" "2012-10-17"
                      "Statement" [{"Effect" "Allow"
                                    "Action" ["sqs:ReceiveMessage" "sqs:GetQueueAttributes"
                                              "sqs:DeleteMessage" "sqs:DeleteMessageBatch"
                                              "sqs:SendMessage" "sqs:SendMessageBatch"
                                              "cloudsearch:document" "cloudsearch:search" "cloudsearch:DescribeDomains"]
                                    "Resource" ["arn:aws:sqs:*:*:t-fun-*"
                                                "arn:aws:cloudsearch:*:*:domain/locations-*"]}
                                   {"Effect" "Allow"
                                    "Action" "sqs:GetQueueUrl"
                                    "Resource" "*"}]}}])

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
                                                       ::lambda/batch-size 1})
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
  (log/infof "Creating stack %s" stack-name)
  (let [response (cf-describe cf-client stack-name)]
    (cond
      (:Stacks response)
      (do (cast/event {:msg "INFRASTRUCTURE - updating existing stack"})
          (invoke-with-throttle-retry {:args [cf-client
                                              {:op :UpdateStack
                                               :request {:StackName stack-name
                                                         :TemplateBody template}}]}))

      (re-find (re-pattern "does not exist") (get-in response [:ErrorResponse :Error :Message]))
      (do (cast/event {:msg "INFRASTRUCTURE - creating new stack"})
          (invoke-with-throttle-retry {:args [cf-client
                                              {:op :CreateStack
                                               :request {:StackName stack-name
                                                         :TemplateBody template
                                                         :RoleARN default-arn-role}}]}))

      :else (throw (ex-info "unknown response creating stack" response)))))

(defn wait
  [cf-client stack-name]
  (cast/event {:msg (format "INFRASTRUCTURE - waiting on %s" stack-name)})
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
    (cast/event {:msg "INFRASTRUCTURE - build-stack"})
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

(defn adjust-template
  [template]
  (-> template
      json/decode
      (update-in ["Resources" "DatomicLambdaRole" "Properties" "Policies"]
                 (fn [policies]
                   (into []
                         (filter #(not (re-find #"RoomkeyTFun.*"
                                                (get-in % ["PolicyName"])))
                                 policies)))) ;; remove any existing Tfun policies
      (update-in ["Resources" "DatomicLambdaRole" "Properties" "Policies"]
                 concat
                 t-fun-query-group-policies) ;; add the current policies
      json/encode))

(defn adjust-deployment-group
  [cf-client deployment-group]
  (cast/event {:msg "INFRASTRUCTURE - adjust-deployment-group"})
  (let [app-name (:app-name (ion/get-app-info))
        response (cf-describe cf-client deployment-group)
        {:keys [ErrorResponse TemplateBody] :as template} (aws/invoke cf-client
                                                                      {:op :GetTemplate
                                                                       :request {:StackName deployment-group}})]
    (cond (:ErrorResponse response) response
          ErrorResponse template
          :else (let [{:keys [Parameters Capabilities]} (get-in response [:Stacks 0])
                      adjusted-template (adjust-template TemplateBody)
                      s3-client (aws/client {:api :s3})
                      bucket-name "rk-persist"
                      key-name (format "%s/template/%s-%08x"
                                       app-name
                                       deployment-group
                                       (hash adjusted-template))]
                  (when (not= (json/encode TemplateBody) adjusted-template)
                    (cast/event {:msg "INFRASTRUCTURE - Updating query group stack" ::bucket bucket-name ::key-name key-name})
                    (aws/invoke s3-client
                                {:op :PutObject
                                 :request {:Bucket bucket-name
                                           :Key key-name
                                           :Body adjusted-template}})
                    (let [update-result (aws/invoke cf-client
                                                    {:op :UpdateStack
                                                     :request {:StackName deployment-group
                                                               :TemplateURL (format "https://s3.amazonaws.com/%s/%s" bucket-name key-name)
                                                               :Parameters Parameters
                                                               :Capabilities Capabilities}})]
                      (when (and (:ErrorResponse update-result)
                                 (not= (get-in update-result [:ErrorResponse :Error :Message])
                                       "No updates are to be performed."))
                        (cast/alert {:msg (format "INFRASTRUCTURE - error updating %s" deployment-group)
                                     ::update-result update-result})
                        update-result)))))))

(def stack-error
  (future (let [deployment-group (:deployment-group (ion/get-app-info))]
            (if-not deployment-group
              (do (cast/alert "INFRASTRUCTURE - Unable to determine deployment group")
                  "Unable to determine deployment group")
              (let [cf-client (aws/client {:api :cloudformation})
                    _ (cast/event {:msg (format "INFRASTRUCTURE - Stack updates starting on %s" deployment-group)})
                    result (some #(% cf-client deployment-group)
                                 [(fn [client group]
                                    (let [result (wait client group)]
                                      (if (or (nil? result) (= :failed result)) ;; :failed indicates a COMPLETE state, so okay to proceed.
                                        nil
                                        result)))
                                  adjust-deployment-group
                                  wait
                                  build-stack])]
                (if result
                  (cast/alert {:msg "INFRASTRUCTURE - Stack build result" ::result result})
                  (cast/event {:msg "INFRASTRUCTURE - Stack created/updated successfully"}))
                result)))))

(defn stack-state
  [{:keys [input] :as params}]
  (pr-str (or @stack-error "OK")))
