(ns t-fun.infrastructure
  (:require
   [cheshire.core :as json]
   [clojure.tools.logging :as log]
   [cognitect.aws.client.api :as aws]
   [cognitect.aws.util :as aws.util]
   [crucible.core :as c]
   [crucible.aws.sqs :as sqs]
   [crucible.aws.sqs.queue :as sqs.q]
   [crucible.aws.lambda :as lambda]
   [crucible.encoding :as e]
   [datomic.ion :as ion]
   [datomic.ion.cast :as cast])
  (:import java.util.UUID))

(defn make-template
  [prefix]
  (let [cloudsearch-queue-name (format "%s-cloudsearch-load" prefix)
        cloudsearch-queue (sqs/queue {::sqs.q/queue-name cloudsearch-queue-name})
        lambda-name (format "%s-cloudsearch-locations" (get (ion/get-app-info) :deployment-group))
        cs-queue->lambda (lambda/event-source-mapping {::lambda/event-source-arn (c/xref (keyword cloudsearch-queue-name)
                                                                                         :arn)
                                                       ::lambda/function-name lambda-name
                                                       ::lambda/batch-size 1})]
    (-> {(keyword cloudsearch-queue-name) cloudsearch-queue
         :tfun-cloudsearch-load-esm cs-queue->lambda}
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
      (:Stacks response) (do (cast/event {:msg "INFRASTRUCTURE - updating existing stack"})
                             (invoke-with-throttle-retry {:args [cf-client
                                                                 {:op :UpdateStack
                                                                  :request {:StackName stack-name
                                                                            :TemplateBody template}}]}))
      (re-find (re-pattern "does not exist") (get-in response [:ErrorResponse :Error :Message]))
      (do {cast/event (:msg "INFRASTRUCTURE - creating new stack")}
          (invoke-with-throttle-retry {:args [cf-client
                                              {:op :CreateStack
                                               :request {:StackName stack-name
                                                         :TemplateBody template}}]}))

      :else (throw (ex-info "unknown response creating stack" response)))))

(defn wait
  [cf-client stack-name]
  (let [duration (* 10 60 1000)                           ;; 10 minutes
        interval (* 10 1000)                              ;; check every 10 seconds
        end-time (+ (System/currentTimeMillis) duration)]
    (loop []
      (log/info "Waiting")
      (let [stack (cf-describe cf-client stack-name)
            status (get-in stack [:Stacks 0 :StackStatus])]
        (cond
          (#{"CREATE_COMPLETE" "UPDATE_COMPLETE"} status) nil
          (#{"ROLLBACK_COMPLETE" "UPDATE_ROLLBACK_COMPLETE"} status) :failed
          (> (System/currentTimeMillis) end-time) :timeout
          :else (do (Thread/sleep interval)
                    (recur)))))))

(defn build-stack
  [cf-client deployment-group]
  (try
    (let [stack-name (format "%s-infrastructure" deployment-group)
          template (make-template deployment-group)
          create-result (create-or-update cf-client stack-name template)]
      (if (and (:ErrorResponse create-result)
               (not= (get-in create-result [:ErrorResponse :Error :Message])
                     "No updates are to be performed."))
        create-result
        (when-let [result (wait cf-client stack-name)]
          (format "%s waiting for %s to stablize" (name result) stack-name))))
    (catch Exception e
      e)))

(def tfun-query-group-policies
  [{"PolicyName" "RoomkeyTfunSQSAccess"
    "PolicyDocument" {"Version" "2012-10-17"
                      "Statement" [{"Effect" "Allow"
                                    "Action" ["sqs:ReceiveMessage"
                                              "sqs:DeleteMessage" "sqs:DeleteMessageBatch"
                                              "sqs:SendMessage" "sqs:SendMessageBatch"]
                                    "Resource" ["arn:aws:sqs:*:*:tfun-*"]}]}}])

(defn adjust-template
  [template]
  (-> template
      :TemplateBody
      json/decode
      (update-in ["Resources" "DatomicLambdaRole" "Properties" "Policies"]
                 (fn [policies]
                   (into []
                         (filter #(not (re-find #"RoomkeyTfun.*"
                                                (get-in % ["PolicyName"])))
                                 policies)))) ;; remove any existing Tfun policies
      (update-in ["Resources" "DatomicLambdaRole" "Properties" "Policies"]
                 concat
                 tfun-query-group-policies) ;; add the current policies
      json/encode))

(defn adjust-deployment-group
  [cf-client deployment-group]
  (let [
        {:keys [Parameters Capabilities]} (-> (aws/invoke cf-client {:op :DescribeStacks :request {:StackName deployment-group}})
                                              (get-in [:Stacks 0]))
        template (aws/invoke cf-client
                             {:op :GetTemplate
                              :request {:StackName deployment-group}})
        adjusted-template (adjust-template template)
        s3-client (aws/client {:api :s3})
        bucket-name "rk-persist"
        key-name (format "tfun/template/%s-%08x" deployment-group (hash adjusted-template))]
    (when (not= (json/encode template) adjusted-template)
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
        (when [:ErrorResponse update-result]
          (cast/alert {:msg (format "INFRASTRUCTURE - error updating %s" deployment-group)
                       ::update-result update-result})
          update-result)))))

(defn build-steps
  [deployment-group]
  (some-fn
   #((cast/event "Waiting for deployment-group")
     (wait % deployment-group))
   #((cast/event "Adjusting deployment group")
     (adjust-deployment-group % deployment-group))
   #((cast/event "Waiting for deployment group to stablize")
     (wait % deployment-group))
   #((cast/event "Bulding infrastructure stack")
     (build-stack % deployment-group))))

(def stack-error
  (future
    (let [deployment-group (or (:deployment-group (ion/get-app-info)) "dc-development-compute-main")
          cf-client (aws/client {:api :cloudformation})
          result ((build-steps deployment-group) cf-client)]
      (if result
        (cast/alert {:msg "INFRASTRUCTURE - Stack build result" ::result result})
        (cast/event {:msg "INFRASTRUCTURE - Stack created/updated successfully"}))
      result)))

(defn stack-state
  [{:keys [input] :as params}]
  (pr-str (or @stack-error "OK")))
