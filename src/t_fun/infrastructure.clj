(ns t-fun.infrastructure
  (:require [cognitect.aws.client.api :as aws]
            [cognitect.aws.util :as aws.util]
            [crucible.core :as c]
            [crucible.aws.sqs :as sqs]
            [crucible.aws.sqs.queue :as sqs.q]
            [crucible.aws.lambda :as lambda]
            [crucible.encoding :as e]
            [clojure.tools.logging :as log]
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
          (#{"CREATE_COMPLETE" "UPDATE_COMPLETE"} status) "Success"
          (#{"ROLLBACK_COMPLETE" "UPDATE_ROLLBACK_COMPLETE"} status) "Failed"
          (> (System/currentTimeMillis) end-time) "Timeout"
          :else (do (Thread/sleep interval)
                    (recur)))))))

(defn build-stack
  [prefix]
  (try
    (let [cf-client (aws/client {:api :cloudformation})
          stack-name (format "%s-infrastructure" prefix)
          template (make-template prefix)
          create-result (create-or-update cf-client stack-name template)]
      (if (:ErrorResponse create-result)
        (when-not (= (get-in create-result [:ErrorResponse :Error :Message])
                     "No updates are to be performed.")
          create-result)
        (let [result (wait cf-client stack-name)]
          (if (= result "Success")
            nil
            result))))
    (catch Exception e
      e)))

(def stack-error (future
                   (let [deployment-group (:deployment-group (ion/get-app-info))
                         _ (cast/event {:msg "INFRASTRUCTURE - Starting stack build"})
                         result (build-stack (format "tfun-%s" deployment-group))]
                     (if result
                       (cast/alert {:msg "INFRASTRUCTURE - Stack build result" ::result result})
                       (cast/event {:msg "INFRASTRUCTURE - Stack created/updated successfully"}))
                     result)))

(defn stack-state
  [{:keys [input] :as params}]
  (pr-str (or @stack-error "OK")))
