(ns t-fun.unit.infrastructure
  (:require [t-fun.infrastructure :refer :all]
            [midje.sweet :as t :refer [=> contains anything]]
            [midje.util :as tu]))

(t/facts "invoke-with-throttle-retry"
         (t/fact "returns immediately on success"
                 (invoke-with-throttle-retry {:args [..client.. ..args..]}) => {:Response :success}
                 (provided (cognitect.aws.client.api/invoke ..client.. ..args..) => {:Response :success}))
         (t/fact "returns handles a retry"
                 (invoke-with-throttle-retry {:args [..client.. ..args..] :sleep 1}) => {:Response :success}
                 (provided (cognitect.aws.client.api/invoke ..client.. ..args..)
                           t/=streams=> [{:ErrorResponse {:Error {:Code "Throttling"}}}
                                         {:Response :success}]))
         (t/fact "returns error after retries exhausted"
                 (invoke-with-throttle-retry {:args [..client.. ..args..] :sleep 1})
                 => {:ErrorResponse {:Error {:Code "Throttling"}}}
                 (provided (cognitect.aws.client.api/invoke ..client.. ..args..)
                           => {:ErrorResponse {:Error {:Code "Throttling"}}}))
         (t/against-background (#'t-fun.infrastructure/interval) => 0))

(t/facts "adjust-template"
         (t/fact "adds policy"
                 (adjust-template "{}" [{:PolicyName "RoomkeyTFunSQSAccess"
                                         :PolicyDocument {:Version "2012-10-17"
                                                          :Statement []}}])
                 => #"RoomkeyTFunSQSAccess")
         (t/fact "removes existing RoomkeyTFun policies"
                 (adjust-template "{\"Resources\":{\"DatomicLambdaRole\":{\"Properties\":{\"Policies\":[{\"PolicyName\":\"RoomkeyTFunSQSAccess\",\"PolicyDocument\":{\"Version\":\"2012-10-17\",\"Statement\":[]}}]}}}}"
                                  [])
                 => "{\"Resources\":{\"DatomicLambdaRole\":{\"Properties\":{\"Policies\":[]}}}}"))

(t/facts "build-cloudsearch-domain"
         (t/fact "runs all options"
                 (build-cloudsearch-domain :none) => nil
                 (t/provided (cognitect.aws.client.api/invoke ..cs-client.. (contains {:op :DescribeDomains}))
                             t/=streams=> [{:DomainStatusList []}
                                           {:DomainStatusList [{:Processing false :RequiresIndexDocuments false}]}
                                           {:DomainStatusList [{:RequiresIndexDocuments true}]}
                                           {:DomainStatusList [{:Processing true}]}
                                           {:DomainStatusList [{:Processing false :RequiresIndexDocuments false}]}]
                             (#'t-fun.infrastructure/fix-schema!? anything anything) t/=streams=> [true false]
                             (#'t-fun.infrastructure/create-cloudsearch-domain ..cs-client.. anything) => nil
                             (cognitect.aws.client.api/invoke ..cs-client.. (contains {:op :IndexDocuments}))
                             => :success
                             (datomic.ion.cast/event (contains {:msg "Creating Cloudsearch domain"})) => nil
                             (datomic.ion.cast/event (contains {:msg "Indexing Cloudsearch domain documents"})) => nil
                             (datomic.ion.cast/event (contains {:msg "Waiting for Cloudsearch domain to finish processing..."})) => nil))
         (t/against-background
          (cognitect.aws.client.api/client anything) => ..cs-client..
          (#'t-fun.infrastructure/interval) => 0))
