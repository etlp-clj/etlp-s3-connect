(ns etlp-s3-connect.core
  (:require [clojure.core.async :as a :refer [>! chan go pipe pipeline]]
            [clojure.core.async.impl.protocols :as impl]
            [clojure.tools.logging.readable :refer [debug info warn]]
            [cognitect.aws.client.api :as aws]
            [cognitect.aws.credentials :as credentials]
            [clojure.pprint :refer [pprint]]
            [clojure.string :as s]
            [etlp.connector.dag :as dag]
            [etlp.connector.protocols :refer [EtlpSource EtlpDestination]]
            [etlp.utils.reducers :refer [lines-reducible]]
            [etlp.utils.core :refer [wrap-error wrap-log wrap-record]])
  (:import [java.io BufferedReader InputStreamReader]
           [clojure.core.async.impl.channels ManyToManyChannel])
  (:gen-class))

(defn s3-reducible [xf-provider blob]
  (if (instance? ManyToManyChannel blob)
    (throw (Exception. "Invalid Blob"))
    (try
      (eduction
       (xf-provider blob)
       (-> blob
           :Body
           InputStreamReader.
           BufferedReader.
           lines-reducible))
      (catch Exception e
        (warn (str "Error Processing:: " e))
        (throw e)))))


(defn s3-invoke [{:keys [region credentials] :as s3conf}]
  (try (aws/client {:api :s3
                    :region region
                    :credentials-provider (credentials/basic-credentials-provider credentials)})
       (catch Exception ex
         (warn ex)
         (throw ex))))

(defn uuid [] (.toString (java.util.UUID/randomUUID)))


(defn list-objects-pipeline [{:keys [client bucket prefix files-channel]}]
  (let [list-objects-request {:op :ListObjectsV2 :request {:Bucket bucket :Prefix prefix}}]
    (a/go
      (try
        (loop [marker nil]
          (let [response   (a/<!
                            (aws/invoke-async client
                                              (assoc-in
                                               list-objects-request [:request :NextContinuationToken] marker)))
                contents   (:Contents response)
                new-marker (:NextContinuationToken response)]
            (if (contains? response :Error)
              (do
                (a/>! files-channel (str (Exception. (get-in response [:Error :Code]))))
                (a/close! files-channel)))
            (when-not (contains? response :Error)
              (doseq [file contents]
                  (a/>! files-channel file))
              (if new-marker
                (recur new-marker)
                (a/close! files-channel)))))
        (catch Exception e
          (a/close! files-channel)
          (throw e))))))


(defn get-object-pipeline-async [{:keys [client bucket files-channel output-channel error-channel pf]}]
 (a/pipeline-async
   pf
   output-channel
   (fn [acc res]
     (a/go
       (try
         (let [content (a/<! (aws/invoke-async
                                client
                                {:op :GetObject
                                 :request {:Bucket bucket :Key (acc :Key)}}))]
           (if (contains? content :Error)
             (do
               (a/>! res content)
               (a/close! res))
             (a/>! res (assoc content :Key (acc :Key) :Bucket bucket))))
         (catch Exception e
           (a/>! res e)))
       (a/close! res)))
   files-channel))

(defn download-file [client bucket file error-chan]
  (aws/invoke-async client {:op :GetObject :request {:Bucket bucket :Key file}}))


(defn download-files-pipeline-async [{:keys [pf client bucket files-channel output-channel error-channel]}]
  (let [errors-chan (a/chan)]
    (a/pipeline-async
      pf
      output-channel
      (fn [acc res]
        (a/go
          (try
            (let [content (a/<! (download-file client bucket (acc :Key) errors-chan))]

              (if (contains? content :Error)
                (a/put! errors-chan content)
                (a/>! res content)))
            (catch Exception e
              (a/put! errors-chan e)))
          (a/close! res)
          (a/close! errors-chan)))
      files-channel)
    (a/go
      (doseq [entry (a/<! errors-chan)]
        (a/>! error-channel entry))
      (a/close! error-channel))))


(def build-file-path (fn [{:keys [s3-prefix file-prefix file-extension]}]
                      (str s3-prefix "/" file-prefix "-" (uuid) "." file-extension)))

(def upload-batch-s3 (fn [{:keys [s3-client s3-bucket s3-prefix file-prefix file-extension] :as config} msg]
                       (aws/invoke-async s3-client {:op      :PutObject
                                                    :request {:Bucket s3-bucket
                                                              :Key    (build-file-path
                                                                       (select-keys config [:s3-bucket
                                                                                            :s3-prefix
                                                                                            :file-prefix
                                                                                            :file-extension]))
                                                              :Body   (.getBytes (s/join "\n" msg))}})))


(defn upload-objects-pipeline-async [{:keys [pf s3-client s3-bucket files-channel output-channel error-channel] :as config}]
  (let [errors-chan (a/chan)]
    (a/pipeline-async
      pf
      output-channel
      (fn [acc res]
        (a/go
          (try
            (info "Uploading Batch of size" (count acc))
            (let [content (a/<! (upload-batch-s3 (select-keys config [:s3-bucket
                                                                      :s3-client
                                                                      :s3-prefix
                                                                      :file-prefix
                                                                      :file-extension]) acc))]

              (if (contains? content :Error)
                (do
                  (info content)
                  (a/put! errors-chan content))
                (a/>! res content)))
            (catch Exception e
              (warn e)
              (a/put! errors-chan e)))
          (a/close! res)
          (a/close! errors-chan)))
      files-channel)
    (a/go
      (doseq [entry (a/<! errors-chan)]
        (warn entry)
        (a/>! error-channel entry))
      (a/close! error-channel))))



(def list-s3-processor  (fn [data]
                          (list-objects-pipeline {:client        (data :s3-client)
                                                  :bucket        (data :bucket)
                                                  :files-channel (data :channel)
                                                  :prefix        (data :prefix)})
                          (data :channel)))

(def get-s3-objects (fn [data]
                      ;; (println "Xform>>>" data)
                      (let [output (data :output-channel)]
                        (get-object-pipeline-async {:client         (data :s3-client)
                                                    :bucket         (data :bucket)
                                                    :files-channel  (data :channel)
                                                    :pf             (data :threads)
                                                    :error-channel  (a/chan 1)
                                                    :output-channel output})
                        output)))

(def upload-s3-objects (fn [data]
                         (let [output (data :output-channel) errors-chan (data :error-channel)]
                           (upload-objects-pipeline-async {:s3-client      (data :s3-client)
                                                           :s3-bucket      (data :s3-bucket)
                                                           :s3-prefix      (data :s3-prefix)
                                                           :files-channel  (data :channel)
                                                           :file-prefix    (data :file-prefix)
                                                           :file-extension (data :file-extension)
                                                           :pf             (data :threads)
                                                           :error-channel  errors-chan
                                                           :output-channel output})
                          output)))

(def etlp-processor (fn [data]
                      (if (instance? ManyToManyChannel data)
                        data
                        (data :channel))))

(defn s3-upload-topology [{:keys [s3-config prefix bucket processors threads partitions file-prefix file-extension] :as conf}]
  (let [s3-client (s3-invoke s3-config)
        entities  {:etlp-input {:channel        (a/chan (a/buffer partitions))
                                :s3-client      s3-client
                                :s3-bucket      bucket
                                :s3-prefix      prefix
                                :file-prefix    file-prefix
                                :file-extension file-extension
                                :threads        threads
                                :output-channel (a/chan (a/buffer partitions))
                                :error-channel  (a/chan (a/buffer partitions))
                                :meta           {:entity-type :processor
                                                 :processor   (processors :upload-s3-processor)}}

                   :etlp-output {:meta {:entity-type :processor
                                        :processor   (processors :etlp-processor)}}}
        workflow [[:etlp-input :etlp-output]]]

    {:entities entities
     :workflow workflow}))


(defn s3-process-topology [{:keys [s3-config prefix bucket processors reducers reducer threads partitions]}]
  (let [s3-client   (s3-invoke s3-config)
        entities    {:etlp-input {:s3-client s3-client
                                  :bucket    bucket
                                  :prefix    prefix
                                  :channel   (a/chan (a/buffer partitions))
                                  :meta      {:entity-type :processor
                                              :processor   (processors :list-s3-processor)}}

                     :get-s3-objects {:s3-client      s3-client
                                      :bucket         bucket
                                      :threads        threads
                                      :channel        (a/chan (a/buffer partitions))
                                      :output-channel (a/chan (a/buffer partitions))
                                      :meta           {:entity-type :processor
                                                       :threads     threads
                                                       :processor   (processors :get-s3-objects)}}

                     :etlp-output {:meta {:entity-type :processor
                                          :processor   (processors :etlp-processor)}}}
        workflow [[:etlp-input :get-s3-objects]
                  [:get-s3-objects :etlp-output]]]

    {:entities entities
     :workflow workflow}))

(defn s3-list-topology [{:keys [s3-config prefix bucket processors reducers reducer threads partitions]}]
  (let [s3-client (s3-invoke s3-config)
        entities  {:list-s3-objects {:s3-client s3-client
                                     :bucket    bucket
                                     :prefix    prefix
                                     :channel   (a/chan (a/buffer partitions))
                                     :meta      {:entity-type :processor
                                                 :processor   (processors :list-s3-processor)}}


                   :etlp-output {:channel (a/chan (a/buffer partitions))
                                 :meta    {:entity-type :processor
                                           :processor   (processors :etlp-processor)}}}
        workflow [[:list-s3-objects :etlp-output]]]

    {:entities entities
     :workflow workflow}))



(defn save-into-database [rows batch]
  (swap! rows + (count batch))
  (println (wrap-log (str "Total Count of Records:: " @rows))))


(defrecord EtlpS3Source [s3-config prefix bucket processors topology-builder reducers reducer threads partitions]
  EtlpSource
  (spec [this] {:supported-destination-streams []
                :supported-source-streams      [{:stream_name "s3_stream"
                                                 :schema      {:type       "object"
                                                               :properties {:s3-config  {:type        "object"
                                                                                         :description "S3 connection configuration."}
                                                                            :bucket     {:type        "string"
                                                                                         :description "The name of the S3 bucket."}
                                                                            :processors {:type        "object"
                                                                                         :description "Processors to be used to extract and transform data from the S3 bucket."}}}}]})

  (check [this]
    (let [errors (conj [] (when (nil? (:s3-config this))
                            "s3-config is missing")
                       (when (nil? (:bucket this))
                         "bucket is missing")
                       (when (nil? (:processors this))
                         "processors is missing"))]
      {:status  (if (empty? errors) :valid :invalid)
       :message (if (empty? errors) "Source configuration is valid." (str "Source configuration is invalid. Errors: " (clojure.string/join ", " errors)))}))

  (discover [this]
            ;; TODO use config and topology to discover schema from mappings
    {:streams [{:stream_name "s3_stream"
                :schema      {:type       "object"
                              :properties {:data {:type "string"}}}}]})
  (read! [this]
    (let [topology (topology-builder this)
          workflow (dag/build topology)]
     workflow)))


(def create-s3-source! (fn [{:keys [s3-config bucket prefix reducers reducer threads partitions] :as opts}]
                        (let [s3-connector (map->EtlpS3Source {:s3-config        s3-config
                                                               :prefix           prefix
                                                               :bucket           bucket
                                                               :threads          threads
                                                               :partitions       partitions
                                                               :processors       {:list-s3-processor list-s3-processor
                                                                                  :get-s3-objects    get-s3-objects
                                                                                  :etlp-processor    etlp-processor}
                                                               :reducers         reducers
                                                               :reducer          reducer
                                                               :topology-builder s3-process-topology})]
                         s3-connector)))

(def create-s3-list-source! (fn [{:keys [s3-config bucket prefix reducers reducer threads partitions] :as opts}]
                              (let [s3-connector (map->EtlpS3Source {:s3-config        s3-config
                                                                     :prefix           prefix
                                                                     :bucket           bucket
                                                                     :threads          threads
                                                                     :partitions       partitions
                                                                     :processors       {:list-s3-processor list-s3-processor
                                                                                        :etlp-processor    etlp-processor}
                                                                     :reducers         reducers
                                                                     :reducer          reducer
                                                                     :topology-builder s3-list-topology})]
                               s3-connector)))

(defrecord S3Destination [s3-config bucket prefix file-extension file-prefix processors threads partitions topology-builder]
  EtlpDestination
  (write! [this]
    (let [topology (topology-builder this)
          workflow (dag/build topology)]
      workflow)))

(def create-s3-destination! (fn [{:keys [s3-config bucket prefix file-prefix file-extension threads partitions] :as opts}]
                              (let [s3-connector (map->S3Destination {:s3-config        s3-config
                                                                      :prefix           prefix
                                                                      :bucket           bucket
                                                                      :threads          threads
                                                                      :partitions       partitions
                                                                      :processors       {:upload-s3-processor upload-s3-objects
                                                                                         :etlp-processor      etlp-processor}
                                                                      :file-prefix      file-prefix
                                                                      :file-extension   file-extension
                                                                      :topology-builder s3-upload-topology})]
                               s3-connector)))
