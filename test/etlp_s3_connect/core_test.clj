(ns etlp-s3-connect.core-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :as a]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [cognitect.aws.client.test-double :as test]
            [etlp-s3-connect.core :refer :all])
  (:import [clojure.core.async.impl.channels ManyToManyChannel]
           [java.io InputStream]))

(defn numeric-line? [line]
  (try
    (number? line)
    (catch Exception e
      false)))


(defmacro def-s3-reducible-test [name input output xform]
  `(clojure.test/deftest ~name
     (let [xf# ~xform
           {:keys [Body] :as blob#} {:Body (io/input-stream (.getBytes ~input))}
           result# (into [] (etlp-s3-connect.core/s3-reducible xf# blob#))]
       (clojure.test/is (= ~output result#)))))


(defmacro def-list-s3-objects-test [name input response expected-output]
  `(clojure.test/deftest ~name
     (let [client# (cognitect.aws.client.test-double/client {:api :s3
                                                             :ops {:ListObjectsV2 (fn [params-map#]
                                                                                    ~response)}})
           files-chan# (a/chan)]
       (list-objects-pipeline (assoc ~input :client client# :files-channel files-chan#))
       (let [results# (a/<!! (a/into [] files-chan#))]
         (is (= results# ~expected-output))))))

(defmacro def-get-object-pipeline-test [name config input xf response expected-output]
  `(clojure.test/deftest ~name
     (let [client# (cognitect.aws.client.test-double/client {:api :s3
                                                             :ops {:GetObject (fn [request#]
                                                                                (if (nil? ~response)
                                                                                  {:Error {:Code "File Not Found"}}
                                                                                  (merge request# ~response)))}})
           xform#        (fn [data#] ~xf)
           s3-reducer#   (comp
                          (filter #(contains? % :Body))
                          (mapcat (partial etlp-s3-connect.core/s3-reducible xform#)))
           results-chan# (a/chan 1 s3-reducer#)]
       (get-object-pipeline-async (assoc ~config :pf 1 :client client# :files-channel ~input :output-channel results-chan#))
       (let [results# (a/<!! (a/into []  results-chan#))]
         (is (= results# ~expected-output))))))
       
       
(defmacro def-download-files-pipeline-async-test [name config input xf response expected-output expected-errors]
  `(clojure.test/deftest ~name
     (let [client#      (cognitect.aws.client.test-double/client {:api :s3
                                                                  :ops {:GetObject (fn [request#]
                                                                                     (if (nil? ~response)
                                                                                       {:Error {:Code "NoSuchKey" :Message "The specified key does not exist"}}
                                                                                       (merge request# ~response)))}})
           errors-chan# (a/chan)
           xform#       (fn [data#] ~xf)
           s3-reducer#  (comp
                         (filter #(contains? % :Body))
                         (mapcat (partial etlp-s3-connect.core/s3-reducible xform#)))
           results-chan# (a/chan 1 s3-reducer#)]
       (download-files-pipeline-async (assoc ~config :pf 2 :client client# :files-channel ~input :output-channel results-chan# :error-channel errors-chan#))
       (let [results# (a/<!! (a/into []  results-chan#))]
         (is (= results# ~expected-output)))
       (let [errors# (a/<!! (a/into [] errors-chan#))]
         (is (= errors# ~expected-errors))))))


(defmacro def-upload-object-pipeline-test [name config input xf response expected-output]
  `(clojure.test/deftest ~name
     (let [client# (cognitect.aws.client.test-double/client {:api :s3
                                                             :ops {:PutObject (fn [request#]
                                                                                (if (nil? ~response)
                                                                                  {:Error {:Code "Network Unreachable"}}
                                                                                  ~response))}})
           xform#        ~xf

           results-chan# (a/chan 1 xform#)
           error-chan# (a/chan)]
       (upload-objects-pipeline-async (assoc ~config :pf 1 :s3-client client# :files-channel ~input :output-channel results-chan# :error-channel error-chan#))
       (let [results# (a/<!! (a/into [] (a/merge [results-chan# error-chan#])))]
         (is (= results# ~expected-output))))))

(def-upload-object-pipeline-test
  network-unreachable-on-put
  {:s3-bucket      "test-bucket"
   :s3-prefix      "foobar"
   :file-prefix    "msg"
   :file-extension "hl7"}
  (a/to-chan [["MSH|some|field|mesg"]])
  (map identity)
  nil
  [[:Error {:Code "Network Unreachable"}]])


(def-upload-object-pipeline-test
  bucket-invalid-on-put
  {:s3-bucket      "test-bucket"
   :s3-prefix      "foobar"
   :file-prefix    "msg"
   :file-extension "hl7"}
  (a/to-chan [["MSH|some|field|mesg"]])
  (map identity)
  {:Error {:Code "Invalid Bucket"}}
  [[:Error {:Code "Invalid Bucket"}]])

;; (run-test network-unreachable-on-put)
;; (run-tests)

(def-download-files-pipeline-async-test
  alt-download-multiple-files
  {:bucket "test-bucket"}
  (a/to-chan [{:Key "test-file-2"} {:Key "test-file-1"}])
  (comp
    (map (fn [d]
           (str/split d #",")))
    (mapcat identity)
    (map (fn [d]
           (Integer/parseInt d))))
  {:Body (io/input-stream (.getBytes "1,2,3,4\n2,3,4\n5,6,7"))}
  [1 2 3 4 2 3 4 5 6 7 1 2 3 4 2 3 4 5 6 7]
  [])

(def-download-files-pipeline-async-test
  alt-download-multiple-files-nil
  {:bucket "test-bucket"}
  (a/to-chan [{:Key "test-file-2"} {:Key "test-file-1"}])
  (comp
    (map (fn [d]
           (str/split d #",")))
    (mapcat identity)
    (map (fn [d]
           (Integer/parseInt d))))
  nil
  []
  [[:Error {:Code "NoSuchKey" :Message "The specified key does not exist"}]])

(def-get-object-pipeline-test
 download-empty-input
 {:bucket "test-bucket"}
 (a/to-chan [])
 (comp
    (map (fn [d]
           (str/split d #",")))
    (mapcat (fn [d] d))
    (map (fn [d] (Integer/parseInt d))))
 nil
 [])

(def-get-object-pipeline-test
 download-multiple-files
 {:bucket "test-bucket"}
 (a/to-chan [{:Key "test-file-2"} {:Key "test-file-1"}])
 (comp
  (map (fn [d]
         (str/split d #",")))
  (mapcat (fn [d] d))
  (map (fn [d] (Integer/parseInt d))))
 {:Key "test-file-2" :Body (io/input-stream (.getBytes "1,2,3,4\n2,3,4\n5,6,7"))}
 [1 2 3 4 2 3 4 5 6 7 1 2 3 4 2 3 4 5 6 7])

(def-get-object-pipeline-test
 download-single-file
 {:bucket "test-bucket"}
 (a/to-chan [{:Key "test-file-2"}])
 (comp
  (map (fn [d]
         (str/split d #",")))
  (mapcat (fn [d] d))
  (map (fn [d] (Integer/parseInt d))))
 {:Key "test-file-2" :Body (io/input-stream (.getBytes "1,2,3,4\n2,3,4\n5,6,7"))}
 [1 2 3 4 2 3 4 5 6 7])

(def-get-object-pipeline-test
 download-files-that-do-not-exist
 {:bucket "test-bucket"}
 (a/to-chan [{:Key "non-existent-file-1"} {:Key "non-existent-file-2"}])
 (comp
  (map (fn [d]
         (str/split d #",")))
  (mapcat (fn [d] d))
  (map (fn [d] (Integer/parseInt d))))
 nil
 [])

(def-s3-reducible-test s3-reducible-test-1 "1\n2\n3\n" [2 3 4]
  (fn [blob]
    (comp
     (map (fn [d] (Integer/parseInt d)))
     (filter numeric-line?)
     (map inc))))

;(run-test s3-reducible-test-1)

(def-s3-reducible-test s3-reducible-test-2 "1\n2\n3\n" [3 4 5]
  (fn [blob]
    (comp
     (map (fn [d] (Integer/parseInt d)))
     (filter numeric-line?)
     (map inc)
     (map inc))))

;(run-test s3-reducible-test-2)

(def-list-s3-objects-test
 test-single-file
 {:bucket "test-bucket" :prefix "test-prefix"}
 {:Contents [{:Key "test-file-1"}]}
 [{:Key "test-file-1"}])

(def-list-s3-objects-test
 test-multiple-files
 {:bucket "test-bucket" :prefix "test-prefix"}
 {:Contents [{:Key "test-file-1"} {:Key "test-file-2"} {:Key "test-file-3"}]}
 [{:Key "test-file-1"} {:Key "test-file-2"} {:Key "test-file-3"}])

(def-list-s3-objects-test
 test-no-files
 {:bucket "test-bucket" :prefix "test-prefix"}
 {:Contents []}
 [])

(def-list-s3-objects-test test-error
 {:bucket "test-bucket" :prefix "test-prefix"}
 {:Error {:Code "AccessDenied"}}
 [(str (Exception. "AccessDenied"))])
