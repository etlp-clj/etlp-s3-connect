(ns etlp-s3-connect.core-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :as a]
            [clojure.java.io :as io]
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


(defmacro det-list-s3-objects-test [name input response expected-output]
  `(clojure.test/deftest ~name
     (let [client# (cognitect.aws.client.test-double/client {:api :s3
                                                             :ops {:ListObjectsV2 (fn [params-map#]
                                                                                    ~response)}})
           files-chan# (a/chan)]
       (list-objects-pipeline (assoc ~input :client client# :files-channel files-chan#))
       (let [results# (a/<!! (a/into [] files-chan#))]
         (is (= results# ~expected-output))))))


(def-s3-reducible-test s3-reducible-test-1 "1\n2\n3\n" [2 3 4]
  (comp
    (map (fn [d] (Integer/parseInt d)))
    (filter numeric-line?)
    (map inc)))

(def-s3-reducible-test s3-reducible-test-2 "1\n2\n3\n" [3 4 5]
  (comp
    (map (fn [d] (Integer/parseInt d)))
    (filter numeric-line?)
    (map inc)
    (map inc)))

(det-list-s3-objects-test
 test-single-file
 {:bucket "test-bucket" :prefix "test-prefix"}
 {:Contents [{:Key "test-file-1"}]}
 [{:Key "test-file-1"}])

(det-list-s3-objects-test
 test-multiple-files
 {:bucket "test-bucket" :prefix "test-prefix"}
 {:Contents [{:Key "test-file-1"} {:Key "test-file-2"} {:Key "test-file-3"}]}
 [{:Key "test-file-1"} {:Key "test-file-2"} {:Key "test-file-3"}])

(det-list-s3-objects-test
 test-no-files
 {:bucket "test-bucket" :prefix "test-prefix"}
 {:Contents []}
 [])

(det-list-s3-objects-test
 test-error
 {:bucket "test-bucket" :prefix "test-prefix"}
 (throw (Exception. "AccessDenied"))
 {:Error {:Code "AccessDenied"}})
