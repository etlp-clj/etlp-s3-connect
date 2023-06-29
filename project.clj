(defproject org.clojars.aregee/etlp-s3-connect "0.1.1-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [com.cognitect.aws/endpoints "1.1.12.380"]
                 [com.cognitect.aws/s3 "825.2.1250.0"]
                 [com.cognitect.aws/api "0.8.635"]
                 [org.clojure/tools.logging "1.2.4"]
                 [org.clojure/core.async "0.4.500"]
                 [org.clojars.aregee/etlp "0.3.2-SNAPSHOT"]]
  :deploy-repositories {"snapshots" {:url "https://repo.clojars.org" :creds :gpg}}
  :repl-options {:init-ns etlp-s3-connect.core})
