(defproject consumption-kstreams-ktable "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [org.clojure/data.json "0.2.6"]
                 [org.apache.kafka/kafka-streams "2.0.0"]
                 [org.apache.kafka/kafka-clients "2.0.0"]
                 [org.apache.kafka/kafka-streams-test-utils "2.0.0"]
                 [org.apache.kafka/kafka-clients "1.1.0" :classifier "test"]
                 [org.clojure/tools.logging "0.4.0"]
                 [org.slf4j/slf4j-log4j12 "1.7.1"]
                 [log4j/log4j "1.2.17"]
                 [bigsy/clj-nippy-serde "0.1.0"]]
  :repositories [["confluent"  {:url "https://packages.confluent.io/maven/"}]])