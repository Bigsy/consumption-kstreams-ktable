(ns consumption-kstreams-ktable.ktable-my-example-test
  (:require [consumption-kstreams-ktable.ktable-my-example :as sut]
            [clojure.test :refer [deftest testing is]]
            [clojure.tools.logging :as log])
  (:import org.apache.kafka.common.serialization.Serdes
           [org.apache.kafka.streams StreamsConfig TopologyTestDriver]
           org.apache.kafka.streams.test.ConsumerRecordFactory
           org.apache.kafka.test.TestUtils))

(def properties
  (let [properties (java.util.Properties.)]
    (.put properties StreamsConfig/APPLICATION_ID_CONFIG "thingy")
    (.put properties StreamsConfig/BOOTSTRAP_SERVERS_CONFIG "dummy:9092")
    (.put properties StreamsConfig/DEFAULT_KEY_SERDE_CLASS_CONFIG (.getName (.getClass (Serdes/String))))
    (.put properties StreamsConfig/DEFAULT_VALUE_SERDE_CLASS_CONFIG (.getName (.getClass sut/nippy-serde)))
    (.put properties StreamsConfig/COMMIT_INTERVAL_MS_CONFIG (* 10 1000))
    (.put properties StreamsConfig/STATE_DIR_CONFIG (.getAbsolutePath (. TestUtils tempDirectory)))
    properties))

(deftest tariff-contract-consumption->charge
  (testing "Kafka Streams with KTABLE"
    (let [topology             (.build (sut/build-join-topology))
          topology-test-driver (TopologyTestDriver. topology properties)
          str-serializer       (.serializer (. Serdes String))
          str-deserializer     (.deserializer (. Serdes String))
          nippy-serializer     (.serializer sut/nippy-serde)
          nippy-deserializer   (.deserializer sut/nippy-serde)
          factory              (ConsumerRecordFactory. str-serializer nippy-serializer)
          contracts-topic      sut/contracts-topic
          tariffs-topic        sut/tariffs-topic
          consumption-topic    sut/consumptions-topic
          charges-topic        sut/charges-topic
          create               (fn [topic k v] (.pipeInput topology-test-driver (.create factory topic k v)))]

      ;load tariffs
      (create tariffs-topic "tariffid-1" {:tariffId "tariffid-1" :version 1 :rate 10})
      (create tariffs-topic "tariffid-2" {:tariffId "tariffid-2" :version 1 :rate 11})
      (create tariffs-topic "tariffid-2" {:tariffId "tariffid-2" :version 2 :rate 12})
      (create tariffs-topic "tariffid-2" {:tariffId "tariffid-2" :version 3 :rate 13})

      ;load contracts
      (create contracts-topic "contractid-1" {:contractId "contractid-1" :tariffId "tariffid-1" :mpxn "mpxn-1"})
      (create contracts-topic "contractid-2" {:contractId "contractid-2" :tariffId "tariffid-2" :mpxn "mpxn-2"})
      (create contracts-topic "contractid-3" {:contractId "contractid-3" :tariffId "tariffid-1" :mpxn "mpxn-3"})

      ;stream consumption
      (create consumption-topic "mpxn-1" {:consumption 10 :mpxn "mpxn-1"})
      (create consumption-topic "mpxn-2" {:consumption 20 :mpxn "mpxn-2"})
      (create consumption-topic "mpxn-3" {:consumption 20 :mpxn "mpxn-3"})

      (let [output (fn [] (.readOutput topology-test-driver charges-topic str-deserializer nippy-deserializer))]
        ;Generated charges
        (is (= {:amount 100, :contractId "contractid-1"} (.value (output))))
        (is (= {:amount 260, :contractId "contractid-2"} (.value (output))))
        (is (= {:amount 200, :contractId "contractid-3"} (.value (output))))))))