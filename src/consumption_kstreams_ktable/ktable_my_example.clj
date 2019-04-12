(ns consumption-kstreams-ktable.ktable-my-example
  (:require [clojure.tools.logging :as log]
            [clj-nippy-serde.serialization :as nser])
  (:import (org.apache.kafka.streams StreamsBuilder KeyValue)
           (org.apache.kafka.streams.kstream KStream ValueJoiner KeyValueMapper Reducer ForeachAction Consumed)
           (org.apache.kafka.common.serialization Serdes)))

(def nippy-serde (nser/nippy-serde))

(def tariffs-topic "tariffs-topic")
(def consumptions-topic "consumptions-topic")
(def contracts-topic "contracts-topic")
(def charges-topic "charges")


(defn ^KStream contract-stream
  [builder input-topic]
  (.stream builder input-topic (Consumed/with (Serdes/String) nippy-serde)))

(defn ^KStream tariff-stream
  [builder input-topic]
  (.stream builder input-topic (Consumed/with (Serdes/String) nippy-serde)))

(defn ^KStream consumption-stream
  [builder input-topic]
  (.stream builder input-topic (Consumed/with (Serdes/String) nippy-serde)))

(defn process
  [^KStream contract-stream ^KStream tariff-stream ^KStream consumption-stream]
  (let [tariff_ktable          (-> tariff-stream
                                   (.groupByKey)
                                   (.reduce (reify Reducer
                                              (apply [_ left right]
                                                ((fn [? ?]
                                                   ;;We need to aggregate tariffs as some have the same key
                                                   (list left right)) left right)))))

        contract-tariff_ktable (-> contract-stream
                                   (.map (reify KeyValueMapper
                                           (apply [_ k v]
                                             ((fn [_ contracts]
                                                ;rekey contracts
                                                (let [value (KeyValue.
                                                              (:tariffId contracts)
                                                              contracts)]
                                                  value)) k v))))
                                   (.leftJoin tariff_ktable
                                              (reify ValueJoiner
                                                (apply [_ left right]
                                                  ((fn [contract tariff]
                                                     ;;Merge contract and possible tariffs
                                                     {:contract contract :tariffs tariff}) left right))))
                                   (.map (reify KeyValueMapper
                                           (apply [_ k v]
                                             ((fn [_ contracts-with-tariffs]
                                                ;;Partition contracs+tariffs by mpxn to join with consumption
                                                (let [value (KeyValue.
                                                              (get-in contracts-with-tariffs [:contract :mpxn])
                                                              contracts-with-tariffs)]
                                                  value)) k v))))
                                   (.groupByKey)
                                   (.reduce (reify Reducer
                                              (apply [_ left right]
                                                ((fn [? ?]
                                                   ;fake reduce->ktable
                                                   left right))))))]
    (-> consumption-stream
        (.leftJoin contract-tariff_ktable
                   (reify ValueJoiner
                     (apply [_ left right]
                       ((fn [consumption contract-tariff]
                          (let [consumption  (:consumption consumption)
                                tariffs      (:tariffs contract-tariff)
                                tariff       (if (list? tariffs) (last tariffs) tariffs) ;variable tariff date logic here
                                rate         (:rate tariff)
                                chargeAmount (* rate consumption)]
                            {:amount     chargeAmount
                             :contractId (get-in contract-tariff [:contract :contractId])})
                          ) left right)))))))

(defn peek-stream
  [stream]
  (.peek stream
         (reify ForeachAction
           (apply [_ k v]
             (log/infof "CHARGES: Key: %s, Data: %s" k v)))))

(defn build-join-topology
  []
  (let [builder      (StreamsBuilder.)
        contracts    (contract-stream builder contracts-topic)
        tariffs      (tariff-stream builder tariffs-topic)
        consumptions (consumption-stream builder consumptions-topic)]
    (-> (process contracts tariffs consumptions)
        ;(.toStream)
        peek-stream
        (.to charges-topic))
    builder))



