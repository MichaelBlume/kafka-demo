(ns kafka-demo.Main
  (:gen-class)
  (:import (kafka.consumer Consumer ConsumerConfig)
           (java.util Properties)))

(defn- make-props
  "convert a clojure map into a Properties object."
  [m]
  (let [props (Properties.)]
    (doseq [[k v] m]
      (.put props k (str v)))
    props))

(defn- get-streams-map [conf topics]
  (-> conf make-props ConsumerConfig.
    Consumer/createJavaConsumerConnector
    (.createMessageStreams topics)))

(defn- get-streams [props topic total-partitions]
  (for [i (range total-partitions)]
    (-> props
      (get-streams-map {topic (int 1)})
      (.get topic)
      first)))

(defn count-events [props topic total-partitions]
  (let [counter (atom 0)]
    (doseq [stream (get-streams props topic total-partitions)]
      (.start
        (Thread.
          #(doseq [m stream]
             (swap! counter inc)))))
    (Thread/sleep 15000)
    @counter))

(defn -main [group topic partition-count]
  (let [props {"zk.connect"                  "localhost:2181"
               "zk.connectiontimeout.ms"     1000000
               "groupid"                     group,
               "fetch.size"                  2097152,
               "socket.receive.buffer.bytes" 65536,
               "auto.commit.interval.ms"     60000,
               "queued.max.messages"         10}
        result (count-events props topic (read-string partitiont-count))]
    (prn result)))
