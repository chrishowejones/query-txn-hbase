(ns query-txn-hbase.query
  (:require [cbass :refer [find-by store new-connection pack-un-pack result-value result-key results->map scan]]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io])
  (:import org.apache.hadoop.hbase.HBaseConfiguration
           [org.apache.hadoop.hbase.client Get HTable Put Scan Result]
           org.apache.hadoop.hbase.filter.ColumnPrefixFilter
           org.apache.hadoop.hbase.util.Bytes))

(defn unpack
  [b]
  (Bytes/toLong b))

(defn pack
  [x]
  (Bytes/toBytes x))

;; replace pack-un-pack serialisation with mine
(pack-un-pack {:p identity :u unpack})

(defn ts-result-value [kv]
  (let [[ts val] (first (val kv))]
    [(unpack val) ts]))

(defn hdata->version-map [^Result data]
  (when-let [r (.getRow data)]
    (into {} (for [kv (-> (.getMap data) vals first)]
               (if-some [v (ts-result-value kv)]
                 [(String. (key kv)) v])))))

(defn- scan-timestamp-rows
  "Scan account-txns for timestamps returning a map of the results keyed by row key value as a string."
  [conn]
  (let [filter (ColumnPrefixFilter. (.getBytes "MSG_TIMESTAMP"))]
    (with-redefs [cbass/hdata->map hdata->version-map]
      (scan conn "account-txns" :filter filter))))

(defn- key->seqnum
  "Extracst the seqnum from the key (column name) and returns the seqnum as a String."
  [k]
  (second (re-find #"MSG_TIMESTAMP-([0-9]*)" k)))

(defn scan-timestamps
  "Scan account-txns for timestamps returning a sequence of tuples of [seqnum [hbase-timestamp message-timestamp]]"
  [conn]
  (let [row-values (map (fn [[k v]] v) (scan-timestamp-rows conn))]
    (map (fn [m] (for [[k v] m] (flatten [(key->seqnum k) v]))) row-values)))

(defn write-seqnum-ts-msgtimestamp
  [out-file timestamp-seq]
  (csv/write-csv out-file timestamp-seq))

(comment


  (with-open [out-file (io/writer "out-file.csv")]
    (doseq [row-timestamps (scan-timestamps)]
      (write-seqnum-ts-msgtimestamp out-file row-timestamps)))

  (scan-timestamps)
  (scan-timestamp-rows)

  (find-by conn "account-txns" "testrow4")

  (let [time (System/currentTimeMillis)]
   (store conn "account-txns" "testrow4" "s" {:MSG_TIMESTAMP-123 (Bytes/toBytes time) :MSG_TIMESTAMP-124 (Bytes/toBytes time) :MSG_TIMESTAMP-125 (Bytes/toBytes time)}))

  )
