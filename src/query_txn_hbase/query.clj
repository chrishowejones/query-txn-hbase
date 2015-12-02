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
  (second (re-find #"MSG_TIMESTAMP[_|A-Z]*-([0-9]+)" k)))

(defn- column-type
  "Return a keyword with the column type (either :msg-timestamp or :storm-timestamp"
  [k]
  (if (re-find #".*STORM.*" k)
    :storm-timestamp
    :msg-timestamp))

(defn- column-timestamps->seqnum-type-timestamp-map
  "Take a map with an entry per column name and a vector of timestamp and hbase timestamp and
   return a map with an entry per seqnum (extracted from the column name) and a value
   that's a map of timestamp type and the vector of timestamp, hbase timestamp.

    e.g.
       Input:

         {\"MSG_TIMESTAMP-123\" [1448973012159 1448973013490],
          \"MSG_TIMESTAMP-124\" [1448973012159 1448973013490],
          \"MSG_TIMESTAMP_STORM-123\" [1448973013159 1448973013490],
          \"MSG_TIMESTAMP_STORM-124\" [1448973013159 1448973013490]}

       Output:

         {\"123\" {:msg-timestamp [1448973012159 1448973013490]}}
         {\"123\" {:storm-timestamp [1448973012159 1448973013490]}}
         {\"124\" {:msg-timestamp [1448973012159 1448973013490]}}
         {\"124\" {:storm-timestamp [1448973012159 1448973013490]}}
  "
  [row]
  (map (fn [[k v]] {(key->seqnum k) {(column-type k) v}}) row))

(defn- merge-timestamps-by-seqnum
  "Take a row with a column name as the key and a vector of timestamp and hbase timestamp and
   return map with an entry per seqnum and a value of a map with an entry per timestamp type with a value of
   the vector of timestamps. E.g.
        Input:

         {\"MSG_TIMESTAMP-123\" [1448973012159 1448973013490],
          \"MSG_TIMESTAMP-124\" [1448973012159 1448973013490],
          \"MSG_TIMESTAMP_STORM-123\" [1448973013159 1448973013490],
          \"MSG_TIMESTAMP_STORM-124\" [1448973013159 1448973013490]}

        Output:

         {\"124\"
          {:storm-timestamp [1448973013159 1448973013490],
           :msg-timestamp [1448973012159 1448973013490]},
          \"123\"
          {:storm-timestamp [1448973013159 1448973013490],
           :msg-timestamp [1448973012159 1448973013490]}}"
  [row]
  (apply merge-with merge (column-timestamps->seqnum-type-timestamp-map row)))

(defn extract-seqnum-and-timestamps
  [seqnum-ts-map]
  (let [seqnum          (first seqnum-ts-map)
        timestamp-map   (-> seqnum-ts-map second)
        hbase-timestamp (-> timestamp-map first second second)
        {:keys [msg-timestamp storm-timestamp]} timestamp-map]
    [seqnum (first msg-timestamp) (first storm-timestamp) hbase-timestamp]))

(defn row-to-seqnum-ts
  "Take a row from HBase and extract the seqnum and the msg, storm and hbase timestamps and return them in a vector."
  [row]
  (map extract-seqnum-and-timestamps (merge-timestamps-by-seqnum row)))


(defn scan-timestamps
  "Scan account-txns for timestamps returning a sequence of tuples of [seqnum [hbase-timestamp message-timestamp]]"
  [conn]
  (let [row-values (map second (scan-timestamp-rows conn))]
    (map row-to-seqnum-ts row-values)))

(defn write-seqnum-ts-msgtimestamp
  [out-file timestamp-seq]
  (csv/write-csv out-file timestamp-seq))

(comment

  (scan-timestamp-rows query-txn-hbase.core/conn)
  (scan-timestamps query-txn-hbase.core/conn)

  (let [time (- (System/currentTimeMillis) 1000)
        second-time (+ 1000 time)]
    (store query-txn-hbase.core/conn "account-txns" "testrow4" "s" {:MSG_TIMESTAMP-201 (Bytes/toBytes time) :MSG_TIMESTAMP_STORM-201 (Bytes/toBytes second-time) :MSG_TIMESTAMP-202 (Bytes/toBytes time) :MSG_TIMESTAMP_STORM-202 (Bytes/toBytes second-time) :MSG_TIMESTAMP-203 (Bytes/toBytes time) :MSG_TIMESTAMP_STORM-203 (Bytes/toBytes second-time)}))

  )
