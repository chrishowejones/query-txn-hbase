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

  (re-find #"MSG_TIMESTAMP[_|A-Z]*-([0-9]*)" "MSG_TIMESTAMP_STORM-123")

  (with-open [out-file (io/writer "out-file.csv")]
    (doseq [row-timestamps (scan-timestamps)]
      (write-seqnum-ts-msgtimestamp out-file row-timestamps)))

  (map second (scan-timestamp-rows query-txn-hbase.core/conn))

  (map (fn [[k v]] v) (scan-timestamp-rows query-txn-hbase.core/conn))

  (let [row-values (map second (scan-timestamp-rows query-txn-hbase.core/conn))]
    (group-row-ts-by-seqnum row-values))

  (defn- column->seqnum2
  "Map across a rows worth of columns and return a sequence of vectors of timestamps with the sequence numbers extracted"
  [row]
  row)

  (let [row-values (map second (scan-timestamp-rows query-txn-hbase.core/conn))]
    (map merge-timestamps-by-seqnum row-values))

  (let [rows (map second (scan-timestamp-rows query-txn-hbase.core/conn))
        seq-num-map (fn [row] (map (fn [[k v]] {(key->seqnum k) {(column-type k) v}}) row))
        merge-seq-num-maps (fn [row] (apply merge-with merge (seq-num-map row)))]
    (map merge-seq-num-maps rows))

  (def one-row
    {"123"
     [["123" :msg-timestamp [1448973012159 1448973013490]]
      ["123" :storm-timestamp [1448973013159 1448973013490]]],
     "124"
     [["124" :msg-timestamp [1448973012159 1448973013490]]
      ["124" :storm-timestamp [1448973013159 1448973013490]]],
     "125"
     [["125" :msg-timestamp [1448973012159 1448973013490]]
      ["125" :storm-timestamp [1448973013159 1448973013490]]]})

  (map (fn [[k v]]
         (map v))
       one-row)

  (scan-timestamp-rows query-txn-hbase.core/conn)
  (scan-timestamps query-txn-hbase.core/conn)

  (find-by conn "account-txns" "testrow4")

  (let [time (System/currentTimeMillis)
        second-time (+ 1000 time)]
    (store query-txn-hbase.core/conn "account-txns" "testrow5" "s" {:MSG_TIMESTAMP-101 (Bytes/toBytes time) :MSG_TIMESTAMP_STORM-101 (Bytes/toBytes second-time) :MSG_TIMESTAMP-102 (Bytes/toBytes time) :MSG_TIMESTAMP_STORM-102 (Bytes/toBytes second-time) :MSG_TIMESTAMP-103 (Bytes/toBytes time) :MSG_TIMESTAMP_STORM-103 (Bytes/toBytes second-time)}))

  (println "String")

  )
