(ns query-txn-hbase.query
  (:require [cbass :refer [find-by store new-connection pack-un-pack result-value result-key hdata->map scan get-table without-ts delete-by]]
            [cbass.scan :refer [scan-filter]]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.string :as str])
  (:import org.apache.hadoop.hbase.HBaseConfiguration
           [org.apache.hadoop.hbase.client Get HTable HTableInterface Put Scan Result]
           org.apache.hadoop.hbase.filter.ColumnPrefixFilter
           org.apache.hadoop.hbase.filter.CompareFilter
           org.apache.hadoop.hbase.filter.RegexStringComparator
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

(defn results->version-map [results row-key-fn]
  (for [r results]
    [(row-key-fn (.getRow r))
     (hdata->version-map r)]))

(defn without-ts-seq [results]
  (for [[k v] results]
    [k (dissoc v :last-updated)]))

(defn iter->map [iter row-key-fn]
  (let [r (.next iter)]
    [(row-key-fn (.getRow r))
     (hdata->version-map r)]))

(defn my-scan [conn table row-fn & {:keys [row-key-fn limit with-ts] :as criteria}]
  (with-open [^HTableInterface h-table (get-table conn table)]
    (let [iter (.iterator (.getScanner h-table (scan-filter criteria)))]
      (loop []
       (when (.hasNext iter)
         (let [row (iter->map iter #(String. %))]
            (row-fn row)
            (recur)))))))

(defn- my-scan-timestamp-rows
  "Scan account-txns for timestamps returning a map of the results keyed by row key value as a string."
  [conn row-fn]
  (let [filter (ColumnPrefixFilter. (.getBytes "MSG_TIMESTAMP"))]
    (my-scan conn "account-txns" row-fn  :filter filter)))

(defn- scan-timestamp-rows
  "Scan account-txns for timestamps returning a map of the results keyed by row key value as a string."
  [conn]
  (let [filter (ColumnPrefixFilter. (.getBytes "MSG_TIMESTAMP"))]
    (with-redefs [cbass/hdata->map hdata->version-map
                  cbass/without-ts without-ts-seq]
      (scan conn "account-txns" :filter filter :lazy? true))))

(defn- key->seqnum
  "Extracst the seqnum from the key (column name) and returns the seqnum as a String."
  [k]
  (second (re-find #"MSG_TIMESTAMP[_|A-Z]*-([0-9]+)" k)))

(defn- column-type
  "Return a keyword with the column type (either :msg-timestamp or :storm-timestamp"
  [k]
  (case (second (re-find #".*(STORM.*-).*" k))
    "STORM-"      :storm-timestamp
    "STORMHBASE-" :stormhbase-timestamp
    nil           :msg-timestamp))

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
        {:keys [msg-timestamp storm-timestamp stormhbase-timestamp]
         :or {storm-timestamp [0] stormhbase-timestamp [0]}} timestamp-map]
    [seqnum (first msg-timestamp) (first storm-timestamp) (first stormhbase-timestamp) hbase-timestamp]))

(defn row-to-seqnum-ts
  "Take a row from HBase and extract the seqnum and the msg, storm and hbase timestamps and return them in a vector."
  [row]
  (map extract-seqnum-and-timestamps (merge-timestamps-by-seqnum row)))

(defn print-row [row]
  (let [row-values (second row)]
    (println (row-to-seqnum-ts row-values))))

(defn write-row [out-file]
  (fn [row]
    (let [row-values (second row)
          seqnum-ts  (row-to-seqnum-ts row-values)]
      (csv/write-csv out-file seqnum-ts))))

(defn scan-timestamps
  "Scan account-txns for timestamps returning a sequence of tuples of [seqnum [hbase-timestamp message-timestamp]]"
  [conn]
  (let [row-values (map second (scan-timestamp-rows conn))]
    (map row-to-seqnum-ts row-values)))


(defn write-seqnum-ts-msgtimestamp
  [out-file timestamp-seq]
  (csv/write-csv out-file timestamp-seq))

(defn write-seqnum-ts-msgtimestamp-lazy
  [out-file conn]
  (my-scan-timestamp-rows conn (write-row out-file)))

(defn- regex-filter
  [regex]
  (org.apache.hadoop.hbase.filter.RowFilter.
   (org.apache.hadoop.hbase.filter.CompareFilter$CompareOp/EQUAL)
   (org.apache.hadoop.hbase.filter.RegexStringComparator. regex)))

(defn delete-by-regex
  [conn regex-seq]
  (let [filters (map regex-filter regex-seq)]
    (map #(delete-by conn "account-txns" :filter %) filters)))

(comment

  (with-open [out-file (io/writer "test-out.csv")]
    (my-scan-timestamp-rows query-txn-hbase.core/conn (write-row out-file)))

  (delete-by-regex query-txn-hbase.core/conn ["[0-9]-2015-10-.*"])

  (scan query-txn-hbase.core/conn "account-txns" :with-ts true)

  (cbass/seq-scan query-txn-hbase.core/conn "account-txns")

  (scan-timestamp-rows query-txn-hbase.core/conn)

  (scan-timestamps query-txn-hbase.core/conn)

  (let [time (- (System/currentTimeMillis) 1000)
        second-time (+ 1000 time)
        account-txn-keys ["1-2015-09-01"
                          "1-2015-09-30"
                          "1-2015-10-02"
                          "1-2015-10-31"
                          "1-2016-09-30"
                          "2-2015-10-13"
                          "100-2015-10-30"
                          "100-2016-09-30"
                          "200000-2015-10-02"
                          "200001-2015-10-02"
                          "testrow1"]]
    (map
     #(store query-txn-hbase.core/conn "account-txns" % "s" {:MSG_TIMESTAMP-201 (Bytes/toBytes time) :MSG_TIMESTAMP_STORM-201 (Bytes/toBytes second-time) :MSG_TIMESTAMP-202 (Bytes/toBytes time) :MSG_TIMESTAMP_STORM-202 (Bytes/toBytes second-time) :MSG_TIMESTAMP-203 (Bytes/toBytes time) :MSG_TIMESTAMP_STORM-203 (Bytes/toBytes second-time)})
     account-txn-keys))

  ;; data for:
  ;;
  ;; 1-2015-09-01
  ;; 1-2015-09-30
  ;; 1-2015-10-02 d
  ;; 1-2015-10-31 d
  ;; 1-2016-09-30
  ;; 2-2015-10-13 d
  ;; 100-2015-10-30 d
  ;; 100-2016-09-30
  ;; 200000-2015-10-02 d
  ;; 200001-2015-10-02
  ;; testrow1

  ;; Query to count the number of messages in the account-txns table.
  (defn count-txns []
    (let [filter (org.apache.hadoop.hbase.filter.RowFilter.
                  (.. org.apache.hadoop.hbase.filter.CompareFilter. CompareOp EQUAL)
                  (org.apache.hadoop.hbase.filter.RegexStringComparator. "[1"))]
      (reduce (fn [cnt x] (+ (-> x second keys count) cnt)) 0
              (cbass/scan query-txn-hbase.core/conn "account-txns" :filter filter))))


  ;; Query to count the number of rows with more that one seqnum col in the account-txns table.
  (defn txns-gt-1-seqnum []
    (let [filter (org.apache.hadoop.hbase.filter.ColumnPrefixFilter. (.getBytes "SEQNUM"))]
      (reduce (fn [cnt x] (+ (if (< 1 (-> x second keys count))) cnt)) 0
              (cbass/scan query-txn-hbase.core/conn "account-txns" :filter filter))))

  )
