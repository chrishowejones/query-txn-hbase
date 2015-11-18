(ns query-txn-hbase.query
  (:require [cbass :refer [find-by store new-connection pack-un-pack result-value result-key scan]]
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
    [ts (unpack val)]))

(defn hdata->version-map [^Result data]
  (when-let [r (.getRow data)]
    (into {} (for [kv (-> (.getMap data) vals first)]
               (if-some [v (ts-result-value kv)]
                 [(String. (key kv)) v])))))



(def conn (new-connection {}))

(def config (HBaseConfiguration/create))

(def txn-table (HTable. config "account-txns"))

(defn get-txn [table key]
  (let [get (Get. (Bytes/toBytes key))]
    (-> table (.get get) (.list))))

(defn query-txn [table key cf cl]
  (let [get (Get. (Bytes/toBytes key))
        result (-> table (.get get))]
    (-> result
        (.getValue (Bytes/toBytes cf) (Bytes/toBytes cl))
        (Bytes/toString))))

(defn add-column
  [put cf-name]
  (fn [column-map]
    (let [col-name  (Bytes/toBytes (column-map :column))
          col-value (Bytes/toBytes (column-map :value))]
      (println (str "calling add for " cf-name col-name col-value))
      (.add put cf-name col-name col-value))))

(defn put-txn
  "Puts a txn row. Takes an HTable, rowkey and a sequence of map of column families and columns.
  e.g. [{:column-family \"columnFamily1\" :columns [{:column \"accnum\" :value 1}
  {:column \"balance\" :value 100.00}]}
  {:column-family \"columnFamily2\" :columns [{:column \"accnum\" :value 3}
  {:column \"balance\" :value 130.00}}]"
  [table key column-families]
  (let [put (Put. (Bytes/toBytes key))]
    (doseq [cf column-families]
      (let [cf-name (Bytes/toBytes (cf :column-family))
            columns (cf :columns)
            add-col (fn [column-map]
                      (let [col-name  (Bytes/toBytes (column-map :column))
                            col-value (byte-array (Bytes/toBytes (let [value (column-map :value)]
                                                         (println "type = " (type value))
                                                         (if (= java.lang.Long (type value))
                                                           (do (println "converting to long") "long")
                                                           value))))]
                        (println (str "calling add for " cf-name col-name col-value))
                        (.add put cf-name col-name col-value)))]
        (println "about to call map")
        (dorun (map add-col columns))))
    (.put table put)))

(defn- update-values [m f & args]
  (reduce (fn [r [k v]] (assoc r k (apply f v args))) {} m))

(defn- scan-timestamp-rows
  "Scan account-txns for timestamps returning a map of the results keyed by row key value as a string."
  []
  (let [filter (ColumnPrefixFilter. (Bytes/toBytes "MSG_TIMESTAMP"))
        scan (doto (Scan.) (.setFilter filter))
        scanner (.getScanner txn-table scan)
        results (iterator-seq (.iterator scanner))]
    (with-redefs [cbass/hdata->map hdata->version-map]
     (results->map results #(String. %)))))

(defn- key->seqnum
  "Extracst the seqnum from the key (column name) and returns the seqnum as a String."
  [k]
  (second (re-find #"MSG_TIMESTAMP-([0-9]*)" k)))

(defn scan-timestamps
  "Scan account-txns for timestamps returning a sequence of tuples of [seqnum [hbase-timestamp message-timestamp]]"
  []
  (let [row-values (map (fn [[k v]] v) (scan-timestamp-rows))]
    (map (fn [m] (for [[k v] m] (flatten [(key->seqnum k) v]))) row-values)))

(defn write-seqnum-ts-msgtimestamp
  [out-file timestamp-seq]
  (csv/write-csv out-file timestamp-seq))



(comment



  (dorun (map write-seqnum-ts-msgtimestamp (scan-timestamps)))


  (re-find (re-matcher  #"MSG_TIMESTAMP-([0-9]*)" "MSG_TIMESTAMP-123"))

  (type (scan-timestamp-rows))
  (scan-timestamps)

  (scan-timestamp-rows)

  (map type {"MSG_TIMESTAMP-123" [1447840632278 1234], "MSG_TIMESTAMP-124" [1447840632278 1235], "MSG_TIMESTAMP-125" [1447840632278 1236]})

  (doseq [row (scan-timestamp-rows)]
    (let [key (first row)]
      (println "key=" key "cols=" (second row))))

  (find-by conn "account-txns" "testrow4")

  (store conn "account-txns" "testrow5" "statement_data" {:MSG_TIMESTAMP-123 (Bytes/toBytes 1234) :MSG_TIMESTAMP-124 (Bytes/toBytes 1235) :MSG_TIMESTAMP-125 (Bytes/toBytes 1236)})

  (map associative? (map (fn [[k v]] v) (scan-timestamps)))


  (reduce (fn [m e] (assoc m "dummy")) (map (fn [[k v]] v) (scan-timestamps)))


  (query-txn txn-table "testrow3" "statement_data" "test2")

  (defn update-values [m f & args]
    (reduce (fn [r [k v]] (assoc r k (apply f v args))) {} m))

  (update-values {:a 1 :b 2 :c 3} #(Bytes/toBytes %))

  (map (fn [m] (update-values m #(Bytes/toLong %))) (map (fn [[k v]] v) (scan-timestamp-rows)))




  (Bytes/toLong (Bytes/toBytes (.longValue (java.lang.Long. 12345))))

  (type 1234567))
