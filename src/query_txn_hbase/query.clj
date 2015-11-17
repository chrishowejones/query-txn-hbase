(ns query-txn-hbase.query
  (:require [cbass :refer [find-by scan new-connection pack-un-pack]]
            [taoensso.nippy :as nippy])
  (:import org.apache.hadoop.conf.Configuration
           [org.apache.hadoop.hbase.client Get HConnection HTableInterface Put Result Scan]
           org.apache.hadoop.hbase.HBaseConfiguration
           org.apache.hadoop.hbase.util.Bytes
           org.apache.hadoop.hbase.client.HTable))

(defn unpack
  [b]
  (Bytes/toLong b))

(defn pack
  [x]
  (Bytes/toBytes x))

;; replace pack-un-pack serialisation with mine
(pack-un-pack {:p pack :u unpack})

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


(defn scan-timestamps
  "Scan account-txns for timestamps"
  []
  (scan conn "account-txns" :starts-with "test" :columns #{"test1"}))


(comment

  (doseq [row (scan-timestamps)]
    (let [key (first row)]
      (println "key=" key "cols=" (second row))))

  (Bytes/to (:test2 (find-by conn "account-txns" "testrow3")))

  (scan-timestamps txn-table)
  (->
   (find-by conn "account-txns" "testrow4" "statement_data" #{"test2"})
   :test2
   bytes
   (Bytes/toLong))

  (query-txn txn-table "testrow3" "statement_data" "test2")
  (let [value 1234567]
    (put-txn txn-table "testrow4" [{:column-family "statement_data" :columns [                                                                             {:column "test2" :value value}]}]))


  (Bytes/toLong (Bytes/toBytes (.longValue (java.lang.Long. 12345))))

  (type 1234567)

  )
