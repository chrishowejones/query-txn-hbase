(ns query-txn-hbase.core
  (:require [clojure.java.io :as io]
            [query-txn-hbase.query
             :refer
             [scan-timestamps write-seqnum-ts-msgtimestamp]])
  (:gen-class))

(defn write-timestamps
  []
  (with-open [out-file (io/writer "out-file.csv")]
    (doseq [row-timestamps (scan-timestamps)]
      (write-seqnum-ts-msgtimestamp out-file row-timestamps))))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "*** Start of job ***")
  (time
   (write-timestamps))
  (println "********************"))
