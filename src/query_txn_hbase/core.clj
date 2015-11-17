(ns query-txn-hbase.core
  (:require [query-txn-hbase.query :as q])
  (:gen-class))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "*** Start of job ***")
  (println "*****" (q/query-txn q/txn-table "testrow" "statement_data" "accnum"))
  (println "********************"))
