(ns query-txn-hbase.core
  (:require [clojure.java.io :as io]
            [query-txn-hbase.query
             :refer
             [scan-timestamps write-seqnum-ts-msgtimestamp]]
            [cbass :refer [new-connection]]
            [environ.core :refer [env]])
  (:gen-class))

(defn- read-config
  [file]
  (read-string (slurp (io/file (io/resource file)))))

(def hbase-config (if-let [conf (env :hbase-config)]
                    conf
                    (:hbase-config (read-config "config.edn"))))

(def conn (new-connection (into {}
                                (map (fn [[k v]] [(name k) v]) hbase-config))))

(defn write-timestamps
  []
  (with-open [out-file (io/writer "out-file.csv")]
    (doseq [row-timestamps (scan-timestamps conn)]
      (write-seqnum-ts-msgtimestamp out-file row-timestamps))))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "*** Start of job ***")
  (time
   (write-timestamps))
  (println "********************"))
