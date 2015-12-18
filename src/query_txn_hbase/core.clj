(ns query-txn-hbase.core
  (:require [cbass :refer [new-connection]]
            [clojure.java.io :as io]
            [clojure.tools.cli :refer [parse-opts]]
            [environ.core :refer [env]]
            [query-txn-hbase.query
             :refer
             [scan-timestamps write-seqnum-ts-msgtimestamp]])
  (:gen-class))

(def ^:private cli-options
  ;; the options for this app
  [["-f" "--file FILE" "Output filepath." :default "out-file.csv"]
   ["-h" "--help" "Display help."]])

(defn- display-help
  "Displays help and exits."
  [summary]
  (println summary)
  (System/exit 0))

(defn- display-errors
  "Displays errors and exits with 1"
  [errors]
  (println errors)
  (System/exit 1))

(defn- read-config
  [file]
  (read-string (slurp (io/resource file))))

(def ^:private hbase-config (if-let [conf (env :hbase-config)]
                    conf
                    (:hbase-config (read-config "config.edn"))))

(def conn (new-connection (into {}
                                (map (fn [[k v]] [(name k) v]) hbase-config))))

(defn- write-timestamps
  [file]
  (with-open [out-file (io/writer file)]
    (doseq [row-timestamps (scan-timestamps conn)]
      (write-seqnum-ts-msgtimestamp out-file row-timestamps))))

(defn- run-main
  [file]
  (println "*** Start of job ***")
  (time
   (write-timestamps file))
  (println "********************"))

(defn -main
  "Run query to extract timestamps. Takes output file as an argument."
  [& args]
  (let [{:keys [options summary errors]} (parse-opts args cli-options)]
    (cond
      errors          (display-errors errors)
      (:help options) (display-help summary)
      :else           (when-let [{:keys [file]} options]
                        (run-main file)))))
