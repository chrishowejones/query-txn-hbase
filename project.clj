(defproject query-txn-hbase "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [cbass "0.1.4-SNAPSHOT"]
                 [com.taoensso/nippy "2.10.0"]
                 [org.apache.hadoop/hadoop-client "2.7.1.2.3.0.0-2557"
                  :exclusions [[org.slf4j/slf4j-log4j12]]]
                 [org.apache.hbase/hbase-client "1.1.1.2.3.0.0-2557"
                  :exclusions [[org.slf4j/slf4j-log4j12]]]]
  :resource-paths ["resources"]
  :main ^:skip-aot query-txn-hbase.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}}
  :repositories [["HDPReleases" "http://nexus-private.hortonworks.com/nexus/content/groups/public/"]])
