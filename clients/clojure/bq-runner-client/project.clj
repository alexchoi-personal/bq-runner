(defproject bq-runner-client "0.1.0"
  :description "Clojure client for bq-runner with Arrow IPC support"
  :url "https://github.com/alexchoi0/bq-runner"
  :license {:name "MIT"
            :url "https://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [org.apache.arrow/arrow-vector "15.0.0"]
                 [org.apache.arrow/arrow-memory-netty "15.0.0"]
                 [org.apache.arrow/arrow-memory-unsafe "15.0.0"]
                 [aleph "0.6.4"]
                 [cheshire "5.12.0"]
                 [manifold "0.4.2"]]
  :profiles {:dev {:dependencies [[criterium "0.4.6"]
                                  [org.clojure/test.check "1.1.1"]]}}
  :source-paths ["src"]
  :test-paths ["test"]
  :java-source-paths []
  :javac-options ["-target" "11" "-source" "11"]
  :jvm-opts ["-Dio.netty.tryReflectionSetAccessible=true"
             "--add-opens" "java.base/java.nio=ALL-UNNAMED"])
