(ns bq-runner.test-server
  "Test server helper - spawns bq-runner process with --stdio flag."
  (:require [bq-runner.client :as client]
            [clojure.java.io :as io]))

(defn- find-binary-path []
  (or (System/getenv "BQ_RUNNER_BINARY")
      (let [cwd (System/getProperty "user.dir")
            project-root (if (re-find #"/adaptors/clojure$" cwd)
                           (clojure.string/replace cwd #"/adaptors/clojure$" "")
                           cwd)]
        (str project-root "/target/debug/bq-runner"))))

(def ^:private binary-path (find-binary-path))

(defn ensure-binary!
  "Ensures the bq-runner binary exists. Throws if not found."
  []
  (when-not (.exists (io/file binary-path))
    (throw (ex-info (str "bq-runner binary not found at: " binary-path
                         "\nRun 'cargo build' first or set BQ_RUNNER_BINARY env var")
                    {:binary-path binary-path})))
  binary-path)

(defn get-binary-path
  "Returns the path to the bq-runner binary."
  []
  (ensure-binary!))

(defn create-connection
  "Creates a new connection by spawning a bq-runner process.
   Each connection gets its own process with isolated state."
  []
  (client/connect (get-binary-path)))
