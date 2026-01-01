(ns bq-runner.client
  "Process-based connection management for bq-runner using stdio"
  (:require [clojure.data.json :as json])
  (:import [java.io BufferedReader InputStreamReader PrintWriter]
           [java.util.concurrent LinkedBlockingQueue TimeUnit]))

(defprotocol IConnection
  (send-request* [conn method params timeout-ms])
  (close* [conn])
  (connected?* [conn]))

(defrecord ProcessConnection [^Process process
                              ^BufferedReader reader
                              ^PrintWriter writer
                              pending-responses
                              closed?
                              reader-thread])

(defn- generate-id []
  (str (java.util.UUID/randomUUID)))

(defn- make-request [method params]
  {:jsonrpc "2.0"
   :method method
   :params params
   :id (generate-id)})

(defn- start-reader-thread [conn]
  (let [reader (:reader conn)
        pending (:pending-responses conn)
        closed? (:closed? conn)]
    (doto (Thread.
           (fn []
             (try
               (loop []
                 (when-not @closed?
                   (when-let [line (.readLine reader)]
                     (let [response (json/read-str line :key-fn keyword)
                           id (:id response)]
                       (when-let [queue (get @pending id)]
                         (.put ^LinkedBlockingQueue queue response)
                         (swap! pending dissoc id)))
                     (recur))))
               (catch Exception _
                 (reset! closed? true)))))
      (.setDaemon true)
      (.start))))

(defn connect
  "Connect to bq-runner by spawning a process with --transport stdio flag.
   binary-path: path to the bq-runner binary"
  [binary-path]
  (let [process-builder (ProcessBuilder. [binary-path "--transport" "stdio"])
        _ (.redirectErrorStream process-builder false)
        process (.start process-builder)
        stdin (.getOutputStream process)
        stdout (.getInputStream process)
        reader (BufferedReader. (InputStreamReader. stdout))
        writer (PrintWriter. stdin true)
        pending (atom {})
        closed? (atom false)
        conn (->ProcessConnection process reader writer pending closed? nil)
        reader-thread (start-reader-thread conn)]
    (assoc conn :reader-thread reader-thread)))

(defn close
  "Close the process connection."
  [conn]
  (reset! (:closed? conn) true)
  (.destroy ^Process (:process conn)))

(defn connected?
  "Check if connection is still open."
  [conn]
  (and (not @(:closed? conn))
       (.isAlive ^Process (:process conn))))

(defn send-request
  "Send a JSON-RPC request and wait for response.
   Returns the result or throws on error."
  ([conn method params]
   (send-request conn method params 30000))
  ([conn method params timeout-ms]
   (when @(:closed? conn)
     (throw (ex-info "Connection is closed" {:method method})))
   (let [request (make-request method params)
         id (:id request)
         queue (LinkedBlockingQueue. 1)
         writer ^PrintWriter (:writer conn)]
     (swap! (:pending-responses conn) assoc id queue)
     (try
       (locking writer
         (.println writer (json/write-str request))
         (.flush writer))
       (if-let [response (.poll queue timeout-ms TimeUnit/MILLISECONDS)]
         (if-let [error (:error response)]
           (throw (ex-info (:message error)
                           {:code (:code error)
                            :method method
                            :params params}))
           (:result response))
         (throw (ex-info "Request timed out"
                         {:method method
                          :timeout-ms timeout-ms})))
       (finally
         (swap! (:pending-responses conn) dissoc id))))))
