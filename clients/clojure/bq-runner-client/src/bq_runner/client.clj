(ns bq-runner.client
  (:require [bq-runner.websocket :as ws]
            [bq-runner.arrow :as arrow]
            [bq-runner.protocol :as proto]
            [manifold.deferred :as d])
  (:import [org.apache.arrow.memory RootAllocator]))

(defn connect!
  [url & {:keys [timeout] :or {timeout 30000}}]
  (let [conn (ws/connect! url :timeout timeout)
        allocator (RootAllocator. Long/MAX_VALUE)]
    (assoc conn :allocator allocator)))

(defn close!
  [{:keys [allocator] :as client}]
  (ws/close! client)
  (when allocator
    (.close ^RootAllocator allocator)))

(defn ping
  [client]
  @(ws/send-rpc! client "bq.ping" {}))

(defn health
  [client]
  @(ws/send-rpc! client "bq.health" {}))

(defn create-session!
  [client]
  (let [result @(ws/send-rpc! client "bq.createSession" {})]
    (:sessionId result)))

(defn destroy-session!
  [client session-id]
  @(ws/send-rpc! client "bq.destroySession" {:sessionId session-id}))

(defn query
  [client session-id sql]
  @(ws/send-rpc! client "bq.query" {:sessionId session-id :sql sql}))

(defn query-arrow
  [client session-id sql]
  @(ws/send-rpc! client "bq.queryArrow" {:sessionId session-id :sql sql}))

(defn release-arrow-result!
  [client shm-path]
  @(ws/send-rpc! client "bq.releaseArrowResult" {:shmPath shm-path}))

(defn query-arrow->maps
  [{:keys [allocator] :as client} session-id sql]
  (let [result (query-arrow client session-id sql)
        shm-path (:shmPath result)]
    (try
      (arrow/with-shared-memory [shm shm-path]
        (let [batch (arrow/read-arrow-batch shm allocator)]
          (try
            (arrow/batch->maps batch)
            (finally
              (arrow/close-batch! batch)))))
      (finally
        (release-arrow-result! client shm-path)))))

(defn query-arrow->columns
  [{:keys [allocator] :as client} session-id sql]
  (let [result (query-arrow client session-id sql)
        shm-path (:shmPath result)]
    (try
      (arrow/with-shared-memory [shm shm-path]
        (let [batch (arrow/read-arrow-batch shm allocator)]
          (try
            (arrow/batch->columns batch)
            (finally
              (arrow/close-batch! batch)))))
      (finally
        (release-arrow-result! client shm-path)))))

(defn create-table!
  [client session-id table-name schema]
  @(ws/send-rpc! client "bq.createTable"
                 {:sessionId session-id
                  :tableName table-name
                  :schema schema}))

(defn insert!
  [client session-id table-name rows]
  @(ws/send-rpc! client "bq.insert"
                 {:sessionId session-id
                  :tableName table-name
                  :rows rows}))

(defn define-table!
  [client session-id name sql]
  @(ws/send-rpc! client "bq.defineTable"
                 {:sessionId session-id
                  :name name
                  :sql sql}))

(defn register-dag!
  [client session-id tables]
  @(ws/send-rpc! client "bq.registerDag"
                 {:sessionId session-id
                  :tables tables}))

(defn run-dag!
  [client session-id & {:keys [table-names retry-count]
                        :or {retry-count 0}}]
  @(ws/send-rpc! client "bq.runDag"
                 (cond-> {:sessionId session-id
                          :retryCount retry-count}
                   table-names (assoc :tableNames table-names))))

(defn get-dag
  [client session-id]
  @(ws/send-rpc! client "bq.getDag" {:sessionId session-id}))

(defn clear-dag!
  [client session-id]
  @(ws/send-rpc! client "bq.clearDag" {:sessionId session-id}))

(defmacro with-session
  [[client session-sym] & body]
  `(let [~session-sym (create-session! ~client)]
     (try
       ~@body
       (finally
         (destroy-session! ~client ~session-sym)))))

(defmacro with-arrow-result
  [[client session-id sql result-sym] & body]
  `(let [arrow-result# (query-arrow ~client ~session-id ~sql)
         shm-path# (:shmPath arrow-result#)]
     (try
       (arrow/with-shared-memory [shm# shm-path#]
         (let [batch# (arrow/read-arrow-batch shm# (:allocator ~client))
               ~result-sym {:result arrow-result#
                            :batch batch#
                            :columns (arrow/batch->columns batch#)
                            :rows (arrow/batch->maps batch#)}]
           (try
             ~@body
             (finally
               (arrow/close-batch! batch#)))))
       (finally
         (release-arrow-result! ~client shm-path#)))))

(defmacro with-client
  [[client-sym url & opts] & body]
  `(let [~client-sym (connect! ~url ~@opts)]
     (try
       ~@body
       (finally
         (close! ~client-sym)))))
