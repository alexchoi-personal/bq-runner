(ns bq-runner.websocket
  (:require [aleph.http :as http]
            [manifold.stream :as s]
            [manifold.deferred :as d]
            [bq-runner.protocol :as proto]))

(defn connect!
  [url & {:keys [timeout] :or {timeout 30000}}]
  (let [pending-requests (atom {})
        conn @(http/websocket-client url)]
    (s/consume
     (fn [msg]
       (let [response (proto/parse-response msg)
             id (:id response)]
         (when-let [deferred (get @pending-requests id)]
           (swap! pending-requests dissoc id)
           (d/success! deferred response))))
     conn)
    {:conn conn
     :pending pending-requests
     :timeout timeout}))

(defn send-rpc!
  [{:keys [conn pending timeout]} method params]
  (let [request (proto/rpc-request method params)
        id (:id request)
        result-deferred (d/deferred)]
    (swap! pending assoc id result-deferred)
    @(s/put! conn (proto/encode-request request))
    (-> result-deferred
        (d/timeout! timeout ::timeout)
        (d/chain
         (fn [response]
           (if (= response ::timeout)
             (do
               (swap! pending dissoc id)
               (throw (ex-info "Request timeout" {:method method :id id})))
             (proto/extract-result response)))))))

(defn close! [{:keys [conn pending]}]
  (doseq [[id d] @pending]
    (d/error! d (ex-info "Connection closed" {:id id})))
  (reset! pending {})
  (s/close! conn))
