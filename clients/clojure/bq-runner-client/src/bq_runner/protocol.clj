(ns bq-runner.protocol
  (:require [cheshire.core :as json]))

(def ^:private id-counter (atom 0))

(defn next-id []
  (swap! id-counter inc))

(defn rpc-request
  ([method params]
   (rpc-request method params (next-id)))
  ([method params id]
   {:jsonrpc "2.0"
    :method method
    :params params
    :id id}))

(defn parse-response [json-str]
  (let [resp (json/parse-string json-str true)]
    (if (:error resp)
      {:error (:error resp)
       :id (:id resp)}
      {:result (:result resp)
       :id (:id resp)})))

(defn extract-result [response]
  (if-let [error (:error response)]
    (throw (ex-info (str "RPC Error: " (:message error))
                    {:error error
                     :code (:code error)
                     :data (:data error)}))
    (:result response)))

(defn encode-request [request]
  (json/generate-string request))
