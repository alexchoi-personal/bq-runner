(ns bq-runner.client-test
  (:require [clojure.test :refer :all]
            [bq-runner.client :as client]
            [bq-runner.protocol :as proto]))

(deftest test-protocol-rpc-request
  (let [req (proto/rpc-request "bq.ping" {} 1)]
    (is (= "2.0" (:jsonrpc req)))
    (is (= "bq.ping" (:method req)))
    (is (= {} (:params req)))
    (is (= 1 (:id req)))))

(deftest test-protocol-parse-response-success
  (let [json-str "{\"jsonrpc\":\"2.0\",\"result\":{\"message\":\"pong\"},\"id\":1}"
        parsed (proto/parse-response json-str)]
    (is (= {:message "pong"} (:result parsed)))
    (is (= 1 (:id parsed)))
    (is (nil? (:error parsed)))))

(deftest test-protocol-parse-response-error
  (let [json-str "{\"jsonrpc\":\"2.0\",\"error\":{\"code\":-32600,\"message\":\"Invalid Request\"},\"id\":1}"
        parsed (proto/parse-response json-str)]
    (is (some? (:error parsed)))
    (is (= -32600 (get-in parsed [:error :code])))
    (is (= 1 (:id parsed)))))

(deftest test-protocol-extract-result-success
  (let [response {:result {:value 42} :id 1}]
    (is (= {:value 42} (proto/extract-result response)))))

(deftest test-protocol-extract-result-error
  (let [response {:error {:code -32600 :message "Error"} :id 1}]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"RPC Error"
                          (proto/extract-result response)))))

(deftest test-protocol-encode-request
  (let [req {:jsonrpc "2.0" :method "bq.ping" :params {} :id 1}
        encoded (proto/encode-request req)]
    (is (string? encoded))
    (is (.contains encoded "bq.ping"))))
