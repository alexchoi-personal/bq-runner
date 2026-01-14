(ns bq-runner.arrow-test
  (:require [clojure.test :refer :all]
            [bq-runner.arrow :as arrow])
  (:import [org.apache.arrow.memory RootAllocator]
           [org.apache.arrow.vector VectorSchemaRoot]
           [org.apache.arrow.vector.types.pojo Schema Field FieldType ArrowType$Int ArrowType$Utf8]
           [org.apache.arrow.vector.ipc ArrowStreamWriter]
           [java.io ByteArrayOutputStream RandomAccessFile]
           [java.nio ByteBuffer]))

(defn- create-test-shm-file
  [path data]
  (let [length (count data)
        total-size (+ 8 length)]
    (with-open [raf (RandomAccessFile. path "rw")]
      (let [channel (.getChannel raf)
            buffer (.map channel java.nio.channels.FileChannel$MapMode/READ_WRITE 0 total-size)]
        (.putLong buffer length)
        (.put buffer (byte-array data))
        (.force buffer)))))

(deftest test-with-allocator
  (arrow/with-allocator [allocator]
    (is (some? allocator))
    (is (instance? RootAllocator allocator))))

(deftest test-open-close-shared-memory
  (let [test-path "/tmp/test_arrow_shm"
        test-data (byte-array [1 2 3 4 5])]
    (try
      (create-test-shm-file test-path test-data)
      (let [shm (arrow/open-shared-memory test-path)]
        (is (some? (:buffer shm)))
        (is (some? (:channel shm)))
        (is (some? (:file shm)))
        (arrow/close-shared-memory! shm))
      (finally
        (java.io.File/.delete (java.io.File. test-path))))))

(deftest test-with-shared-memory-macro
  (let [test-path "/tmp/test_arrow_shm_macro"
        test-data (byte-array [10 20 30])]
    (try
      (create-test-shm-file test-path test-data)
      (arrow/with-shared-memory [shm test-path]
        (is (some? (:buffer shm))))
      (finally
        (java.io.File/.delete (java.io.File. test-path))))))
