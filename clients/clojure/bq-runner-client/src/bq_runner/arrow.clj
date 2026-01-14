(ns bq-runner.arrow
  (:import [java.io RandomAccessFile ByteArrayInputStream]
           [java.nio MappedByteBuffer ByteBuffer]
           [java.nio.channels FileChannel FileChannel$MapMode]
           [org.apache.arrow.memory RootAllocator BufferAllocator]
           [org.apache.arrow.vector.ipc ArrowStreamReader]
           [org.apache.arrow.vector VectorSchemaRoot FieldVector]
           [org.apache.arrow.vector.types.pojo Schema Field ArrowType$Int ArrowType$FloatingPoint ArrowType$Utf8 ArrowType$Bool ArrowType$Date ArrowType$Timestamp ArrowType$Binary]))

(defn open-shared-memory
  [path]
  (let [file (RandomAccessFile. path "r")
        channel (.getChannel file)
        size (.size channel)
        buffer (.map channel FileChannel$MapMode/READ_ONLY 0 size)]
    {:file file
     :channel channel
     :buffer buffer
     :size size}))

(defn close-shared-memory!
  [{:keys [channel file]}]
  (.close channel)
  (.close file))

(defn- read-length-prefix [^MappedByteBuffer buffer]
  (.position buffer 0)
  (.order buffer java.nio.ByteOrder/LITTLE_ENDIAN)
  (.getLong buffer))

(defn- get-arrow-data [^MappedByteBuffer buffer]
  (let [length (read-length-prefix buffer)
        data (byte-array length)]
    (.position buffer 8)
    (.get buffer data)
    data))

(defn read-arrow-batch
  [shm-handle ^BufferAllocator allocator]
  (let [data (get-arrow-data (:buffer shm-handle))
        input-stream (ByteArrayInputStream. data)
        reader (ArrowStreamReader. input-stream allocator)]
    (when (.loadNextBatch reader)
      {:root (.getVectorSchemaRoot reader)
       :reader reader})))

(defn- convert-arrow-value [v]
  (cond
    (nil? v) nil
    (instance? org.apache.arrow.vector.util.Text v) (.toString v)
    (instance? java.util.List v) (mapv convert-arrow-value v)
    (instance? java.util.Map v) (into {} (map (fn [[k v]] [(keyword k) (convert-arrow-value v)]) v))
    :else v))

(defn- field-vector->clj
  [^FieldVector vector]
  (let [n (.getValueCount vector)]
    (mapv (fn [i]
            (when-not (.isNull vector i)
              (convert-arrow-value (.getObject vector i))))
          (range n))))

(defn batch->columns
  [{:keys [root]}]
  (let [^VectorSchemaRoot vsr root
        schema (.getSchema vsr)
        fields (.getFields schema)]
    (into {}
          (map (fn [^Field field]
                 (let [name (.getName field)
                       vector (.getVector vsr name)]
                   [(keyword name) (field-vector->clj vector)])))
          fields)))

(defn batch->maps
  [{:keys [root]}]
  (let [^VectorSchemaRoot vsr root
        schema (.getSchema vsr)
        fields (.getFields schema)
        field-names (mapv #(.getName ^Field %) fields)
        row-count (.getRowCount vsr)]
    (mapv (fn [row-idx]
            (into {}
                  (map (fn [^String field-name]
                         (let [vector (.getVector vsr field-name)
                               value (when-not (.isNull vector row-idx)
                                       (convert-arrow-value (.getObject vector row-idx)))]
                           [(keyword field-name) value])))
                  field-names))
          (range row-count))))

(defn close-batch!
  [{:keys [reader root]}]
  (when root
    (.close ^VectorSchemaRoot root))
  (when reader
    (.close ^ArrowStreamReader reader)))

(defn with-allocator*
  [f]
  (let [allocator (RootAllocator. Long/MAX_VALUE)]
    (try
      (f allocator)
      (finally
        (.close allocator)))))

(defmacro with-allocator
  [[allocator-sym] & body]
  `(with-allocator* (fn [~allocator-sym] ~@body)))

(defn with-shared-memory*
  [path f]
  (let [shm (open-shared-memory path)]
    (try
      (f shm)
      (finally
        (close-shared-memory! shm)))))

(defmacro with-shared-memory
  [[shm-sym path] & body]
  `(with-shared-memory* ~path (fn [~shm-sym] ~@body)))
