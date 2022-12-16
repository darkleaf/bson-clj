(ns darkleaf.bson.core
  (:import
   (java.time Instant)
   (java.util Map List)
   (clojure.lang IPersistentMap) ;collection?
   (org.bson
    BsonDocumentWrapper
    BsonType
    BsonWriter BsonReader)
   (org.bson.codecs
    Codec
    BsonTypeClassMap BsonTypeCodecMap
    EncoderContext DecoderContext)
   (org.bson.codecs.configuration
    CodecRegistry CodecRegistries)
   (org.bson.conversions Bson)))

(set! *warn-on-reflection* true)

(defn- ^BsonTypeClassMap bson-type-class-map []
  (BsonTypeClassMap.
   {BsonType/DOCUMENT  IPersistentMap
    #_#_BsonType/ARRAY     Iterable
    #_#_BsonType/DATE_TIME Instant}))

(defn- write-value [^BsonWriter writer
                    ^CodecRegistry registry
                    ^EncoderContext encoderContext
                    v]
  (if (nil? v)
    (.writeNull writer)
    (let [codec (.get registry (class v))]
      (.encodeWithChildContext encoderContext codec writer v))))

(defn- read-value [^BsonReader reader
                   ^BsonTypeCodecMap bsonTypeCodecMap
                   ^DecoderContext decoderContext]
  (let [bsonType  (.getCurrentBsonType reader)]
    (if (= bsonType BsonType/NULL)
      (do
        (.readNull reader)
        nil)
      (let [codec (.get bsonTypeCodecMap bsonType)
            value (.decode codec reader decoderContext)]
        value))))

(defn- write-name [^BsonWriter writer name]
  (.writeName writer (-> name symbol str))) ;; todo

(defn- read-name [^BsonReader reader]
  (keyword (.readName reader)))

(defn- end-of-document? [^BsonReader reader]
  (= (.readBsonType reader)
     BsonType/END_OF_DOCUMENT))

(defn- ^CodecProvider persistent-map []
  (let [bsonTypeClassMap (bson-type-class-map)]
    (reify CodecProvider
      (get [_ clazz registry]
        (let [bsonTypeCodecMap (BsonTypeCodecMap. bsonTypeClassMap registry)]
          (when (.isAssignableFrom IPersistentMap clazz)
            (reify Codec
              (getEncoderClass [_]
                clazz)
              (encode [_ writer obj encoderContext]
                (.writeStartDocument writer)
                (doseq [[k v] obj]
                  (write-name writer k)
                  (write-value writer registry encoderContext v))
                (.writeEndDocument writer))
              (decode [_ reader decoderContext]
                (.readStartDocument reader)
                (loop [acc (transient {})]
                  (if (end-of-document? reader)
                    (do
                      (.readEndDocument reader)
                      (persistent! acc))
                    (let [field-name (read-name reader)
                          value      (read-value reader
                                                 bsonTypeCodecMap
                                                 decoderContext)
                          acc        (assoc! acc field-name value)]
                      (recur acc))))))))))))

#_(defn- ^CodecProvider persistent-vector []
    (let [bsonTypeClassMap (bson-type-class-map)]
      (reify CodecProvider
        (get [_ clazz registry]
          (let [bsonTypeCodecMap (BsonTypeCodecMap. bsonTypeClassMap registry)]
            (when (.isAssignableFrom Iterable clazz)
              (reify Codec
                (getEncoderClass [_]
                  clazz)
                (encode [_ writer obj encoderContext]
                  (.writeStartArray writer)
                  (doseq [v obj]
                    (write-value writer registry encoderContext v))
                  (.writeEndArray writer))
                (decode [_ reader decoderContext]
                  (.readStartArray reader)
                  (loop [acc (transient [])]
                    (if (end-of-document? reader)
                      (do
                        (.readEndArray reader)
                        (persistent! acc))
                      (let [value (read-value reader
                                              bsonTypeCodecMap
                                              decoderContext)
                            acc   (conj! acc value)]
                        (recur acc))))))))))))

#_(defn- map->record [^Class class ^IPersistentMap map]
    (.. class
        (getMethod "create" (into-array [IPersistentMap]))
        (invoke nil (into-array [map]))))

#_(defn- ^CodecProvider record []
    (reify CodecProvider
      (get [_ clazz registry]
        (let [map-codec (.get registry IPersistentMap)]
          (when (.isAssignableFrom IRecord clazz)
            (reify Codec
              (getEncoderClass [_]
                clazz)
              (encode [_ writer obj encoderContext]
                (.encode map-codec writer obj encoderContext))
              (decode [_ reader decoderContext]
                (let [m (.decode map-codec reader decoderContext)]
                  (map->record clazz m)))))))))

(defn ^Bson ->bson [x]
  (reify Bson
    (toBsonDocument [_ _ codec-registry]
      (BsonDocumentWrapper/asBsonDocument x codec-registry))))

#_(MongoClientSettings/getDefaultCodecRegistry)
(defn codec-registry []
  (let [providers [(persistent-map)
                   Bson/DEFAULT_CODEC_REGISTRY]]
    (CodecRegistries/fromProviders ^List providers)))
