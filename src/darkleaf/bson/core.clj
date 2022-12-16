(ns darkleaf.bson.core
  (:import
   (java.time Instant)
   (java.util Map List)
   (clojure.lang IPersistentMap Sequential)
   (org.bson
    BsonDocumentWrapper
    BsonType
    BsonWriter BsonReader)
   (org.bson.codecs
    Codec
    BsonTypeClassMap BsonTypeCodecMap
    EncoderContext DecoderContext)
   (org.bson.codecs.configuration
    CodecProvider CodecRegistry CodecRegistries)
   (org.bson.conversions Bson)))

(set! *warn-on-reflection* true)

(defn- ^BsonTypeClassMap bson-type-class-map []
  (BsonTypeClassMap.
   {BsonType/DOCUMENT  IPersistentMap
    BsonType/ARRAY     Sequential
    BsonType/DATE_TIME Instant}))

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
  (let [bsonType (.getCurrentBsonType reader)]
    (if (= bsonType BsonType/NULL)
      (.readNull reader)
      (let [codec (.get bsonTypeCodecMap bsonType)]
        (.decode codec reader decoderContext)))))

(defn- write-name [^BsonWriter writer name]
  (.writeName writer (-> name symbol str)))

(defn- read-name [^BsonReader reader]
  (keyword (.readName reader)))

(defn- end-of-document? [^BsonReader reader]
  (= (.readBsonType reader)
     BsonType/END_OF_DOCUMENT))

(defn- ^CodecProvider persistent-map [bsonTypeClassMap]
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
                    (recur acc)))))))))))

(defn- ^CodecProvider persistent-vector [bsonTypeClassMap]
  (reify CodecProvider
    (get [_ clazz registry]
      (let [bsonTypeCodecMap (BsonTypeCodecMap. bsonTypeClassMap registry)]
        (when (.isAssignableFrom Sequential clazz)
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
                    (recur acc)))))))))))

;; todo: interactive development and record class reloading
#_#_
(defn- map->record [^Class class ^IPersistentMap map]
  (.. class
      (getMethod "create" (into-array [IPersistentMap]))
      (invoke nil (into-array [map]))))

(defn- ^CodecProvider record []
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

#_(-clj-codec-registry {BsonType/DATE_TIME Instant})
(defn ^CodecRegistry -clj-codec-registry []
  (let [class-map (bson-type-class-map)]
    (CodecRegistries/fromProviders
     (List/of (persistent-map class-map)
              (persistent-vector class-map)))))

;; а может оно и не нужно и Bson/DEFAULT_CODEC_REGISTRY достаточно
#_(MongoClientSettings/getDefaultCodecRegistry)
(defn ^CodecRegistry codec-registry []
  (CodecRegistries/fromRegistries
   (List/of (-clj-codec-registry)
            Bson/DEFAULT_CODEC_REGISTRY)))
