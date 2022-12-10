(ns example.registry
  (:import
   (org.bson.codecs.configuration CodecRegistry CodecRegistries)
   (org.bson.codecs.configuration CodecProvider)
   (org.bson.codecs Codec BsonTypeClassMap BsonTypeCodecMap
                    EncoderContext DecoderContext)
   (org.bson BsonType BsonWriter BsonReader)

   (clojure.lang IPersistentMap Sequential IRecord)
   (java.util Map)
   (java.time Instant)

   (org.bson.codecs
    ValueCodecProvider
    BsonValueCodecProvider
    CollectionCodecProvider
    IterableCodecProvider
    MapCodecProvider
    JsonObjectCodecProvider
    BsonCodecProvider)
   (org.bson.codecs.jsr310
    Jsr310CodecProvider)))

(set! *warn-on-reflection* true)

;; todo: ratio
(defn ^BsonTypeClassMap bson-type-class-map []
  (BsonTypeClassMap.
   (Map/of BsonType/DOCUMENT IPersistentMap
           BsonType/ARRAY Sequential
           BsonType/DATE_TIME Instant)))

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
  (.writeName writer (-> name symbol str)))

(defn- read-name [^BsonReader reader]
  (keyword (.readName reader)))

(defn- end-of-document? [^BsonReader reader]
  (= (.readBsonType reader)
     BsonType/END_OF_DOCUMENT))

(defn ^CodecProvider persistent-map []
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

(defn ^CodecProvider persistent-vector []
  (let [bsonTypeClassMap (bson-type-class-map)]
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
                          acc (conj! acc value)]
                      (recur acc))))))))))))

(defn- map->record [^Class class ^IPersistentMap map]
  (.. class
      (getMethod "create" (into-array [IPersistentMap]))
      (invoke nil (into-array [map]))))

(defn ^CodecProvider record []
  (let [map-provider (persistent-map)]
    (reify CodecProvider
      (get [_ clazz registry]
        (let [map-codec (.get map-provider IPersistentMap registry)]
          (when (.isAssignableFrom IRecord clazz)
            (reify Codec
              (getEncoderClass [_]
                clazz)
              (encode [_ writer obj encoderContext]
                (.encode map-codec writer obj encoderContext))
              (decode [_ reader decoderContext]
                (let [m (.decode map-codec reader decoderContext)]
                  (map->record clazz m))))))))))


(def ^java.util.List providers
  [(ValueCodecProvider.)
   (BsonValueCodecProvider.)
   (record)
   (persistent-map)
   (persistent-vector)
   (Jsr310CodecProvider.)

   (MapCodecProvider.)
   #_(JsonObjectCodecProvider.)
   #_(BsonCodecProvider.)])

(defn ^CodecRegistry registry []
  (CodecRegistries/fromProviders providers))
