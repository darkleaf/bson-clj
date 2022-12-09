(ns example.registry
  (:import
   (org.bson.codecs.configuration CodecRegistry CodecRegistries)
   (org.bson.codecs.configuration CodecProvider)
   (org.bson.codecs Codec BsonTypeClassMap BsonTypeCodecMap)
   (org.bson BsonType)

   (clojure.lang IPersistentMap Sequential)
   (java.util Map)

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

#_(ns example.persistent-map-provider
    (:import
     (org.bson Transformer BsonType UuidRepresentation)
     (org.bson.codecs Codec BsonTypeClassMap BsonTypeCodecMap ContainerCodecHelper)

     (clojure.lang IPersistentMap)))

;; todo: ratio
(defn ^BsonTypeClassMap bson-type-class-map []
  (BsonTypeClassMap.
   (Map/of BsonType/DOCUMENT IPersistentMap
           BsonType/ARRAY Sequential)))

(defn persistent-map []
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
                  (.writeName writer (name k))
                  (if (nil? v)
                    (.writeNull writer)
                    (let [codec (.get registry (class v))]
                      (.encodeWithChildContext encoderContext codec writer v))));
                (.writeEndDocument writer))
              (decode [_ reader decoderContext]
                (.readStartDocument reader)
                (let [m (transient {})]
                  (while (not= (.readBsonType reader)
                               BsonType/END_OF_DOCUMENT)
                    (let [fieldName (keyword (.readName reader))
                          bsonType  (.getCurrentBsonType reader)]
                      (if (= bsonType BsonType/NULL)
                        (do
                          (.readNull reader)
                          (assoc! m fieldName nil))
                        (let [codec (.get bsonTypeCodecMap bsonType)
                              value (.decode codec reader decoderContext)]
                          (assoc! m fieldName value)))))
                  (.readEndDocument reader)
                  (persistent! m))))))))))


(defn persistent-vector []
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
                  (if (nil? v)
                    (.writeNull writer)
                    (let [codec (.get registry (class v))]
                      (.encodeWithChildContext encoderContext codec writer v))));
                (.writeEndArray writer))
              (decode [_ reader decoderContext]
                (.readStartArray reader)
                (let [v (transient [])]
                  (while (not= (.readBsonType reader)
                               BsonType/END_OF_DOCUMENT)
                    (let [bsonType (.getCurrentBsonType reader)]
                      (if (= bsonType BsonType/NULL)
                        (do
                          (.readNull reader)
                          (conj! v nil))
                        (let [codec (.get bsonTypeCodecMap bsonType)
                              value (.decode codec reader decoderContext)]
                          (conj! v value)))))
                  (.readEndArray reader)
                  (persistent! v))))))))))


(def ^java.util.List providers
  [(ValueCodecProvider.)
   #_(BsonValueCodecProvider.)
   (persistent-map)
   (persistent-vector)


   #_(MapCodecProvider.)

   #_(pm-provider/->PersistentMapCodecProvider (BsonTypeClassMap.))
   #_(CollectionCodecProvider.)
   #_(IterableCodecProvider.)
   #_(Jsr310CodecProvider.)
   #_(JsonObjectCodecProvider.)
   #_(BsonCodecProvider.)])

(defn ^CodecRegistry registry []
  (CodecRegistries/fromProviders providers))
