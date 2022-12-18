(ns darkleaf.bson-clj.core
  (:import
   (clojure.lang IPersistentMap IPersistentVector IRecord)
   (java.time Instant)
   (java.util Map List)
   (org.bson BsonDocumentWrapper BsonType UuidRepresentation)
   (org.bson.codecs Codec BsonTypeClassMap
                    BsonCodecProvider
                    BsonValueCodecProvider
                    DocumentCodecProvider
                    IterableCodecProvider
                    JsonObjectCodecProvider
                    MapCodecProvider
                    ValueCodecProvider)
   (org.bson.codecs.configuration CodecProvider CodecRegistry CodecRegistries)
   (org.bson.codecs.jsr310 Jsr310CodecProvider)
   (org.bson.conversions Bson)))

(set! *warn-on-reflection* true)

(defn- map-key->str [x]
  (-> x symbol str))

(defn- str->map-key [x]
  (keyword x))

(defn- ^CodecProvider persistent-map []
  (reify CodecProvider
    (get [_ clazz registry]
      (let [codec (.get registry Map)]
        (when (.isAssignableFrom IPersistentMap clazz)
          (reify Codec
            (getEncoderClass [_]
              clazz)
            (encode [_ w obj ctx]
              (.encode codec w (update-keys obj map-key->str) ctx))
            (decode [_ r ctx]
              (-> (into {} (.decode codec r ctx))
                  (update-keys str->map-key)))))))))

(defn- ^CodecProvider persistent-vector []
  (reify CodecProvider
    (get [_ clazz registry]
      (let [codec (.get registry Iterable)]
        (when (.isAssignableFrom IPersistentVector clazz)
          (reify Codec
            (getEncoderClass [_]
              clazz)
            (encode [_ w obj ctx]
              (.encode codec w obj ctx))
            (decode [_ r ctx]
              (vec (.decode codec r ctx)))))))))

;; todo: interactive development and record class reloading
(defn- map->record [^Class clazz ^IPersistentMap map]
  (.. clazz
      (getMethod "create" (into-array [IPersistentMap]))
      (invoke nil (into-array [map]))))

(defn- ^CodecProvider record []
  (reify CodecProvider
    (get [_ clazz registry]
      (let [codec (.get registry IPersistentMap)]
        (when (.isAssignableFrom IRecord clazz)
          (reify Codec
            (getEncoderClass [_]
              clazz)
            (encode [_ w obj ctx]
              (.encode codec w obj ctx))
            (decode [_ r ctx]
              (let [m (.decode codec r ctx)]
                (map->record clazz m)))))))))

(defn- ^BsonTypeClassMap bson-type-class-map []
  (BsonTypeClassMap.
   {BsonType/DOCUMENT  IPersistentMap
    BsonType/ARRAY     IPersistentVector
    BsonType/DATE_TIME Instant}))

(defn ^Bson ->bson [x]
  (reify Bson
    (toBsonDocument [_ _ codec-registry]
      (BsonDocumentWrapper/asBsonDocument x codec-registry))))

#_Bson/DEFAULT_CODEC_REGISTRY
#_(MongoClientSettings/getDefaultCodecRegistry)
(defn ^CodecRegistry codec-registry []
  (let [class-map (bson-type-class-map)
        providers [(record)
                   (persistent-map)
                   (persistent-vector)

                   (ValueCodecProvider.)
                   (BsonValueCodecProvider.)
                   (DocumentCodecProvider. class-map)
                   (IterableCodecProvider. class-map)
                   (MapCodecProvider. class-map)
                   (Jsr310CodecProvider.)
                   (JsonObjectCodecProvider.)
                   (BsonCodecProvider.)]]
    (-> ^List providers
        (CodecRegistries/fromProviders)
        (CodecRegistries/withUuidRepresentation UuidRepresentation/STANDARD))))
