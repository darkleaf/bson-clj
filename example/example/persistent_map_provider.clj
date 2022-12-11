(ns example.persistent-map-provider
  (:import
   (org.bson Transformer BsonType UuidRepresentation)
   (org.bson.codecs Codec BsonTypeClassMap BsonTypeCodecMap ContainerCodecHelper)
   (org.bson.codecs.configuration CodecProvider)
   (clojure.lang IPersistentMap)))

(set! *warn-on-reflection* true)



(defrecord PersistentMapCodecProvider [^BsonTypeClassMap bsonTypeClassMap
                                       #_^Transformer valueTransformer]
  CodecProvider
  (get [_ clazz registry]
    (let [bsonTypeCodecMap (BsonTypeCodecMap. bsonTypeClassMap registry)]
      (when (.isAssignableFrom IPersistentMap clazz)
        (reify Codec
          (getEncoderClass [_]
            clazz)
          (encode [_ writer obj encoderContext])
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
                          ;; todo? UUID wtf
                          value (.decode codec reader decoderContext)
                          #_#_value (.transform valueTransformer value)]
                      (assoc! m fieldName value)))))
              (.readEndDocument reader)
              (persistent! m))))))))


(comment
  ;; оно соответственно в запросах будет работать
  (clj-provider :keyword-fields #{:type :kind :state :status}))



#_(-> (Test/getBasis)
      first
      meta
      :tag
      class)


#_(let [c Test]
    (.. c
        (getDeclaredConstructor (make-array Class 0))
        (newInstance (make-array Object 0))))
