(ns darkleaf.bson.core
  (:import
   (org.bson.conversions Bson)
   (org.bson BsonDocumentWrapper)))

(set! *warn-on-reflection* true)

(defn ^Bson ->bson [x]
  (reify Bson
    (toBsonDocument [_ _ codec-registry]
      (BsonDocumentWrapper/asBsonDocument x codec-registry))))
