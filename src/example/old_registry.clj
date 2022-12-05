(ns example.old-registry
  (:require
   [example.persistent-map-provider :as pm-provider])
  (:import
   (org.bson.codecs.configuration CodecRegistries)
   (org.bson.codecs
    ValueCodecProvider
    BsonValueCodecProvider
    DocumentCodecProvider
    CollectionCodecProvider
    IterableCodecProvider
    MapCodecProvider
    JsonObjectCodecProvider
    BsonCodecProvider
    EnumCodecProvider

    BsonTypeClassMap)
   (org.bson.codecs.jsr310
    Jsr310CodecProvider)
   (com.mongodb
    DBRefCodecProvider
    DBObjectCodecProvider
    DocumentToDBRefTransformer
    Jep395RecordCodecProvider)
   (com.mongodb.client.model.geojson.codecs
    GeoJsonCodecProvider)
   (com.mongodb.client.gridfs.codecs GridFSFileCodecProvider)))

(set! *warn-on-reflection* true)

(def ^java.util.List providers
  [(ValueCodecProvider.)
   (BsonValueCodecProvider.)
   (DBRefCodecProvider.)
   (DBObjectCodecProvider.)
   (DocumentCodecProvider. (DocumentToDBRefTransformer.))

   (pm-provider/->PersistentMapCodecProvider (BsonTypeClassMap.))

   (CollectionCodecProvider. (DocumentToDBRefTransformer.))
   (IterableCodecProvider. (DocumentToDBRefTransformer.))
   (MapCodecProvider. (DocumentToDBRefTransformer.))
   (GeoJsonCodecProvider.)
   (GridFSFileCodecProvider.)
   (Jsr310CodecProvider.)
   (JsonObjectCodecProvider.)
   (BsonCodecProvider.)
   (EnumCodecProvider.)
   (Jep395RecordCodecProvider.)])

(def clj-registry
  (CodecRegistries/fromProviders providers))
