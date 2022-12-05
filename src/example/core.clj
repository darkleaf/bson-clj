(ns example.core
  (:require
   [example.registry :refer [clj-registry]])
  (:import
   (org.bson Document)
   (org.bson.codecs Codec)

   (org.bson.codecs.configuration CodecRegistries)

   (com.mongodb.client.model Filters)
   (com.mongodb.client MongoClient MongoClients MongoCollection MongoDatabase)))

(set! *warn-on-reflection* true)

(let [uri "mongodb://root:example@localhost:27017"]
  (with-open [client (MongoClients/create uri)]
    (let [db   (.. client
                   (getDatabase "sample")
                   (withCodecRegistry clj-registry))
          coll (.getCollection db "movies" #_clojure.lang.IPersistentMap)]
      #_(.insertOne coll (Document. {"a" 1, "b" 2}))
      (->> (.. coll
               (find (Filters/eq "a" 1))
               first)
           (into {})
           class))))




;;ocument doc = collection.find(eq("title", "Back to the Future")).first();
