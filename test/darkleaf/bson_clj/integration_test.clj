(ns darkleaf.bson-clj.integration-test
  (:require
   [clojure.test :as t]
   [darkleaf.bson-clj.core :as bson])
  (:import
   (clojure.lang IPersistentMap)
   (com.mongodb.client MongoClients)
   (com.mongodb.client.gridfs GridFSBuckets)
   (com.mongodb.client.gridfs.model GridFSUploadOptions)
   (java.io ByteArrayInputStream)
   (java.util Map)
   (org.bson.types ObjectId)))

(set! *warn-on-reflection* true)

(def mongo-host (or (System/getenv "MONGO_HOST") "localhost"))
(def ^String mongo-uri (str "mongodb://root:example@" mongo-host ":27017"))

(t/deftest java-map-test
  (with-open [client (MongoClients/create mongo-uri)]
    (let [db     (.. client
                     (getDatabase "test"))
          coll   (.getCollection db "map-test" Map)
          id     (ObjectId.)
          _      (.insertOne coll (Map/of "_id" id "foo" "bar"))
          filter (bson/->bson (Map/of "_id" id))
          doc    (.findOneAndDelete coll filter)]
      (t/is (= "bar" (get doc "foo"))))))

(t/deftest clj-map-test
  (with-open [client (MongoClients/create mongo-uri)]
    (let [db     (.. client
                     (getDatabase "test")
                     (withCodecRegistry (bson/codec-registry)))
          coll   (.getCollection db "map-test" IPersistentMap)
          id     (ObjectId.)
          _      (.insertOne coll {:_id id :foo "bar"})
          filter (bson/->bson {:_id id})
          doc    (.findOneAndDelete coll filter)]
      (t/is (= "bar" (doc :foo))))))

#_
(t/deftest grid-fs-test
  (with-open [client (MongoClients/create mongo-uri)]
    (let [db     (.. client
                     (getDatabase "test")
                     (withCodecRegistry (bson/codec-registry)))
          bucket (GridFSBuckets/create db)
          opts   (.. (GridFSUploadOptions.)
                     (metadata))
          content (-> "content" (.getBytes) (ByteArrayInputStream.))
          id      (.uploadFromStream bucket "test.file" content)])))
