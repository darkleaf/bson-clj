(ns darkleaf.bson-clj.persistent-map-test
  (:require
   [clojure.test :as t]
   [darkleaf.bson.core :as bson])
  (:import
   (clojure.lang IPersistentMap)
   (com.mongodb.client MongoClients)
   (org.bson.types ObjectId)))

(set! *warn-on-reflection* true)

(t/deftest ok-test
  (let [uri "mongodb://root:example@localhost:27017"]
    (with-open [client (MongoClients/create uri)]
      (let [db     (.. client
                       (getDatabase "test")
                       (withCodecRegistry (bson/codec-registry)))
            coll   (.getCollection db "map-test" IPersistentMap)
            id     (ObjectId.)
            _      (.insertOne coll {:_id id :foo "bar"})
            filter (bson/->bson {:_id id})
            doc    (.findOneAndDelete coll filter)]
        (t/is (= "bar" (doc :foo)))))))
