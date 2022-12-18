(ns darkleaf.bson-clj.map-test
  (:require
   [clojure.test :as t]
   [darkleaf.bson.core :as bson])
  (:import
   (java.util Map)
   (com.mongodb.client MongoClients)
   (org.bson.types ObjectId)))

(t/deftest ok-test
  (let [uri "mongodb://root:example@localhost:27017"]
    (with-open [client (MongoClients/create uri)]
      (let [db     (.. client
                       (getDatabase "test"))
            coll   (.getCollection db "map-test" Map)
            id     (ObjectId.)
            _      (.insertOne coll (Map/of "_id" id "foo" "bar"))
            filter (bson/->bson (Map/of "_id" id))
            doc    (.findOneAndDelete coll filter)]
        (t/is (= "bar" (get doc "foo")))))))
