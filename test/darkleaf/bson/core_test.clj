(ns darkleaf.bson.core-test
  (:require
   [darkleaf.bson.core :as bson]
   [clojure.test :as t])
  (:import
   (org.bson BsonBinaryWriter BsonBinaryReader)
   (org.bson.io BasicOutputBuffer)
   (org.bson.codecs EncoderContext DecoderContext)
   (org.bson.codecs.configuration CodecRegistry)
   (java.nio ByteBuffer)

   (org.bson Document BsonDocument BsonBinary)
   (org.bson.json JsonObject)
   (org.bson.types ObjectId Binary)
   (org.bson.conversions Bson)
   (java.time Instant)
   (java.util Map List)))

(set! *warn-on-reflection* true)

(defn bson-write [x ^CodecRegistry registry]
  (let [out   (BasicOutputBuffer.)
        w     (BsonBinaryWriter. out)
        ctx   (.. (EncoderContext/builder) build)
        codec (.get registry (class x))]
    (.encode codec w x ctx)
    (.toByteArray out)))

(defn bson-read [^bytes bytes ^Class class ^CodecRegistry registry]
  (let [r     (-> bytes ByteBuffer/wrap BsonBinaryReader.)
        ctx   (.. (DecoderContext/builder) build)
        codec (.get registry class)]
    (.decode codec r ctx)))

#_(defrecord Test [a b])

(def ^{:tag 'bytes} bar-bytes (.getBytes "bar"))
(def inst (Instant/parse "2022-12-10T16:31:00Z"))

(t/deftest roundtrip
  (let [registry (bson/codec-registry)]
    (t/are [v] (let [v# v]
                 (t/is (= v# (-> v#
                                 (bson-write registry)
                                 (bson-read (class v#) registry)))))
      {}
      {:foo nil}
      {:foo true}
      {:foo/bar true}
      {:foo "bar"}
      {:foo 1}
      {:foo 0.1}
      {:foo {:bar "buzz"}}
      {:foo []}
      {:foo [true]}
      {:foo [1]}
      {:foo [{:a 2}]}
      {:foo (list)}
      {:foo (list 1 2 3)}
      {:foo inst}
      {:foo [inst inst]}
      {:foo (Binary. bar-bytes)}
      {:foo (ObjectId/get)}
      #_(->Test 1 2))))

(t/deftest roundtip-changing-type
  (let [registry (bson/codec-registry)]
    (t/are [from to] (t/is (= to (-> from
                                     (bson-write registry)
                                     (bson-read (class to) registry))))
      #_#_
      {:foo 1/2}
      {:foo 0.5}

      {:foo #{1}}
      {:foo [1]}

      {:foo #{{:a 1}}}
      {:foo [{:a 1}]}

      (Map/of "bar" "buzz")
      {:bar "buzz"}

      {:foo (Map/of "bar" "buzz")}
      {:foo {:bar "buzz"}}

      {:foo (List/of 1 2 3)}
      {:foo [1 2 3]}

      (JsonObject. "{\"foo\": \"bar\"}")
      {:foo "bar"}

      (Document. "foo" "bar")
      {:foo "bar"}

      ;; BsonValueCodec
      (BsonDocument. "foo" (BsonBinary. bar-bytes))
      {:foo (Binary. bar-bytes)}

      ;; BsonCodec
      (reify Bson
        (toBsonDocument [_ _ _]
          (BsonDocument. "foo" (BsonBinary. bar-bytes))))
      {:foo (Binary. bar-bytes)})))
