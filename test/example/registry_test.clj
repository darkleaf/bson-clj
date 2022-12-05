(ns example.registry-test
  (:require
   [example.registry :as sut]
   [clojure.test :as t])
  (:import
   (org.bson BsonBinaryWriter BsonBinaryReader)
   (org.bson.io BasicOutputBuffer)
   (org.bson.codecs EncoderContext DecoderContext)
   (org.bson.codecs.configuration CodecRegistry)
   (java.nio ByteBuffer)))

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

(t/deftest roundtrip
  (let [registry (sut/registry)]
    (t/are [v] (t/is (= v (-> v
                              (bson-write registry)
                              (bson-read (class v) registry))))
      {}
      {:foo true}
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


      #_{:foo 1/2})))
