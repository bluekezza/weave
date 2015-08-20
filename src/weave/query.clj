(ns weave.query
  (:require [schema.core :as s])
  (:import [org.bson.types ObjectId]))

(s/defn nest :- s/Keyword
  ([levels :- [s/Str]
    key :- s/Keyword] (nest (conj levels key)))
  ([levels :- [(s/either s/Str s/Keyword)]]
     (let [string-keys (map name levels)
           combined-str (apply str (interpose "." string-keys))]
       (keyword combined-str))))
