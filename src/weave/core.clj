(ns weave.core
  (:require [schema.core :as s]
            [clojure.tools.logging :as log]))

;; SCHEMAS
(defn Fn
  "a prototype way to describe the schema for a functions input and output types"
  [arg-types return-type]
  Object)

(def Str (s/both s/Str (s/pred #(not (clojure.string/blank? %)))))

(def UUID Str)

(def DB Object)

(def Query clojure.lang.PersistentHashMap)

(def DocumentId Str)

(def Document clojure.lang.PersistentHashMap)

(def CollectionName s/Keyword)

(def DocumentType s/Keyword)

(def PropertyPath [(s/either s/Keyword s/Int)])

(def DocumentJoin {:foreign-path PropertyPath
                   :foreign-type DocumentType
                   :value Str
                   :insertion-path PropertyPath
                   :insertion-type (s/enum :list :value)
                   })

(def ForeignKeyTypes [:static :dynamic :multi])

(def ForeignKey {:name Str
                 :type (apply s/enum ForeignKeyTypes)
                 :from DocumentType
                 (s/optional-key :from-path) PropertyPath
                 (s/optional-key :to) DocumentType
                 (s/optional-key :to-path) PropertyPath
                 (s/optional-key :insertion-path) PropertyPath
                 (s/optional-key :func) (s/maybe Object)
                 })

(def JValue (s/either s/Int s/Str))

(def CommandType (s/enum :fetch :scan :join))

(def ScanCommand
  {:type (s/enum :scan)
   :doc-id Str
   })

(def JoinCommandId Str)

(def JoinCommand
  {:id JoinCommandId   
   :type (s/enum :join)
   :parent-id DocumentId
   :join DocumentJoin
   :document (s/maybe Document)
   })

(def FetchCommand
  {:type (s/enum :fetch)
   :collection CollectionName
   :path PropertyPath
   :values [JValue]
   :values->joins {JValue [UUID]}
   })

(def Command (s/either FetchCommand ScanCommand JoinCommand))

(def IdGeneratorFn (Fn [] Str))

(def ProcessState
  {:foreign-keys [ForeignKey]
   :results [DocumentId]
   :documents {DocumentId Document}
   :scans [ScanCommand]
   :joins {JoinCommandId JoinCommand}
   :joins-seq [JoinCommandId]
   :fetches [FetchCommand]
   :id-gen IdGeneratorFn
   :complete s/Bool
   })

;; UTILITIES
(defn uuid [] (str (java.util.UUID/randomUUID)))
