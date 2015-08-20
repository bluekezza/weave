(ns weave.core
  (:require [schema.core :as s]
            [clojure.tools.logging :as log]))

;; SCHEMAS
(def Str (s/both s/Str (s/pred #(not (clojure.string/blank? %)))))

(def DB Object)

(def Query clojure.lang.PersistentHashMap)

(def DocumentId s/Str)

(def Document Object) ;;TODO, make this more specific

(def CollectionName s/Keyword)

(def DocumentType s/Keyword)

(def PropertyPath [(s/either s/Keyword s/Int)])

(def DocumentJoin {:foreign-path PropertyPath
                   :foreign-type DocumentType
                   :value s/Str
                   :insertion-path PropertyPath
                   :insertion-type (s/enum :list :value)
                   })

(def ForeignKeyTypes [:static :dynamic :multi])

(def ForeignKey {:name s/Str
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

(def UUID s/Str)

(def ScanCommand
  {:type (s/enum :scan)
   :doc-id s/Str
   })

(def JoinCommandId s/Str)

(def JoinCommand
  {:id JoinCommandId   
   :type (s/enum :join)
   :parent-id DocumentId
   :join /DocumentJoin
   :document (s/maybe Document)   ;<- this is where the fetch will inject the document for the join
   })

(def FetchCommand
  {:type (s/enum :fetch)
   :collection CollectionName
   :path PropertyPath
   :values [JValue]
   :values->joins {JValue [UUID]}
   })

(def Command (s/either FetchCommand ScanCommand JoinCommand))

(def ProcessState
  {:foreign-keys [ForeignKey]
   :results [DocumentId]
   :documents {DocumentId Document}
   ;:commands
   :scans [ScanCommand]
   :joins {JoinCommandId JoinCommand}
   :joins-seq [JoinCommandId] ;- clojure.lang.PersistentList?
   :fetches [FetchCommand]
   :id-gen Object ;- no-args function that returns an id
   :complete s/Bool
   }
)

;; UTILITIES
(defn uuid [] (str (java.util.UUID/randomUUID)))
