(ns weave
  (:import [org.bson.types ObjectId]
           [clojure.lang ExceptionInfo])
  (:require [schema.core :as s]
            [clojure.tools.logging :as log]
            [weave.query :as query]
            [weave.core :as c]
            [monger.core :as mg]
            [monger.collection :as mc]
            [clojure.pprint :refer :all]))

(def ^:private tracing
  "the function to use for tracing, e.g:nil log/info prn"
  nil)

(defn ^:private trace 
  [& args]
  (apply tracing args))

(defn ^:private clean
  "Used when displaying state to remove unprintable properties"
  [state]
  (-> state
      (dissoc :id-gen)
      (dissoc :foreign-keys)
      (update-in [:joins-seq] vec)))

(s/defn ^:private get-joins-for-foreign-key :- [c/DocumentJoin]
  "Using the foreign key and a parent document work out the Metadata of the document on the child side"
  [document :- c/Document
   foreign-key :- c/ForeignKey]
  (let [joins (case (:type foreign-key)
                :static
                (let [get-default-insertion-path (fn [from-path]
                                                   (let [last-elem (name (last from-path))
                                                         last-elem (cond 
                                                                    (re-find #"Ids$" last-elem)
                                                                    (clojure.string/replace last-elem #"Ids$" "s")
                                                                    (re-find #"Id$" last-elem)
                                                                    (clojure.string/replace last-elem #"Id$" "s")
                                                                    :else last-elem
                                                                    )]
                                                     (conj (vec (drop-last from-path)) (keyword last-elem))))
                      ;;by default joins insert into the parent by making the from-path singular i.e imageId -> images
                      {:keys [from from-path to to-path]} foreign-key
                      from-value (get-in document from-path)
                      insertion-path (get-default-insertion-path from-path)
                      common-join-props {:insertion-path insertion-path
                                         :insertion-type :list
                                         :foreign-type to
                                         :foreign-path to-path
                                         :value nil ;<= value to be set later
                                         }]
                  (if (or (nil? from-value) (empty? from-value))
                    []
                    (if (coll? from-value)
                      (map #(assoc common-join-props :value %) from-value)   
                      [(assoc common-join-props :value from-value)])))
                :dynamic
                (let [{:keys [from from-path func]} foreign-key
                      from-value (get-in document from-path)]
                  (if (or (nil? from-value) (empty? from-value))
                    []
                    (if (coll? from-value)
                      (map-indexed (fn [index val] 
                                     (let [path-with-index (conj from-path index)
                                           ret (func document val path-with-index)]
                                       (if (empty? (:value ret))
                                         nil
                                         ret)))
                                   from-value)
                      (let [ret (func document from-value from-path)]
                        (if (empty? (:value ret))
                          []
                          [ret])))))
                :multi
                (let [{:keys [from-path func]} foreign-key
                      from-value (get-in document from-path)
                      clone-joins (func document from-value)
                      ]
                  clone-joins))]
        (vec (filter #(not (nil? %)) joins))))

(s/defn ^:private identify-child-joins :- [c/DocumentJoin]
  [type         :- c/DocumentType
   foreign-keys :- [c/ForeignKey]
   doc          :- c/Document]
  (let [foreign-keys-by-from-type (group-by #(get-in % [:from]) foreign-keys)
        foreign-keys (get foreign-keys-by-from-type type)]
    (->> foreign-keys                                              ;- [ForeignKey]
         (mapcat (fn [fk] (get-joins-for-foreign-key doc fk)))     ;- [DocumentJoin]
    )))

(s/defn ^:private jack-in-child :- Object
  "inserts the child into the document (under the collection key)"
  ([parent-doc :- c/Document
    child      :- {:join c/DocumentJoin :document Object}]
   (jack-in-child parent-doc (:document child) (:join child)))
  ([parent-doc :- c/Document
    child-doc  :- c/Document
    join       :- c/DocumentJoin]
   ;; for multi/clone joins we will get an index as part of the path, however theses indexes will need to come in 
   ;; order if we wanted to actually create a vector. 
   ;; Instead we generate a map with integer keys which can be converted later on. 
   ;; Not great but not a total hack given the time available
   (let [insertion-path (join :insertion-path)
         insertion-type (join :insertion-type)
         prop-exists? (fn [obj keys]
                        (get-in obj keys))]
     (if (not (prop-exists? parent-doc insertion-path))
       (case insertion-type
         :value
         (assoc-in parent-doc insertion-path child-doc)
         :list
         (assoc-in parent-doc insertion-path [child-doc]))
       ;;the easy case the property already exists
       (case insertion-type
         :value
         (assoc-in parent-doc insertion-path child-doc)  ;we'll be erasing whats there!!
         :list               
         (update-in parent-doc insertion-path #(conj % child-doc)))))))

(s/defn ^:private join-summary :- c/Str
  [{:keys [foreign-type foreign-path value] :as join} :- c/DocumentJoin]
  (str {:type foreign-type :path foreign-path :values [value]}))

(s/defn ^:private command-summary :- c/Str
  [command :- c/Command]
  (case (:type command)
    :scan (str "scan " (:doc-id command))
    :fetch (str "fetch, collection" (:collection command) ", path:" (:path command) ", values:" (command :values))
    :join (str "join, from:" (get-in command [:join :foreign-type]) ", path:" (get-in command [:join :foreign-path]) ", value:" (get-in command [:join :value]) ", into:" (command :parent-id) ", at:" (get-in command [:join :insertion-path]))))

(s/defn ^:private ->scan :- c/ScanCommand
  [doc :- c/Document]
  {:type :scan
   :doc-id (:_id doc)})

(s/defn ^:private ->join :- c/JoinCommand
  [id        :- c/UUID
   parent-id :- c/DocumentId
   join      :- c/DocumentJoin]
  {:id id
   :type :join
   :parent-id parent-id
   :join join
   :document nil})

(s/defn ^:private ->fetch :- c/FetchCommand
  [{:keys [id join] :as join-command} :- c/JoinCommand]
  (let [{:keys [foreign-type foreign-path value]} join]
    (when tracing (trace "->fetch " (join-summary join)))
    {:type :fetch
     :collection foreign-type
     :path foreign-path
     :values [value]
     :values->joins {value [id]}}))

(s/defn ^:private return :- [c/Document]
  "returns the documents from the process state"
  [{:keys [results documents] :as state} :- c/ProcessState]
  (let [doc-ids results                                          ;- [DocumentId]
        selected-map-of-ids-docs (select-keys documents doc-ids) ;- {DocumentId Document}
        ]
  (vec (vals selected-map-of-ids-docs))))

(s/defn ^:private pop-first :- [(s/one s/Command "Command") (s/one c/ProcessState "ProcessState")]
  [state :- c/ProcessState
   from  :- s/Keyword]
  (let [command (first (from state))
        remaining (or (vec (next (from state))) [])
        new-state (assoc state from remaining)]
    [command new-state]))

(s/defn ^:private query-state-documents :- [c/Document]
  ([state :- c/ProcessState
    {:keys [collection path values] :as fetch} :- c/FetchCommand]
     (when-not (= 1 (count values))
       (throw (Exception. (str "only one value should be expected currently in FetchCommand :values"))))
    (query-state-documents state collection path (first values)))
  ([{:keys [documents] :as state} :- c/ProcessState
    type                          :- c/CollectionName
    path                          :- c/PropertyPath
    value                         :- Object]
   (when (seq documents)
     (let [predicate (fn [doc]
                       (and (= type (:_type doc))
                            (= value (get-in doc path))))   ;single-value assumption
           matches (filter predicate (vals documents))]
       (when (seq matches)
         (first matches))))))

(s/defn ^:private handle-fetch-results :- c/ProcessState
  "executes the fetch hydrating the document in the relevant join"
  [state :- c/ProcessState
   {:keys [id collection path values values->joins] :as command} :- c/FetchCommand
   docs :- [c/Document]]
  (if-not (= (count docs) 1)
    (let [message (str "Received " (count docs) " documents for what currently should be a unique query.... this suggests the data is bad. Query => " (str "(congo/fetch " (:collection command) " :where {" (first (:path command)) " " (first (:values command)) "})"))]
      (throw (Exception. message))))
  (let [;group the docs by id
        docs-by-id (into {} (map (fn [doc] [(:_id doc) doc]) docs))   ;- {DocumentId Document}       
        ;add these documents to our overall map
        state (update-in state [:documents] #(merge % docs-by-id))
        ;create a lookup to get from current value->doc !!!is their a bad assumption here about the uniqueness of the foreign-key... it should be unique
        docs-by-value (into {} (map (fn [doc] [(get-in doc path) doc]) docs))
        ;ASSUME WE ONLY HAVE ONE JOIN + ONE VALUE 
        ;TODO MAKE A FUNCTION AND REDUCE OVER IT
        value (first values)
        join-command-ids (get values->joins value)
        join-command-id (first join-command-ids)  ;<- limitation
        join-command (get (state :joins) join-command-id)
        doc (get docs-by-value value)
        join-command (assoc join-command :document doc)
        ;update the join-command in the state :joins
        state (assoc-in state [:joins join-command-id] join-command)
        ;create a new scan-command per document
        state (update-in state [:scans] #(conj % (->scan doc)))]
    state))

(s/defn ^:private optimize :- c/ProcessState
  [{:keys [documents fetches scans] :as in-state} :- c/ProcessState]
  (let [state in-state
        already-fetched (when (and (not (seq scans))
                                   (seq documents)
                                   (seq fetches))
                          (reduce
                           (fn [acc fetch]
                             (if-let [matching-doc (query-state-documents state fetch)]
                               (conj acc {:fetch fetch
                                          :document matching-doc
                                          })
                               acc
                               ))
                           []
                           fetches))
                           ;- [{:fetch Fetch :document Document}
        state (if (seq already-fetched)
                ;apply the handle-fetch-results to each fetch
                (reduce
                  (fn [acc-state {:keys [fetch document] :as pre-fetched-row}]
                    (when tracing (trace "optimized => " (command-summary fetch)))
                    (-> acc-state
                        ;remove the fetch from the state
                        (update-in [:fetches] (fn [fetches]
                                                (filterv (fn [f]
                                                           (not (= fetch f)))
                                                         fetches)))
                        ;handle the fetch instead of executing it
                        (handle-fetch-results fetch [document]))  ;<-- again single to multiple
                     )
                  state
                  already-fetched)
                state)]
  (when (nil? state)
    (throw (Exception. (str "state is nil"))))
  state))

(s/defn ^:private _join :- c/ProcessState
  "implements the join"
  [state :- c/ProcessState
   {:keys [id join parent-id] :as command} :- c/JoinCommand]
  (when tracing (trace "_join"))
  (let [child-doc-id (get-in command [:document :_id])
        child-doc (get (:documents state) child-doc-id)
        parent-doc (get (:documents state) parent-id)
        parent-doc (jack-in-child parent-doc child-doc join)
        state (update-in state [:documents] #(assoc % parent-id parent-doc))]
    state))

(s/defn ^:private analyse :- [(s/one c/Command "Command") (s/one c/ProcessState "ProcessState")]
  "analyses the commands to figure out the next step, sorts them putting the next one first, proceeds in the order scans, fetches, joins"
  [state :- c/ProcessState]
  (when tracing (trace "analysing"))
  (let [{:keys [scans fetches joins-seq]} state
        ;work through the commands doing the scans, fetches then joins, when empty complete!
        [command new-state] (cond
                             (seq scans)
                               (pop-first state :scans)
                             (seq fetches)
                               (-> state
                                   (pop-first :fetches))
                             (seq joins-seq)
                               (let [[join-id state] (pop-first state :joins-seq)
                                     join-command (get (:joins state) join-id)
                                     state (update-in state [:joins] #(dissoc % join-id))
                                     ]
                                 [join-command state])
                             :else
                               [nil (assoc state :complete true)])]
    (when tracing
      (if command
        (trace "analysis => " (command-summary command))
        (trace "analysis => complete")))
    [command new-state]))

(s/defn ^:private _scan :- c/ProcessState
  [{:keys [foreign-keys id-gen] :as state} :- c/ProcessState
   {:keys [doc-id] :as command} :- c/ScanCommand]
  (when tracing (trace "_scan:" (command-summary command)))
  (let [doc (get (:documents state) doc-id)
        collection (:_type doc)
        doc-joins (identify-child-joins collection foreign-keys doc) ;-[DocumentJoin]
        ;generate join commands to execute the join into the document
        join-commands (map #(->join (id-gen) (:_id doc) %) doc-joins)
        ;generate fetch commands to hydrate the joins
        fetch-commands (map ->fetch join-commands)
        state (update-in state [:fetches] #(vec (concat % fetch-commands)))
        ;PREPEND the join ids to the start of the  in sequence into joins-seq
        state (update-in state [:joins-seq] #(concat (map :id join-commands) %))
        ;create the joins-by-id
        join-commands-by-id (into {} (map (fn [jc] [(:id jc) jc]) join-commands))
        ;merge these into the joins lookup map
        state (update-in state [:joins] #(merge % join-commands-by-id))]
    state))

(defn ^:private union-re-patterns [& patterns]
  "ref: http://stackoverflow.com/questions/21416917/multiple-regular-expressions-in-clojure"
  (let [uber-str (apply str (interpose "|" (map #(str "(" % ")") patterns)))]
    (re-pattern uber-str)))

;;public+pure
(s/defn filter-foreign-keys :- [c/ForeignKey]
  [foreign-keys :- [c/ForeignKey]
   patterns :- [java.util.regex.Pattern]]
  (when (= 0 (count patterns)) (throw (ExceptionInfo. "no patterns" 
                                                     {:type :unprocessable-entity
                                                      :body {:cause "no patterns provided for filter-foreign-keys"
                                                             :result patterns}
                                                      })))
  (let [uber-pattern (if (> (count patterns) 1)
                            (apply union-re-patterns patterns)
                            (first patterns))
        matches (filterv #(re-matches uber-pattern (:name %)) foreign-keys)]
    matches))

(s/defn ^:private ->state :- ProcessState
  "create the initial state for the processor"
  [foreign-keys :-   [c/ForeignKey]
   root-documents :- [c/Document]
   id-generate-fn :- Object  ;no-args function]
  (when tracing (trace "->state"))
  (let [doc-ids (vec (map :_id root-documents))
        scan-commands (vec (map #(->scan %) root-documents))]
    {:baggage baggage
     :foreign-keys foreign-keys    ;explains the joins we should make
     :results doc-ids              ;store the ids of the root documents we will return
     :documents (into {} (map (fn [doc] [(:_id doc) doc]) root-documents)) ;keeps the state of all documents
     :scans scan-commands       ;initiate the process with a scan
     :joins {}
     :joins-seq []
     :fetches []
     :id-gen id-generate-fn
     :complete false               ;indicates when the joining process has completed
     }))

;; IMPURE
(s/defn ^:IO ^:private get-documents-with-values :- [c/Document]
  [db         :- c/DB
   path       :- c/PropertyPath
   collection :- c/CollectionName
   values     :- [c/JValue]]
  (let [using-mongo-id (= [:_id] path)
        wip-values (if using-mongo-id
                     (vec (map #(ObjectId. %) values))
                     values)
        path-in-dot-notation (query/nest path)
        query {path-in-dot-notation {"$in" wip-values}}
        _ (when tracing (trace "query->" collection query))
        results (mc/find-as-maps db collection query)
        _ (when tracing (trace "query<-" (count results)))]
    results))

(s/defn ^:IO ^:private _fetch :- c/ProcessState
  [db    :- c/DB
   state :- c/ProcessState
   {:keys [id collection path values values->joins] :as command} :- c/FetchCommand]
  (when tracing (trace "_fetch " (command-summary command)))
  (let [docs (get-documents-with-values baggage path collection values)
        state (handle-fetch-results state command docs)]
    state))

(s/defn ^:IO ^:private process :- c/ProcessState
  [db             :- c/DB
   entering-state :- c/ProcessState]
  (loop [{:keys [commands complete] :as state} entering-state]
    (when tracing (trace "process-start"))
    (let [wip-state (optimize state)
          [command wip-state] (analyse wip-state)]
      (if (wip-state :complete)
        wip-state
        (let [_ (when tracing (trace "processing: " command))
              _ (when tracing (trace "processing: " (command-summary command)))
              new-state (case (:type command)
                          :scan (_scan wip-state command)
                          :fetch (_fetch db wip-state command)
                          :join (_join wip-state command)
                          )]
          (recur new-state))))))

;; PUBLIC API
(s/defn ^:IO find-maps :- [c/Document]
  "wraps the find-maps call with the additional of the foreign keys to join"
  [db         :- c/DB
   collection :- c/CollectionName
   where      :- c/Query
   join       :- [c/ForeignKey]]
  (when tracing (trace (str "(weave/find-maps db " collection " " where " " join)))
  (let [results (mg/find-maps db collection where)
        _ (when tracing (trace "primary-fetch " results))
        state (->state join results c/uuid)
        wip-state (process db state)
        docs (return wip-state)]
    docs))
