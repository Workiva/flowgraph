;; Copyright 2016-2019 Workiva Inc.
;; 
;; Licensed under the Apache License, Version 2.0 (the "License");
;; you may not use this file except in compliance with the License.
;; You may obtain a copy of the License at
;; 
;;     http://www.apache.org/licenses/LICENSE-2.0
;; 
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.

(ns flowgraph.edge
  (:require [flowgraph.utils :refer [lock-form locking-sym-on-coordination]]
            [flowgraph.queue :refer :all]
            [flowgraph.protocols :refer :all]
            [recide.core :refer [insist]]
            [manifold.deferred :as d] ;; TODO: replace with tessera
            [potemkin :refer [unify-gensyms]])
  (:refer-clojure :exclude [pop peek]))

(defn transform
  "A 1->1 spec that transforms data by passing it through the function f.
  Accepts optional named values (along with appropriate supporting values):
  #{:batching? :asynchronous? :coordinated? :collecting? :priority}"
  [f dest & {:as spec}]
  (insist (and (ifn? f) (keyword? dest)))
  (assoc spec :function f :dest-name dest :type :transform))

(defn discriminate
  "A 1->* spec that simply passes data to one of a number of destinations,
  determined by the result of calling predicate pred on each item. result->destination
  is expected to be a map from result-of-pred to destination name.
  Accepts optional named values (along with appropriate supporting values):
  #{:coordinated? :priority}"
  [pred result->destination & {:as spec}]
  (insist (and (ifn? pred) (map? result->destination) (every? keyword? (vals result->destination))))
  (assoc spec
         :predicate pred
         :result->destination result->destination
         :dest-names (vals result->destination)
         :type :discriminate))

(defn duplicate
  "A 1->* spec that duplicates data along every outgoing edge.
  Accepts optional named values (along with appropriate supporting values):
  #{:coordinated? :priority}"
  [dests & {:as spec}]
  (insist (and (sequential? dests) (every? keyword? dests)))
  (assoc spec :dest-names dests :type :duplicate))

(defn commix
  "A *->1 spec that commixs data flow; i.e., the first source queue
  containing a ready item is polled for a value which is put into the destination.
  This is the opposite of duplicate.
  Accepts optional named values (along with appropriate supporting values):
  #{:coordinated? :priority}"
  [dest & {:as spec}]
  (insist (keyword? dest))
  (assoc spec :dest-name dest :type :commix))

(defn fuse
  "A *->1 spec that fuses data from a multiplicity of sources: it places into
  the destination queue a vector containing the outputs of all input queues. Note
  that this does nothing until all input queues are ready to deliver an item. This
  means, among other things, that combination operations are always :coordinated?
  Accepts optional named values (along with appropriate supporting values):
  #{:priority}"
  [dest & {:as spec}]
  (insist (keyword? dest))
  (assoc spec :dest-name dest :type :fuse))

(def default-edge-spec
  {:asynchronous? false,
   :batching? false,
   :collecting? false,
   :coordinated? false,
   :timeout-ms nil,
   :priority 1})

(defn edge-spec
  [[source-or-sources spec]] ;; <-- expects mapentry
  (insist (or (and (sequential? source-or-sources)
                   (every? keyword? source-or-sources)
                   (#{:commix :fuse} (:type spec)))
              (and (keyword? source-or-sources)
                   (#{:transform :discriminate :duplicate} (:type spec)))))
  (cond-> (into default-edge-spec spec)
    (#{:transform :discriminate :duplicate} (:type spec)) (assoc :source-name source-or-sources)
    (#{:commix :fuse} (:type spec)) (assoc :source-names source-or-sources)))

(defn edge-specs
  [specs]
  (map edge-spec specs))

(defn source-or-sources [edge] (or (:source-names edge) (:source-name edge)))
(defn dest-or-dests [edge] (or (:dest-names edge) (:dest-name edge)))

;; ========================
;;     EDGE GENERATION:
;; ========================

(defmacro use-timeout [sym ms error-sym edge-name]
  (if ms
    `(d/chain (d/timeout! ~sym ~ms ::timed-out)
              (fn [x#] (when (= x# ::timed-out) (~error-sym :timed-out '~edge-name))))
    nil))

(defn modify-counter
  "Uses the edge spec to return nil or a sequence containing the correct swap!
  action for the global-counter, based on whether the edge is batching, collecting,
  or neither; and whether it leads to the sink or not."
  [{:keys [dest-name batching? collecting?]}]
  (if (or batching? collecting?)
    `((swap! global-counter## - (count init##)
             ~@(when (not= :sink dest-name)
                 (if batching?
                   `((- (count result##)))
                   `(-1)))))
    (when (= :sink dest-name) `((swap! global-counter## dec)))))

(defn pusher
  "Chooses the correct push-* function from the queues protocol based on whether
  the edge is batching, coordinated, neither, or both."
  [{:keys [batching? coordinated?]}]
  (cond (and batching? coordinated?) `push-deferred-batch
        batching? `push-batch
        coordinated? `push-deferred
        :else `push))

(defmulti transform-body
  (juxt :asynchronous? :coordinated?))
(defmethod transform-body [false false]
  [{:keys [dest-name timeout-ms source-sym destination-sym] :as spec}]
  `(when-let [init## (pop ~source-sym guard## force##)]
     (let [result## (try (function## init##) (catch Throwable e# (error-fn## e# edge-name##)))]
       (~(pusher spec) ~destination-sym guard## result##)
       ~@(modify-counter spec))
     true))
(defmethod transform-body [false true]
  [{:keys [dest-name timeout-ms source-sym destination-sym] :as spec}]
  `(let [deferred# (d/deferred)]
     (when-let [init## (pop ~source-sym guard## force##)]
       (~(pusher spec) ~destination-sym guard## deferred#)
       (let [result## (try (function## init##) (catch Throwable e# (error-fn## e# edge-name##)))]
         ~@(modify-counter spec)
         (d/success! deferred# result##))
       true)))
(defmethod transform-body [true false]
  [{:keys [dest-name timeout-ms source-sym destination-sym] :as spec}]
  `(when-let [init## (pop ~source-sym guard## force##)]
     (let [future# (d/chain (d/future (try (function## init##) (catch Throwable e# (error-fn## e# edge-name##))))
                            (fn [result##]
                              (~(pusher spec) ~destination-sym guard## result##)
                              ~@(modify-counter spec)))]
       (use-timeout future# ~timeout-ms error-fn## edge-name##))
     true))
(defmethod transform-body [true true]
  [{:keys [dest-name timeout-ms source-sym destination-sym] :as spec}]
  `(let [deferred# (d/deferred)]
     (when-let [init## (pop ~source-sym guard## force##)]
       (~(pusher spec) ~destination-sym guard## deferred#)
       (let [future# (d/chain (d/future (try (function## init##) (catch Throwable e# (error-fn## e# edge-name##))))
                              (fn [result##]
                                ~@(modify-counter spec)
                                (d/success! deferred# result##)))]
         (use-timeout future# ~timeout-ms error-fn## edge-name##))
       true)))

(defn compile-transform-gen
  [graph-name {:keys [coordinated? asynchronous? batching? collecting? function source-name dest-name]
               :as edge-spec}]
  (let [edge-name (->> [graph-name ":" (name source-name) '-> (name dest-name) ":"
                        (when asynchronous? 'asynchronous-)
                        (when batching? 'batching-)
                        (when collecting? 'collecting-)
                        (if coordinated? 'coordinated 'uncoordinated)]
                       (apply str)
                       (symbol))
        source-sym (gensym (name source-name))
        destination-sym (gensym (name dest-name))
        body (transform-body (assoc edge-spec
                                    :dest-name dest-name
                                    :source-sym source-sym
                                    :destination-sym destination-sym))
        body (if coordinated? (locking-sym-on-coordination source-sym #{source-sym destination-sym} body) body)
        shell `(fn [function##]
                 (fn [queues## global-counter## error-fn##]
                   (let [~source-sym (get queues## ~source-name)
                         ~destination-sym (get queues## ~dest-name)
                         edge-name## '~edge-name]
                     (fn ~edge-name
                       [force## guard##]
                       (try
                         ~body
                         (catch Exception e#
                           (error-fn## e# '~edge-name)))))))]
    ((eval (unify-gensyms shell)) function)))

(defn compile-duplicate-gen
  [graph-name {:keys [coordinated? priority source-name dest-names] :as edge-spec}]
  (let [name (->> (concat [graph-name ":" source-name '->]
                          (interpose '& dest-names)
                          [":" (if coordinated? 'coordinated 'uncoordinated)])
                  (apply str)
                  (symbol))
        counter-diff (- (count dest-names) (if (some #(= :sink %) dest-names) 2 1))
        main `(when-let [v# (pop source## guard## force##)]
                (doseq [destination# destinations##]
                  (push destination# guard## v#))
                ~@(when-not coordinated? `((swap! global-counter## + ~counter-diff)))
                true)
        shell `(fn [queues## global-counter## error-fn##]
                 (let [source## (get queues## ~source-name)
                       destinations## (map #(get queues## %) ~dest-names)]
                   (fn ~name
                     [force## guard##]
                     ~(if coordinated?
                        `(when (locking source## ~main)
                           (swap! global-counter## + ~counter-diff)
                           true)
                        main))))]
    (eval (unify-gensyms shell))))

(defn compile-discriminate-gen
  [graph-name {:keys [coordinated? result->destination predicate source-name dest-names] :as edge-spec}]
  (let [name (->> (concat [graph-name ":" source-name '->]
                          (interpose '| dest-names)
                          [":" (if coordinated? 'coordinated 'uncoordinated)])
                  (apply str)
                  (symbol))
        shell `(fn [predicate## result->destination##]
                 (fn [queues## global-counter## error-fn##]
                   (let [source## (get queues## ~source-name)
                         edge-name## '~name]
                     (fn ~name
                       [force## guard##]
                       ~(if coordinated?
                          `(when-let [destination# (locking source##
                                                     (when-let [v# (pop source## guard## force##)]
                                                       (let [destination# (result->destination## (try (predicate## v#)
                                                                                                      (catch Throwable e#
                                                                                                        (error-fn## e# edge-name##))))]
                                                         (push (get queues## destination#) guard## v#)
                                                         destination#)))]
                             (when (= :sink destination#)
                               (swap! global-counter## dec)))
                          `(when-let [v# (pop source## guard## force##)]
                             (let [destination# (result->destination## (predicate## v#))]
                               (push (get queues## destination#) guard## v#)
                               (when (= :sink destination#)
                                 (swap! global-counter## dec)))))))))]
    ((eval (unify-gensyms shell)) predicate result->destination)))

(defn compile-commix-gen
  [graph-name {:keys [coordinated? source-names dest-name] :as edge-spec}]
  (let [name (->> (concat [graph-name ":"]
                          (interpose '| source-names)
                          ['-> dest-name ":" (if coordinated? 'coordinated 'uncoordinated)])
                  (apply str)
                  (symbol))
        main `(do (push destination## guard## value##)
                  ~@(when (= :sink dest-name) `((swap! global-counter## dec)))
                  true)
        shell `(fn [queues## global-counter## error-fn##]
                 (let [sources## (map #(get queues## %) ~source-names)
                       destination## (get queues## ~dest-name)]
                   (fn ~name
                     [force## guard##]
                     (loop [[source## & sources##] sources##]
                       ~(if coordinated?
                          `(let [pushed?# (locking source##
                                            (when-let [value## (pop source## guard## force##)]
                                              ~main))]
                             (or pushed?# (when sources## (recur sources##))))
                          `(if-let [value## (pop source## guard## force##)]
                             ~main
                             (when sources## (recur sources##))))))))]
    (eval (unify-gensyms shell))))

(defn compile-fuse-gen
  [graph-name {:keys [source-names dest-name] :as edge-spec}]
  (let [name (->> (concat [graph-name ":"]
                          (interpose '& source-names)
                          ['-> dest-name ":" 'coordinated])
                  (apply str)
                  (symbol))
        shell `(fn [queues## global-counter## error-fn##]
                 (let [sources## (map #(get queues## %) ~source-names)
                       destination## (get queues## ~dest-name)
                       locking-queue## (first sources##)]
                   (fn ~name
                     [force## guard##]
                     (try
                       (locking locking-queue##
                         (when (every? #(ready? %) sources##)
                           (let [value## (mapv #(pop % guard## force##) sources##)]
                             (push destination## guard## value##)
                             ~(if (= :sink dest-name)
                                `(swap! global-counter## - (count value##))
                                `(swap! global-counter## - (dec (count value##)))))
                           true))
                       (catch Exception e#
                         (error-fn## e# '~name))))))]
    (eval (unify-gensyms shell))))

(defn compile-edge-gen
  [graph-name edge-spec]
  ((case (:type edge-spec)
     :transform compile-transform-gen
     :duplicate compile-duplicate-gen
     :discriminate compile-discriminate-gen
     :commix compile-commix-gen
     :fuse compile-fuse-gen) graph-name edge-spec))
