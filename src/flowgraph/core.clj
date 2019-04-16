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

(ns flowgraph.core
  (:require [flowgraph.flowgraph :as flowgraph]
            [flowgraph.protocols :refer :all :as qp]
            [flowgraph.edge :as edge]
            [flowgraph.thread-control :as tc]
            [flowgraph.queue :as queue]
            [flowgraph.vertex :as vertex]
            [utiliva.comparator :refer [compare-comp]]
            [utiliva.core :refer [map-vals]]
            [potemkin :refer [unify-gensyms]]
            [backtick]
            [clojure.tools.macro :refer [symbol-macrolet]]
            [flowgraph.signal :refer [simple-signaller]])
  (:import [java.util.concurrent Executors ExecutorService]
           [java.lang Runtime])
  (:refer-clojure :exclude [pmap]))

(defn- >priority [e1 e2] (- (:priority e2) (:priority e1)))
(defn- commix>fuse>duplicate>discriminate>transform
  [e1 e2]
  (let [type-priority (zipmap [:commix :fuse :duplicate :discriminate :transform] (range))]
    (compare (type-priority (:type e1)) (type-priority (:type e2)))))
(def edge-comparator (compare-comp commix>fuse>duplicate>discriminate>transform >priority))

(defn flowgraph
  ([edge-specs]
   (flowgraph (gensym "anonymous-graph") edge-specs))
  ([graph-name edge-specs]
   (flowgraph graph-name edge-specs {}))
  ([graph-name edge-specs vertex-specs]
   (flowgraph graph-name edge-specs vertex-specs nil))
  ([graph-name edge-specs vertex-specs constructed-args]
   (flowgraph graph-name edge-specs vertex-specs constructed-args nil))
  ([graph-name edge-specs vertex-specs constructed-args num-threads]
   (let [edge-specs (edge/edge-specs edge-specs)
         edge-constructors (for [edge (sort edge-comparator edge-specs)]
                             (edge/compile-edge-gen graph-name edge))
         vertex-specs (vertex/vertex-specs vertex-specs edge-specs)
         vertex-constructors (zipmap (keys vertex-specs)
                                     (for [spec vertex-specs] (vertex/vertex-gen spec)))
         graph (flowgraph/map->FlowGraph {:name graph-name
                                          :status (atom :idle)
                                          :constructed-args constructed-args
                                          :job-queue (queue/queue {})
                                          :current-promise (atom nil)
                                          :vertices (atom nil)
                                          :vertex-specs vertex-specs
                                          :edges (atom nil)
                                          :edge-specs edge-specs
                                          :n-threads (or num-threads *thread-capacity*)
                                          :signaller (simple-signaller)
                                          :active-thread-count (atom 0)
                                          :executor (Executors/newCachedThreadPool)
                                          :constructors {:vertices vertex-constructors
                                                         :edges edge-constructors}
                                          :item-countdown (atom 0)
                                          :job-guard (atom 0)})
         recoverable-error-fn (tc/recoverable-error-fn graph)
         vertices (into {} (map-vals #(%) vertex-constructors))
         edges (map #(% vertices (:item-countdown graph) recoverable-error-fn)
                    edge-constructors)]
     (qp/set-guard! (:job-queue graph) nil) ;; implementation detail
     (reset! (:vertices graph) vertices)
     (reset! (:edges graph) edges)
     (doseq [task (repeat (:n-threads graph) (fn [] (tc/thread-flow graph)))]
       (.submit ^ExecutorService (:executor graph) ^java.lang.Runnable task))
     graph)))

(defmacro deflow
  ([graph-name args edge-specs]
   (unify-gensyms `(deflow ~graph-name ~args ~edge-specs {})))
  ([graph-name args edge-specs vertex-specs]
   `(defn ~graph-name
      ~args
      (flowgraph '~graph-name ~edge-specs ~vertex-specs (zipmap '~args ~args)))))

;; Simple example:

#_(deflow pmapper [f] {:source (edge/transform #(apply f %) :sink :coordinated? true)})
#_(defn pmap
    "A ridiculously naive implemenation of pmap using a flowgraph graph. For some cases,
  this may be faster than the built-in pmap. No promises. YMMV."
    [f & cs]
    (let [graph (flowgraph 'pmap {:source (edge/transform #(apply f %) :sink :coordinated? true)})
          results @(submit graph (apply map list cs))]
      (shutdown graph)
      results))
