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

(ns flowgraph.flowgraph
  (:require [flowgraph.thread-control :refer :all]
            [flowgraph.protocols :as protocols]
            [tesserae.core :as tess]))

#_(def queue-histogram
    (em/get-or-register em/DEFAULT 'flowgraph.flowgraph.input-queue.histogram
                        (em/histogram (em/reservoir) "When a job is submitted to a graph, how long is the queue?")))

(defrecord FlowGraph
           [name ;; symbol.
            constructed-args ;; whatever was passed to the constructor when this was made.
            status ;; atom. :working / :error / :broken / :idle / :cancelled
            job-queue ;; queue. Contains [promise inputs] tuples.
            current-promise ;; atom. Contains the promise to which the current results should be delivered
            vertices ;; atom; {:queue-name queue}
            vertex-specs ;; full generated vertex specs
            edges ;; atom; sorted sequence of edge functions, in the order they should be evaluated
            edge-specs ;; full generated edge specs
            n-threads ;; number of threads this graph is configured to use at once
            signaller ;; the signaller!
            active-thread-count ;; atom containing int. Right before a thread dies, it decs this.
            executor ;; ThreadPoolExecutor (Executors/newCachedThreadPool)
            constructors ;; {:vertices {:queue-name constructor}, :edges edge-constructors} -- edge constructors sorted appropriately
            item-countdown ;; atom; used to track completion of jobs
            job-guard] ;; atom; used to isolate queue interactions to one job at a time. See queue namespace.
  protocols/Flow
  (submit [this items]
    (let [p (tess/promise)]
      #_(em/update queue-histogram (protocols/raw-size job-queue))
      (protocols/push job-queue nil [p items])
      (possibly-start-job this)
      p))
  (shutdown [this] (stop-flow this)))
