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

(ns flowgraph.thread-control
  (:require [flowgraph.protocols :as queue :refer [ready? pop push peek drain]]
            [flowgraph.signal :refer :all]
            [flowgraph.error :refer [timeout error]]
            [tesserae.core :as tess]
            [utiliva.core :as util]
            [manifold.deferred :as d])
  (:import [java.util.concurrent Executors])
  (:refer-clojure :exclude [pop peek]))

(defn unless-shutdown
  "Use this with swap! instead of reset! on the graph's status, unless
  you're explicitly starting it back up after it's been shut down."
  [old new] (if (= old :shutdown) old new))

(defn when-viable
  "Use this with swap! instead of reset! on the graph's status for 'normal' operations.
  It won't overwrite :shutdown, :error, :cancelled, or :broken states."
  [old new] (if (#{:shutdown :error :broken :cancelled} old) old new))

(defn- clear-vertices
  "Empties all vertices of queued items, produces a new guard value,
  sets all vertices to use it, then returns it."
  [graph]
  (let [guard (swap! (:job-guard graph) unchecked-inc)]
    (doseq [[vname vertex] @(:vertices graph)]
      (queue/set-guard! vertex guard))
    guard))

(defn- work-loop
  "Goes down the list of edges trying to do work. If the flow graph is in a :working
  state, and if the :job-guard hasn't changed, and if it finds work to do, then this
  function recurs. Otherwise, it returns nil."
  [force? graph guard]
  (when (and (= @(:status graph) :working)
             (= guard @(:job-guard graph))
             (some #(% force? guard) @(:edges graph)))
    (recur force? graph guard)))

(defn job-canceller
  "Produces a watch-fn that puts the graph into a cancelled state IFF the
  tessera is ever revoked."
  [graph]
  (fn [p]
    (when (tess/revoked? p)
      (locking (:status graph)
        (when (= p @(:current-promise graph))
          (reset! (:current-promise graph) nil)
          (reset! (:status graph) :cancelled))))))

(declare launch-threads)

(defn- start-job
  [graph guard p items]
  (do (reset! (:current-promise graph) p)
      (doseq [item items]
        (queue/push (-> graph :vertices deref :source) guard item))
      (reset! (:item-countdown graph) (count items))
      (swap! (:status graph) when-viable :working)
      (launch-threads graph guard)))

(defn possibly-start-job
  "If the graph is in an idle state, and if there is some job queued, this kicks off the next job.
  Sets the graph state to :working. "
  [graph]
  (locking (:status graph)
    (let [status @(:status graph)]
      (when (= status :idle)
        (let [guard (clear-vertices graph)]
          (when-let [[p items] (queue/pop (:job-queue graph) nil)]
            (if (tess/watch p (job-canceller graph))
              (start-job graph guard p items)
              (when-not (realized? p)
                (tess/fumble p (IllegalStateException.
                                (format "Flowgraph graph '%s' encountered a tessera malfunction: watch fn rejected."
                                        (:name graph))))))))))))

(defn- possibly-finish
  "Checks whether we think we're done based off of our item counter. If so,
  we deliver the accumulated results to the current promise, then we possibly-start-job.
  But if not, we run a single pass over the edges forcing any waiting work, then launch all
  threads again."
  [graph guard]
  (if (zero? @(:item-countdown graph))
    (let [p @(:current-promise graph)]
      (reset! (:current-promise graph) nil)
      (tess/fulfil p (drain (:sink @(:vertices graph)) guard))
      (locking (:status graph)
        (swap! (:status graph) when-viable :idle)
        (possibly-start-job graph)))
    (do (work-loop true graph guard)
        (launch-threads graph guard))))

(defn- handle-error-state
  "If the graph is in an error state, we abandon the current job (if we have one),
  we deliver an 'unknown error' IllegalStateException to the current promise (if
  there is one), set the graph status to :idle, and possibly-start-job."
  [graph]
  (locking (:status graph)
    (when-let [p @(:current-promise graph)]
      (reset! (:current-promise graph) nil)
      (tess/fumble p (error (format "unknown error executing graph named '%s'." (:name graph)) {})))
    (swap! (:status graph) unless-shutdown :idle)
    (possibly-start-job graph)))

(defn- handle-cancelled-state
  "If the graph is in an error state, we abandon the current job (if we have one),
  we deliver an 'unknown error' IllegalStateException to the current promise (if
  there is one), set the graph status to :idle, and possibly-start-job."
  [graph]
  (locking (:status graph)
    (swap! (:status graph) unless-shutdown :idle)
    (possibly-start-job graph)))

(defn recoverable-error-fn
  "This is designed to be the default error handler for edges. Depending on whether
  the error is a timeout or something else, this delivers an IExceptionInfo with the detailed messages
  to the current promise (if there is one), then sets the graph status to :error."
  [graph]
  (fn [e & [edge-name & {:as data}]]
    (locking (:status graph)
      (when-let [p @(:current-promise graph)]
        (reset! (:current-promise graph) nil)
        (if (= e :timed-out)
          (->> (timeout (format "A timeout occurred in %s in the graph '%s'."
                                (if edge-name (str "the edge '" edge-name "'") "an unknown operation")
                                (:name graph))
                        {:data data})
               (tess/fumble p))
          (->> (error (format "An error occurred in %s in the graph '%s'"
                              (if edge-name (str "the edge '" edge-name "'") "an unknown operation")
                              (:name graph))
                      {:data data}
                      e)
               (tess/fumble p))))
      (reset! (:status graph) :error))))
(defn hard-fail-error-fn [])

(defn- recreate-all-state
  "Used in response to total failure (graph status = :broken). Throws away the
  old vertices and edges, generates new ones, swaps them in, and possibly-start-job."
  [graph]
  (locking (:status graph)
    (let [new-queues (into {} (util/map-vals #(%)) (:vertices @(:constructors graph)))
          new-edges (map #(% new-queues
                             (:item-countdown graph)
                             recoverable-error-fn
                             hard-fail-error-fn)
                         (:edges @(:constructors graph)))
          new-edges (concat (map #(partial % false) new-edges)
                            (map #(partial % true) new-edges))]
      (reset! (:vertices graph) new-queues)
      (reset! (:edges graph) new-edges)
      (swap! (:status graph) unless-shutdown :idle)
      (possibly-start-job graph))))

(defn stop-flow
  "Designed to be called when tearing down a flow graph. This sets the graph status
  to :shutdown, delivers IllegalStateExceptions to current and all queued promises,
  instructs the executor to shutdownNow(), and signals any currently running threads
  to :die."
  [graph]
  (locking (:status graph)
    (reset! (:status graph) :shutdown)
    (when-let [p @(:current-promise graph)]
      (tess/fumble p (error "Flow graph was shut down while processing your request." {:graph graph})))
    (let [e (error "Flow graph was shut down before your request was processed." {:graph graph})]
      (doseq [[p _] (drain (:job-queue graph) graph)]
        (tess/fumble p e)))
    (.shutdownNow ^java.util.concurrent.ExecutorService (:executor graph))
    (signal (:signaller graph) :die (:n-threads graph))))

(defn thread-flow
  "Over-arching thread flow. As long as the current thread is not interrupted, the
  graph state is not set to :shutdown, and the thread hasn't been signalled to :die,
  this does a work-loop until exiting, decrements the graph's active thread count,
  and waits for another signal. In the event that this is the last thread to go into
  waiting status, it is responsible for delivering a completed job and for handling
  :error and :broken states."
  [graph]
  (let [guard (await-signal (:signaller graph))]
    (when-not (or (.isInterrupted (Thread/currentThread))
                  (= :shutdown @(:status graph))
                  (= :die guard))
      (try
        (work-loop false graph guard)
        (let [active (swap! (:active-thread-count graph) dec)]
          (when (zero? active)
            (case @(:status graph)
              :working (possibly-finish graph guard)
              :error (handle-error-state graph)
              :cancelled (handle-cancelled-state graph)
              :broken (recreate-all-state graph))))
        (catch Exception e
          ((recoverable-error-fn graph) e nil)))
      (recur graph))))

(defn launch-threads
  "Contracted to kick off threads up to the graph's thread-count to work on the current
  job. In the current implementation, this works by sending signals using signal."
  [graph guard]
  (swap! (:active-thread-count graph) + (:n-threads graph))
  (signal (:signaller graph) @(:job-guard graph) (:n-threads graph)))
