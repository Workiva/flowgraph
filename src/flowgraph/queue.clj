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

(ns flowgraph.queue
  (:require [flowgraph.protocols :as p :refer :all]
            [flowgraph.error :refer [raise-queue-err]]
            [recide.core :refer [insist]]
            [recide.sanex :as sanex]
            [clojure.pprint :as pprint])
  (:import [java.util.concurrent
            BlockingQueue LinkedBlockingQueue PriorityBlockingQueue
            BlockingDeque LinkedBlockingDeque])
  (:refer-clojure :exclude [pop peek]))

(defn- slurp-juc-queue
  "empties the queue, does not block on asynchronous work, returns all items."
  [^java.util.concurrent.BlockingQueue juc-queue]
  (let [arr-results (.toArray juc-queue)]
    (.clear juc-queue)
    (seq arr-results)))

(defrecord SimpleQueue [^BlockingQueue juc-queue guard fns]
  p/GuardedQueue
  (set-guard! [_ g]
    (reset! guard g)
    (.clear juc-queue))
  (ready? [_] (not (.isEmpty juc-queue)))
  (pop [this g force?] (pop this g))
  (pop [_ g] (if (identical? g @guard) ((:poll fns) juc-queue)
                 (raise-queue-err :access "guarded queue presented with wrong token."
                                  {:method 'pop, :token g, :expected @guard, ::sanex/sanitary? true})))
  (push [_ g item] (if (identical? g @guard) ((:put fns) juc-queue item)
                       (raise-queue-err :access "guarded queue presented with wrong token."
                                        {:method 'push, :token g, :expected @guard, ::sanex/sanitary? true})))
  (push-deferred [_ g deferred-item] (raise-queue-err :unsupported "SimpleQueues don't support push-deferred."
                                                      {:method 'push-deferred, :class SimpleQueue, ::sanex/sanitary? true}))
  (push-batch [_ g batch] (if (identical? g @guard) (doseq [item batch] ((:put fns) juc-queue item))
                              (raise-queue-err :access "guarded queue presented with wrong token."
                                               {:method 'push-batch, :token g, :expected @guard, ::sanex/sanitary? true})))
  (push-deferred-batch [_ g deferred-item]
    (raise-queue-err :unsupported "SimpleQueues don't support push-deferred-batch."
                     {:method 'push-deferred-batch, :class SimpleQueue, ::sanex/sanitary? true}))
  (peek [_] ((:peek fns) juc-queue))
  (drain [_ g]
    (when (identical? g @guard)
      (slurp-juc-queue juc-queue)))
  (raw-size [_] (.size juc-queue)))

(defn ->queue-fns [stack?]
  (if stack?
    {:poll (fn poll-last [^BlockingDeque q] (.pollLast q))
     :put  (fn put-last [^BlockingDeque q x] (.putLast q x))
     :peek (fn peek-last [^BlockingDeque q] (.peekLast q))}

    {:poll (fn poll [^BlockingQueue q] (.poll q))
     :put  (fn put  [^BlockingQueue q x] (.put q x))
     :peek (fn peek [^BlockingQueue q] (.peek q))}))

(defn- simple-queue
  "For when the queue will hold tasks."
  [juc-queue stack?]
  (let [guard (atom 0)]
    (->SimpleQueue juc-queue guard (->queue-fns stack?))))

(defmethod clojure.pprint/simple-dispatch SimpleQueue
  [queue]
  (clojure.pprint/pprint-logical-block :prefix "#SimpleQueue"
                                       :suffix ""
                                       (clojure.pprint/write-out (-> queue
                                                                     (assoc :empty? (.isEmpty ^BlockingQueue (:juc-queue queue)))
                                                                     (dissoc :juc-queue)))
                                       ""))

(defmethod print-method SimpleQueue
  [queue ^java.io.Writer w]
  (.write w (str "#SimpleQueue"
                 (-> queue
                     (assoc :empty? (.isEmpty ^BlockingQueue (:juc-queue queue)))
                     (dissoc :juc-queue)))))

(defrecord DefermentQueue [^BlockingQueue juc-queue current-batch guard fns]
  GuardedQueue
  (set-guard! [_ g]
    (reset! guard g)
    (.clear juc-queue)
    (reset! current-batch []))
  (ready? [this] (locking this
                   (or (not-empty @current-batch)
                       (when-let [item ((:peek fns) juc-queue)]
                         (or (= ::real (first item))
                             (realized? ^clojure.lang.IDeref (second item)))))))
  (pop [this g force?] (pop this g))
  (pop [this g]
    (if (identical? g @guard)
      (when-let [[type item] (locking this
                               (if (empty? @current-batch)
                                 (when (ready? this)
                                   (let [[type item :as result] ((:poll fns) juc-queue)]
                                     (if (= ::deferred-batch type)
                                       (let [[item & items] @item]
                                         (reset! current-batch items)
                                         [::real item])
                                       result)))
                                 (let [item (first @current-batch)]
                                   (swap! current-batch rest)
                                   [::real item])))]
        (condp = type
          ::real item
          ::deferred-item @item))
      (raise-queue-err :access "guarded queue presented with wrong token"
                       {:method 'pop, :token g, :expected @guard, ::sanex/sanitary? true})))
  (push [_ g item] (if (identical? g @guard) ((:put fns) juc-queue [::real item])
                       (raise-queue-err :access "guarded queue presented with wrong token"
                                        {:method 'push, :token g, :expected @guard, ::sanex/sanitary? true})))
  (push-deferred [_ g deferred-item] (if (identical? g @guard)
                                       ((:put fns) juc-queue [::deferred-item deferred-item])
                                       (raise-queue-err :access "guarded queue presented with wrong token"
                                                        {:method 'push-deferred, :token g, :expected @guard, ::sanex/sanitary? true})))
  (push-batch [_ g batch] (if (identical? g @guard)
                            (doseq [item batch] ((:put fns) juc-queue [::real item]))
                            (raise-queue-err :access "guarded queue presented with wrong token"
                                             {:method 'push-batch, :token g, :expected @guard, ::sanex/sanitary? true})))
  (push-deferred-batch [_ g deferred-item] (if (identical? g @guard)
                                             ((:put fns) juc-queue [::deferred-batch deferred-item])
                                             (raise-queue-err :access "guarded queue presented with wrong token"
                                                              {:method 'push-deferred-batch, :token g, :expected @guard, ::sanex/sanitary? true})))
  (peek [_] ((:peek fns) juc-queue))
  (drain [this g]
    (when (identical? g @guard)
      (let [raw (concat (map (fn [i] [::real i]) @current-batch) (slurp-juc-queue juc-queue))]
        (reset! current-batch [])
        (reduce (fn [c [type item]]
                  (case type
                    ::real (conj c item)
                    ::deferred-batch (into c @item)
                    ::deferred-item (conj c @item)))
                []
                raw))))
  (raw-size [_] (+ (count @current-batch) (.size juc-queue))))

(defmethod clojure.pprint/simple-dispatch DefermentQueue
  [queue]
  (clojure.pprint/pprint-logical-block :prefix "#DefermentQueue"
                                       :suffix ""
                                       (clojure.pprint/write-out (-> queue
                                                                     (assoc :empty? (.isEmpty ^BlockingQueue (:juc-queue queue)))
                                                                     (dissoc queue :juc-queue)))
                                       ""))
(defmethod print-method DefermentQueue
  [^BlockingQueue queue ^java.io.Writer w]
  (.write w (str "#DefermentQueue"
                 (-> queue
                     (assoc :empty? (.isEmpty ^BlockingQueue (:juc-queue queue)))
                     (dissoc queue :juc-queue)))))

(defn- deferment-queue
  "For when the queue will hold promises -- if the computation is asynchronous,
  or if the edge into this queue is both coordinated and involves some computation."
  [juc-queue stack?]
  (->DefermentQueue juc-queue (atom ()) (atom 0) (->queue-fns stack?)))

(defrecord BatchedQueue
           [task-queue n todos guard]
  GuardedQueue
  (set-guard! [_ g]
    (reset! guard g)
    (reset! todos [])
    (set-guard! task-queue g))
  (ready? [_]
    (or (boolean (seq @todos))
        (ready? task-queue)))
  (pop [this g force?]
    (if force?
      (when (identical? g @guard)
        (locking this
          (let [x (pop task-queue g true)
                v @todos]
            (reset! todos [])
            (if x
              (conj v x)
              (when (not-empty v) v)))))
      (pop this g)))
  (pop [this g]
    (when (identical? g @guard)
      (when-let [x (pop task-queue g)]
        (or (locking this
              (let [v (swap! todos conj x)]
                (when (= n (count v))
                  (reset! todos [])
                  v)))
            (pop this g)))))
  (push [_ g v] (when (identical? g @guard) (push task-queue g v)))
  (push-deferred [_ g v] (when (identical? g @guard) (push-deferred task-queue g v)))
  (push-batch [_ g v] (when (identical? g @guard) (push-batch task-queue g v)))
  (push-deferred-batch [_ g v] (when (identical? g @guard) (push-deferred-batch task-queue g v)))
  (peek [_] (peek task-queue))
  (drain [_ g] (drain task-queue g))
  ;; I'm not sure what raw-size should mean here.
  (raw-size [_] (+ (min 1 (count @todos)) (raw-size task-queue))))

(defn- batch-a-queue
  "For when the queue wants to stick stuff out in a batch."
  [task-queue n]
  (let [todos (atom [])]
    (->BatchedQueue task-queue n todos (atom 0))))

(defrecord CollectingQueue ;; batches 'until' (f last-received-item next-received-item) returns true.
           [task-queue f semantics todos guard allow-force?]
  GuardedQueue
  (set-guard! [_ g]
    (reset! guard g)
    (dosync (ref-set todos []))
    (set-guard! task-queue g))
  (ready? [_]
    (or (boolean (seq @todos)) 1
        (ready? task-queue)))
  (pop [this g force?]
    (when (identical? g @guard)
      (if (and force? allow-force?)
        ;; if forcing, we act as normal except if there is no next item to apply the until-condition to, we assume it would have been met.
        (if-let [x (pop task-queue g)]
          (dosync
           (if (and (not-empty (ensure todos))
                    (f x (clojure.core/peek (ensure todos)))) ;; TODO: ensure in current iteration not really necessary
             (if (= semantics :exclusive)
               (let [v (ensure todos)]
                 (ref-set todos [x])
                 v)
               (let [v (ensure todos)]
                 (ref-set todos [])
                 (conj v x)))
             (do (alter todos conj x)
                 (pop this g true))))
          (dosync (let [v (ensure todos)]
                    (when-not (empty? v)
                      (ref-set todos [])
                      v))))
        (when-let [x (pop task-queue g)]
          (dosync
           (if (and (not-empty (ensure todos))
                    (f x (clojure.core/peek (ensure todos)))) ;; TODO: ensure in current iteration not really necessary
             (if (= semantics :exclusive)
               (let [v (ensure todos)]
                 (ref-set todos [x])
                 v)
               (let [v (ensure todos)]
                 (ref-set todos [])
                 (conj v x)))
             (do (alter todos conj x)
                 (pop this g))))))))
  (pop [this g] (pop this g false))
  (push [_ g v] (push task-queue g v))
  (push-deferred [_ g v] (push-deferred task-queue g v))
  (push-batch [_ g v] (push-batch task-queue g v))
  (push-deferred-batch [_ g v] (push-deferred-batch task-queue g v))
  (peek [_] (peek task-queue))
  (drain [_ g] (drain task-queue g))
  ;; I'm not sure what raw-size means here.
  (raw-size [_] (+ (raw-size task-queue) (count @todos))))

(defn- collecting-queue
  [queue f semantics allow-force?]
  (let [todos (ref [])]
    (->CollectingQueue queue f semantics todos (atom 0) allow-force?)))

;; If the underlying queue is to be a priority queue, then we create a
;; PriorityBlockingQueue, else a LinkedBlockingQueue.
;;
;; If the queue is to receive promises or futures, we wrap with
;; promise-queue, else with simple-queue.
;;
;; When the queue is to be batched, we wrap that with batch-a-queue.

(defn queue
  [{deferred? :deferred?,              ;; false or true
    batching? :batching?,               ;; false or n
    collecting? :collecting?                ;; false or n or fn
    priority-queue? :priority-queue?, ;; false or Comparator
    collect-inclusive? :collect-inclusive? ;; false or true
    allow-force? :allow-force? ;; true or false
    stack? :stack?
    :or {deferred? false,
         batching? false,
         collecting? false,
         priority-queue? false,
         stack? false,
         collect-inclusive? false
         allow-force? true}}]
  (insist (not (and collecting? batching?))) ;; can't have both
  (insist (not (and priority-queue? stack?))) ;; ditto
  (as-> (cond (boolean priority-queue?) (PriorityBlockingQueue. *thread-capacity*
                                                                priority-queue?)

              (boolean stack?) (do
                                 (throw (IllegalArgumentException. "Stacks are not yet surfaced in decursus."))
                                 (LinkedBlockingDeque.))
              :else (LinkedBlockingQueue.))
        queue
    (if deferred? (deferment-queue queue stack?) (simple-queue queue stack?))
    (if batching? (batch-a-queue queue batching?) queue)
    (if (and collecting? (number? collecting?)) (batch-a-queue queue collecting?) queue)
    (if (and collecting? (instance? clojure.lang.IFn collecting?))
      (if collect-inclusive?
        (collecting-queue queue collecting? :inclusive allow-force?)
        (collecting-queue queue collecting? :exclusive allow-force?))
      queue)))
