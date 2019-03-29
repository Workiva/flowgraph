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

(ns flowgraph.protocols
  (:refer-clojure :exclude [pop peek]))

(def ^:dynamic *thread-capacity*
  (+ 2 (.. Runtime getRuntime availableProcessors)))

(defprotocol GuardedQueue
  (set-guard! [_ guard] "Sets the guard and empties the queue. Operations performed with any other guard will fail silently (what could possibly go wrong?).")
  (ready? [_] "Ready to spit out an item at least if forced.")
  (pop [_ guard] [_ guard force?] "Requests item from queue. If force? is true, this yields items the queue might otherwise wish to hold on to.")
  (push [_ guard item] "Pushes a regular value into the queue.")
  (push-deferred [_ guard deferred-item] "Pushes a deferred item into the queue.")
  (push-batch [_ guard batch] "Pushes a batch of regular values into the queue.")
  (push-deferred-batch [_ guard deferred-batch] "Pushes into the queue a deferred item that represents a batch of results.")
  (peek [_] "Peek at the next raw item in the underlying queue. Be careful -- this doesn't behave analogously to pop.")
  (drain [_ guard] "Returns a sequence of all the items from this queue.")
  (raw-size [_] "Returns the number of raw things in the queue...regardless of batching semantics"))

(defprotocol Flow
  (submit [this stuff] "Returns a tessera")
  (shutdown [this]))
