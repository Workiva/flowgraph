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

(ns flowgraph.signal
  (:require [flowgraph.queue :as q]
            [flowgraph.protocols :as p])
  (:import [java.util.concurrent LinkedBlockingQueue]))

(defprotocol Signaller
  (await-signal [signaller] "Waits for the signaller to signal. Returns the sign signaled.")
  (signal [signaller sign n] "Signals sign n times."))

(extend-protocol Signaller
  clojure.lang.IDeref
  (await-signal [o] (deref o))
  LinkedBlockingQueue
  (await-signal [lbq] (.take lbq))
  (signal [lbq sign n] (.addAll lbq (repeat n sign))))

(defn simple-signaller [] (LinkedBlockingQueue.))
