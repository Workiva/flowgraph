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

(ns flowgraph.utils
  (:require [clojure.walk]))

(defn lock-form
  "Produces (locking ~locking-sym ~form), except with some optimizations in the case of when-lets
  to minimize locking extent."
  [locking-sym coordination-syms [head & [bindings & body] :as form]]
  (if (and (contains? `#{when-let} head)
           (vector? bindings)
           (some (partial contains? coordination-syms) (flatten bindings)))
    (let [[quarantine free] (->> body
                                 (reverse)
                                 (split-with (-> (partial some (partial contains? coordination-syms))
                                                 complement
                                                 (comp flatten)))
                                 (map reverse)
                                 (reverse))]
      (if (empty? free)
        `(locking ~locking-sym ~form)
        (let [[sym binding] bindings
              new-sym (gensym sym)
              new-bindings [sym `(locking ~locking-sym
                                   (when-let [~new-sym ~(clojure.walk/postwalk-replace {sym new-sym}
                                                                                       binding)]
                                     ~@(clojure.walk/postwalk-replace {sym new-sym}
                                                                      quarantine)
                                     ~new-sym))]]
          `(~head ~new-bindings ~@free))))
    `(locking ~locking-sym ~form)))

(defn locking-sym-on-coordination
  "Inefficient technique, but easy (?) to understand. Give it a symbol to use
  for synchronization/locking, a set of symbols whose occurrence must occur within
  the synchronized/locked block, and a form. It will return the same form with a
  minimally-intrusive locking form added. Performs a small re-writing trick in the
  case of when-lets."
  [locking-sym coordination-syms form]
  (let [coordination-count (count (filter coordination-syms (flatten form)))
        f (volatile! true)]
    (if (zero? coordination-count)
      form
      (clojure.walk/postwalk (fn [form] (if (and @f (seq? form))
                                          (let [c (count (filter coordination-syms (flatten form)))]
                                            (if (= c coordination-count)
                                              (do (vreset! f false) (lock-form locking-sym coordination-syms form))
                                              form))
                                          form))
                             form))))
