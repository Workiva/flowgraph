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

(ns flowgraph.graph-test
  (:require [clojure.test :refer :all]
            [flowgraph :refer :all])
  (:refer-clojure :exclude [merge]))

(def sleepy-factor 5)

(defn random-sleeping
  [f n]
  (fn [& args]
    (Thread/sleep (rand-int n))
    (apply f args)))

(defn bijected?
  [expected actual]
  (and (= (count expected) (count actual))
       (every? identity
               (map =
                    (sort expected)
                    (sort actual)))))

(defn order-preserved?
  [expected actual]
  (= (seq expected) (seq actual)))

(deflow direct-flow-test-graph [async? coordinated? sleep]
  {:source (transform (if sleep (random-sleeping inc sleep) inc) :middle
                      :asynchronous? async?
                      :coordinated? coordinated?)
   :middle (transform (if sleep (random-sleeping dec sleep) dec) :sink
                      :asynchronous? async?
                      :coordinated? coordinated?)})

(deftest simple-direct-flow-test
  (println "Running simple-direct-flow-test")
  (dotimes [i 60] (let [init (shuffle (range 1000))
                        graph (direct-flow-test-graph false false false)
                        actual @(submit graph init)]
                    (shutdown graph)
                    (is (bijected? init actual)))))

(deftest simple-direct-flow-recycled-test
  (println "Running simple-direct-flow-recycled-test")
  (let [graph (direct-flow-test-graph false false false)]
    (dotimes [i 60] (let [init (shuffle (range 1000))
                          actual @(submit graph init)]
                      (is (bijected? init actual))))
    (shutdown graph)))

(deftest ^:slow sleepy-direct-flow-test
  (println "Running sleepy-direct-flow-test")
  (dotimes [i 10] (let [init (shuffle (range 100))
                        graph (direct-flow-test-graph false false sleepy-factor)
                        actual @(submit graph init)]
                    (shutdown graph)
                    (is (bijected? init actual)))))

(deftest ^:slow sleepy-direct-flow-recycled-test
  (println "Running sleepy-direct-flow-recycled-test")
  (let [graph (direct-flow-test-graph false false sleepy-factor)]
    (dotimes [i 10] (let [init (shuffle (range 100))
                          actual @(submit graph init)]
                      (is (bijected? init actual))))
    (shutdown graph)))

(deftest ^:slow sleepy-async-flow-test
  (println "sleepy-async-flow-test")
  (dotimes [i 10] (let [init (shuffle (range 500))
                        graph (direct-flow-test-graph true false sleepy-factor)
                        actual @(submit graph init)]
                    (shutdown graph)
                    (is (bijected? init actual)))))

(deftest ^:slow sleepy-async-flow-recycled-test
  (println "sleepy-async-flow-recycled-test")
  (let [graph (direct-flow-test-graph true false sleepy-factor)]
    (dotimes [i 10] (let [init (shuffle (range 1000))
                          actual @(submit graph init)]
                      (is (bijected? init actual))))
    (shutdown graph)))

(deftest ^:slow sleepy-async-coordinated-flow-test
  (println "sleepy-async-coordinated-flow-test")
  (dotimes [i 10] (let [init (shuffle (range 1000))
                        graph (direct-flow-test-graph true true sleepy-factor)
                        actual @(submit graph init)]
                    (shutdown graph)
                    (is (bijected? init actual))
                    (is (order-preserved? init actual)))))

(deftest ^:slow sleepy-async-coordinated-flow-recycled-test
  (println "sleepy-async-coordinated-flow-recycled-test")
  (let [graph (direct-flow-test-graph true true sleepy-factor)]
    (dotimes [i 10] (let [init (shuffle (range 1000))
                          actual @(submit graph init)]
                      (is (bijected? init actual))
                      (is (order-preserved? init actual))))
    (shutdown graph)))

(deflow circular-flow-test-graph
  [async? coordinated? sleep]
  {:source (transform inc :middle
                      :priority 2
                      :coordinated? true)
   :middle (discriminate even? {true :circle-back false :decrement}
                         :coordinated? true)
   :decrement (transform dec :sink
                         :coordinated? true)
   :circle-back (transform (if sleep (random-sleeping identity sleep) identity) :source
                           :coordinated? coordinated?
                           :asynchronous? async?)})

(deftest simple-circular-flow-test
  (println "simple-circular-flow-test")
  (dotimes [i 60] (let [init (shuffle (range 1000))
                        expected (->> init
                                      (filter even?)
                                      ((juxt identity (partial map #(+ 2 %))))
                                      (apply concat))
                        graph (circular-flow-test-graph false false false)
                        actual @(submit graph init)]
                    (shutdown graph)
                    (is (bijected? expected actual)))))

(deftest simple-circular-flow-recycled-test
  (println "simple-circular-flow-recycled-test")
  (let [graph (circular-flow-test-graph false false false)]
    (dotimes [i 60] (let [init (shuffle (range 1000))
                          expected (->> init
                                        (filter even?)
                                        ((juxt identity (partial map #(+ 2 %))))
                                        (apply concat))
                          actual @(submit graph init)]
                      (is (bijected? expected actual))))
    (shutdown graph)))

(deftest sleepy-circular-flow-test
  (println "Running sleepy-circular-flow-test")
  (dotimes [i 10] (let [init (shuffle (range 200))
                        expected (->> init
                                      (filter even?)
                                      ((juxt identity (partial map #(+ 2 %))))
                                      (apply concat))
                        graph (circular-flow-test-graph false false sleepy-factor)
                        actual @(submit graph init)]
                    (shutdown graph)
                    (is (bijected? expected actual)))))

(deftest sleepy-circular-flow-recycled-test
  (println "Running sleepy-circular-flow-recycled-test")
  (let [graph (circular-flow-test-graph false false sleepy-factor)]
    (dotimes [i 10] (let [init (shuffle (range 200))
                          expected (->> init
                                        (filter even?)
                                        ((juxt identity (partial map #(+ 2 %))))
                                        (apply concat))
                          actual @(submit graph init)]
                      (is (bijected? expected actual))))
    (shutdown graph)))

(deftest sleepy-async-circular-flow-test
  (println "Running sleepy-async-circular-flow-test")
  (dotimes [i 20] (let [init (shuffle (range 1000))
                        expected (->> init
                                      (filter even?)
                                      ((juxt identity (partial map #(+ 2 %))))
                                      (apply concat))
                        graph (circular-flow-test-graph true false sleepy-factor)
                        actual @(submit graph init)]
                    (shutdown graph)
                    (is (bijected? expected actual)))))

(deftest sleepy-async-circular-flow-recycled-test
  (println "Running sleepy-async-circular-flow-recycled-test")
  (let [graph (circular-flow-test-graph true false sleepy-factor)]
    (dotimes [i 20] (let [init (shuffle (range 1000))
                          expected (->> init
                                        (filter even?)
                                        ((juxt identity (partial map #(+ 2 %))))
                                        (apply concat))
                          actual @(submit graph init)]
                      (is (bijected? expected actual))))
    (shutdown graph)))

(deftest sleepy-async-coordinated-circular-flow-test
  (println "Running sleepy-async-coordinated-circular-flow-test")
  (dotimes [i 20] (let [init (shuffle (range 1000))
                        expected-first (->> init
                                            (filter even?))
                        expected-second (->> init
                                             (filter odd?)
                                             (map inc))
                        expected (concat expected-first expected-second)
                        graph (circular-flow-test-graph true true sleepy-factor)
                        actual @(submit graph init)]
                    (shutdown graph)
                    (is (bijected? expected actual))
                    (is (order-preserved? expected actual)))))

(deftest sleepy-async-coordinated-circular-flow-recycled-test
  (println "Running sleepy-async-coordinated-circular-flow-recycled-test")
  (let [graph (circular-flow-test-graph true true sleepy-factor)]
    (dotimes [i 20] (let [init (shuffle (range 1000))
                          expected-first (->> init
                                              (filter even?))
                          expected-second (->> init
                                               (filter odd?)
                                               (map inc))
                          expected (concat expected-first expected-second)
                          actual @(submit graph init)]
                      (is (bijected? expected actual))
                      (is (order-preserved? expected actual))))
    (shutdown graph)))

(deflow duplicate-join-test-graph
  [coordinated? sleep]
  {:source (duplicate [:middle-left :middle-right]
                      :coordinated? coordinated?)
   :middle-left (transform (if sleep (random-sleeping identity sleep) identity) :bottom-left
                           :coordinated? true)
   :middle-right (transform identity :bottom-right
                            :coordinated? true)
   [:bottom-left :bottom-right] (fuse :sink)})

(deftest simple-duplicate-join-test
  (println "Running simple-duplicate-join-test")
  (dotimes [i 60] (let [init (shuffle (range 1000))
                        graph (duplicate-join-test-graph false false)
                        actual @(submit graph init)]
                    (shutdown graph)
                    (is (bijected? init (map first actual))))))

(deftest simple-duplicate-join-recycled-test
  (println "Running simple-duplicate-join-recycled-test")
  (let [graph (duplicate-join-test-graph false false)]
    (dotimes [i 60] (let [init (shuffle (range 1000))
                          actual @(submit graph init)]
                      (is (bijected? init (map first actual)))))
    (shutdown graph)))

(deftest coordinated-duplicate-join-test
  (println "Running coordinated-duplicate-join-test")
  (dotimes [i 60] (let [init (shuffle (range 1000))
                        graph (duplicate-join-test-graph true false)
                        actual @(submit graph init)]
                    (shutdown graph)
                    (is (bijected? init (map first actual)))
                    (is (order-preserved? init (map first actual)))
                    (is (every? #(= (first %) (second %)) actual)))))

(deftest coordinated-duplicate-join-recycled-test
  (println "Running coordinated-duplicate-join-recycled-test")
  (let [graph (duplicate-join-test-graph true false)]
    (dotimes [i 60] (let [init (shuffle (range 10000))
                          actual @(submit graph init)]
                      (is (bijected? init (map first actual)))
                      (is (order-preserved? init (map first actual)))
                      (is (every? #(= (first %) (second %)) actual))))
    (shutdown graph)))

(deftest ^:slow sleepy-coordinated-duplicate-join-test
  (println "Running sleepy-coordinated-duplicate-join-test")
  (dotimes [i 20] (let [init (shuffle (range 100 200))
                        graph (duplicate-join-test-graph true sleepy-factor)
                        actual @(submit graph init)]
                    (shutdown graph)
                    (is (bijected? init (map first actual)))
                    (is (order-preserved? init (map first actual)))
                    (is (every? #(= (first %) (second %)) actual)))))

(deftest ^:slow sleepy-coordinated-duplicate-join-recycled-test
  (println "Running sleepy-coordinated-duplicate-join-recycled-test")
  (let [graph (duplicate-join-test-graph true sleepy-factor)]
    (dotimes [i 20] (let [init (shuffle (range 100 200))
                          actual @(submit graph init)]
                      (is (bijected? init (map first actual)))
                      (is (order-preserved? init (map first actual)))
                      (is (every? #(= (first %) (second %)) actual))))
    (shutdown graph)))

(deflow duplicate-join-test-unroll-graph
  [asynchronous? sleepy?]
  {:source (duplicate [:middle-left :middle-right]
                      :coordinated? true)
   :middle-left (transform (if sleepy? (random-sleeping first sleepy?) first) :bottom-left
                           :batching? 1
                           :asynchronous? true
                           :coordinated? true)
   :middle-right (transform first :bottom-right
                            :batching? 1
                            :asynchronous? true
                            :coordinated? true)
   [:bottom-left :bottom-right] (fuse :sink)})

(deftest ^:slow coordinated-unrolling-graph
  (println "Running coordinated-unrolling-graph")
  (let [graph (duplicate-join-test-unroll-graph false false)]
    (dotimes [i 40] (let [init (partition 10 (range 10000))
                          actual @(submit graph init)]
                      (is (bijected? (flatten init)
                                     (map first actual)))
                      (is (order-preserved? (flatten init)
                                            (map first actual)))
                      (is (every? #(= (first %) (second %)) actual))))
    (shutdown graph)))

(deftest ^:slow sleepy-coordinated-unrolling-graph
  (println "Running sleepy-coordinated-unrolling-graph")
  (let [graph (duplicate-join-test-unroll-graph false sleepy-factor)]
    (dotimes [i 40] (let [init (partition 10 (range 10000))
                          actual @(submit graph init)]
                      (is (bijected? (flatten init)
                                     (map first actual)))
                      (is (order-preserved? (flatten init)
                                            (map first actual)))
                      (is (every? #(= (first %) (second %)) actual))))
    (shutdown graph)))

(deftest ^:slow coordinated-async-unrolling-graph
  (println "Running coordinated-async-unrolling-graph")
  (let [graph (duplicate-join-test-unroll-graph true false)]
    (dotimes [i 40] (let [init (partition 10 (range 10000))
                          actual @(submit graph init)]
                      (is (bijected? (flatten init)
                                     (map first actual)))
                      (is (order-preserved? (flatten init)
                                            (map first actual)))
                      (is (every? #(= (first %) (second %)) actual))))
    (shutdown graph)))

(deftest ^:slow sleepy-coordinated-async-unrolling-graph
  (println "Running sleepy-coordinated-async-unrolling-graph")
  (let [graph (duplicate-join-test-unroll-graph true sleepy-factor)]
    (dotimes [i 40] (let [init (partition 10 (range 10000))
                          actual @(submit graph init)]
                      (is (bijected? (flatten init)
                                     (map first actual)))
                      (is (order-preserved? (flatten init)
                                            (map first actual)))
                      (is (every? #(= (first %) (second %)) actual))))
    (shutdown graph)))

(deflow duplicate-join-test-unroll-collect-graph
  [asynchronous? sleepy? collect-fn]
  {:source (duplicate [:top-left :top-right]
                      :coordinated? true)
   :top-left (transform (if sleepy? (random-sleeping first sleepy?) first) :middle-left
                        :batching? 1
                        :asynchronous? true
                        :coordinated? true)
   :top-right (transform first :middle-right
                         :batching? 1
                         :asynchronous? true
                         :coordinated? true)
   :middle-left (transform identity :bottom-left
                           :collecting? collect-fn
                           :coordinated? true)
   :middle-right (transform identity :bottom-right
                            :collecting? collect-fn
                            :coordinated? true)
   [:bottom-left :bottom-right] (fuse :sink)})

(deftest ^:slow coordinated-unrolling-collecting-graph
  (println "Running coordinated-unrolling-collecting-graph")
  (let [graph (duplicate-join-test-unroll-collect-graph false false (fn [a b] (= 0 (mod a 10))))]
    (dotimes [i 40] (let [init (partition 10 (range 2000))
                          actual @(submit graph init)]
                      (is (order-preserved? init (map first actual)))
                      (is (every? #(= (first %) (second %)) actual))))
    (shutdown graph)))

(deftest ^:slow sleepy-coordinated-unrolling-collecting-graph
  (println "Running sleepy-coordinated-unrolling-collecting-graph")
  (let [graph (duplicate-join-test-unroll-collect-graph false
                                                        sleepy-factor
                                                        (fn [a b] (= 0 (mod a 10))))]
    (dotimes [i 40] (let [init (partition 10 (range 2000))
                          actual @(submit graph init)]
                      (is (order-preserved? init (map first actual)))
                      (is (every? #(= (first %) (second %)) actual))))
    (shutdown graph)))

(deftest ^:slow coordinated-async-unrolling-collecting-graph
  (println "Running coordinated-async-unrolling-collecting-graph")
  (let [graph (duplicate-join-test-unroll-collect-graph true false (fn [a b] (= 0 (mod a 10))))]
    (dotimes [i 40] (let [init (partition 10 (range 2000))
                          actual @(submit graph init)]
                      (is (order-preserved? init (map first actual)))
                      (is (every? #(= (first %) (second %)) actual))))
    (shutdown graph)))

(deftest ^:slow sleepy-coordinated-async-unrolling-collecting-graph
  (println "Running sleepy-coordinated-async-unrolling-collecting-graph")
  (let [graph (duplicate-join-test-unroll-collect-graph true
                                                        sleepy-factor
                                                        (fn [a b] (= 0 (mod a 10))))]
    (dotimes [i 40] (let [init (partition 10 (range 2000))
                          actual @(submit graph init)]
                      (is (order-preserved? init (map first actual)))
                      (is (every? #(= (first %) (second %)) actual))))
    (shutdown graph)))

(deflow discriminate-commix-test-graph
  [coordinated?]
  {:source (discriminate even? {true :evens false :odds}
                         :coordinated? coordinated?)
   [:evens :odds] (commix :sink :coordinated? coordinated?)})

(deftest simple-discriminate-commix-test
  (println "Running simple-discriminate-commix-test")
  (dotimes [i 60] (let [init (shuffle (range 1000))
                        graph (discriminate-commix-test-graph false)
                        actual @(submit graph init)]
                    (shutdown graph)
                    (is (bijected? init actual)))))

(deftest simple-discriminate-commix-recycled-test
  (println "Running simple-discriminate-commix-recycled-test")
  (let [graph (discriminate-commix-test-graph false)]
    (dotimes [i 60] (let [init (shuffle (range 1000))
                          actual @(submit graph init)]
                      (is (bijected? init actual))))
    (shutdown graph)))

(deftest coordinated-discriminate-commix-test
  (println "Running coordinated-discriminate-commix-test")
  (dotimes [i 60] (let [init (range 1000)
                        graph (discriminate-commix-test-graph true)
                        actual @(submit graph init)]
                    (shutdown graph)
                    (is (bijected? init actual))
                    (is (every? identity
                                (map (comp #(apply < %) val)
                                     (group-by even? actual)))))))

(deftest coordinated-discriminate-commix-recycled-test
  (println "Running coordinated-discriminate-commix-recycled-test")
  (let [graph (discriminate-commix-test-graph true)]
    (dotimes [i 60] (let [init (range 1000)
                          actual @(submit graph init)]
                      (is (bijected? init actual))
                      (is (every? identity
                                  (map (comp #(apply < %) val)
                                       (group-by even? actual))))))
    (shutdown graph)))

(deflow batch-test-graph
  [coordinated? async? sleep]
  {:source (transform (if sleep (random-sleeping identity sleep) identity) :sink
                      :coordinated? coordinated?
                      :batching? 3
                      :asynchronous? async?)})

(deftest simple-batch-test
  (println "Running simple-batch-test")
  (dotimes [i 60] (let [init (shuffle (range 1000))
                        graph (batch-test-graph false false nil)
                        actual @(submit graph init)]
                    (shutdown graph)
                    (is (bijected? init actual)))))

(deftest simple-batch-recycled-test
  (println "Running simple-batch-recycled-test")
  (let [graph (batch-test-graph false false nil)]
    (dotimes [i 60] (let [init (shuffle (range 1000))
                          actual @(submit graph init)]
                      (is (bijected? init actual))))
    (shutdown graph)))

(deftest ^:slow simple-sleepy-batch-test
  (println "Running simple-sleepy-batch-test")
  (dotimes [i 60] (let [init (shuffle (range 250))
                        graph (batch-test-graph false false sleepy-factor)
                        actual @(submit graph init)]
                    (shutdown graph)
                    (is (bijected? init actual)))))

(deftest ^:slow simple-sleepy-batch-recycled-test
  (println "Running simple-sleepy-batch-recycled-test")
  (let [graph (batch-test-graph false false sleepy-factor)]
    (dotimes [i 60] (let [init (shuffle (range 250))
                          actual @(submit graph init)]
                      (is (bijected? init actual))))
    (shutdown graph)))

(deftest coordinated-batch-test
  (println "Running coordinated-batch-test")
  (dotimes [i 60] (let [init (shuffle (range 1000))
                        graph (batch-test-graph true false nil)
                        actual @(submit graph init)]
                    (shutdown graph)
                    (is (= (seq init) (seq actual))))))

(deftest coordinated-batch-recycled-test
  (println "Running coordinated-batch-recycled-test")
  (let [graph (batch-test-graph true false nil)]
    (dotimes [i 60] (let [init (shuffle (range 1000))
                          actual @(submit graph init)]
                      (is (= (seq init) (seq actual)))))
    (shutdown graph)))

(deftest coordinated-sleepy-batch-test
  (println "Running coordinated-sleepy-batch-test")
  (dotimes [i 60] (let [init (shuffle (range 250))
                        graph (batch-test-graph true false sleepy-factor)
                        actual @(submit graph init)]
                    (shutdown graph)
                    (is (= (seq init) (seq actual))))))

(deftest coordinated-sleepy-batch-recycled-test
  (println "Running coordinated-sleepy-batch-recycled-test")
  (let [graph (batch-test-graph true false sleepy-factor)]
    (dotimes [i 60] (let [init (shuffle (range 250))
                          actual @(submit graph init)]
                      (is (= (seq init) (seq actual)))))
    (shutdown graph)))

(deftest simple-async-batch-test
  (println "Running simple-async-batch-test")
  (dotimes [i 60] (let [init (shuffle (range 1000))
                        graph (batch-test-graph false true nil)
                        actual @(submit graph init)]
                    (shutdown graph)
                    (is (bijected? init actual)))))

(deftest simple-async-batch-recycled-test
  (println "Running simple-async-batch-recycled-test")
  (let [graph (batch-test-graph false true nil)]
    (dotimes [i 60] (let [init (shuffle (range 1000))
                          actual @(submit graph init)]
                      (is (bijected? init actual))))
    (shutdown graph)))

(deftest simple-sleepy-async-batch-test
  (println "Running simple-sleepy-async-batch-test")
  (dotimes [i 60] (let [init (shuffle (range 1000))
                        graph (batch-test-graph false true sleepy-factor)
                        actual @(submit graph init)]
                    (shutdown graph)
                    (is (bijected? init actual)))))

(deftest simple-sleepy-async-batch-recycled-test
  (println "Running simple-sleepy-async-batch-recycled-test")
  (let [graph (batch-test-graph false true sleepy-factor)]
    (dotimes [i 60] (let [init (shuffle (range 1000))
                          actual @(submit graph init)]
                      (is (bijected? init actual))))
    (shutdown graph)))

(deftest coordinated-async-batch-test
  (println "Running coordinated-async-batch-test")
  (dotimes [i 60] (let [init (shuffle (range 1000))
                        graph (batch-test-graph true true nil)
                        actual @(submit graph init)]
                    (shutdown graph)
                    (is (= (seq init) (seq actual))))))

(deftest coordinated-async-batch-recycled-test
  (println "Running coordinated-async-batch-recycled-test")
  (let [graph (batch-test-graph true true nil)]
    (dotimes [i 60] (let [init (shuffle (range 1000))
                          actual @(submit graph init)]
                      (is (= (seq init) (seq actual)))))
    (shutdown graph)))

(deftest coordinated-sleepy-async-batch-test
  (println "Running coordinated-sleepy-async-batch-test")
  (dotimes [i 60] (let [init (shuffle (range 1000))
                        graph (batch-test-graph true true sleepy-factor)
                        actual @(submit graph init)]
                    (shutdown graph)
                    (is (= (seq init) (seq actual))))))

(deftest coordinated-sleepy-async-batch-recycled-test
  (println "Running coordinated-sleepy-async-batch-recycled-test")
  (let [graph (batch-test-graph true true sleepy-factor)]
    (dotimes [i 60] (let [init (shuffle (range 1000))
                          actual @(submit graph init)]
                      (is (= (seq init) (seq actual)))))
    (shutdown graph)))

(deflow error-test-graph
  [async?]
  {:source (transform (fn [_] (throw (ex-info "I like to take exception." {:desired-exception true})))
                      :sink
                      :asynchronous? async?)})

(deftest asynchronous-exception-handling
  (println "Running asynchronous-exception-handling")
  (let [graph (error-test-graph true)]
    (try @(submit graph [1])
         (catch Exception e
           (is (= "I like to take exception." (.getMessage (.getCause e))))))))

(deftest synchronous-exception-handling
  (println "Running synchronous-exception-handling")
  (let [graph (error-test-graph false)]
    (try @(submit graph [1])
         (catch Exception e
           (is (= "I like to take exception." (.getMessage (.getCause e))))))))

(deflow timeout-test-graph
  []
  {:source (transform (fn [x] (Thread/sleep 2000) (inc x)) :sink
                      :asynchronous? true
                      :timeout-ms 1000)})

(deftest timeout-exception-handling
  (println "Running timeout-exception-handling")
  (let [graph (timeout-test-graph)]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"A timeout occurred in the edge.*in the graph"
                          @(submit graph [1])))))

(comment
  ;; TODO: NOTE:
  ;;  The following tests are isolated to the forthcoming 'stack' option
  ;;  for decursus nodes.
  (deflow duplicate-join-test-graph-stack
    [coordinated? sleep]
    {:source (duplicate [:middle-left :middle-right]
                        :coordinated? coordinated?)
     :middle-left (transform (if sleep (random-sleeping identity sleep) identity) :bottom-left
                             :coordinated? true)
     :middle-right (transform identity :bottom-right
                              :coordinated? true)
     [:bottom-left :bottom-right] (fuse :sink)}
    {:source {:stack? true}
     :middle-left {:stack? true}
     :middle-right {:stack? true}
     :bottom-left {:stack? true}
     :bottom-right {:stack? true}})

  (deftest simple-duplicate-stacked-join-test
    (dotimes [i 60] (let [init (shuffle (range 1000))
                          graph (duplicate-join-test-graph-stack false false)
                          actual @(submit graph init)]
                      (shutdown graph)
                      (is (bijected? init (map first actual))))))

  (deflow stack-node-graph
    []
    {:source (transform identity :sink :coordinated? true)}
    {:source {:stack? true}})

  (deftest stack-node-test
    (let [graph (stack-node-graph)]
      (is (= (reverse (range 100))
             @(submit graph (range 100))))))

  (defn batcher [x] x)

  (deflow stack-loop-graph
    []
    {:source (transform batcher :middle
                        :coordinated? true
                        :batching? 1)
     :middle (transform batcher :sink
                        :coordinated? true
                        :batching? 1)}
    {:middle {:stack? true}
     :source {:stack? true}})

  (deftest stack-loop-test
    (let [graph (stack-loop-graph)]
      (println @(submit graph (range 100))))))
