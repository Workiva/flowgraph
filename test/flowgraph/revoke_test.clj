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

(ns flowgraph.revoke-test
  (:require [clojure.test :refer :all]
            [flowgraph :refer :all]
            [tesserae.core :as tess])
  (:refer-clojure :exclude [merge]))

(deflow infinitely-looping-graph []
  {:source (transform inc :middle)
   :middle (transform dec :end)
   :end (discriminate zero? {true :source false :sink})})

(deftest test:flowgraph-respects-cancels
  (let [graph (infinitely-looping-graph)
        tessera (submit graph [0])
        v (deref tessera 200 ::timed-out)]
    (is (= ::timed-out v))
    (tess/revoke tessera)
    (is (= 1 (first @(submit graph [1]))))))
