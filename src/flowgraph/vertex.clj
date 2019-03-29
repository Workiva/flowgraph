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

(ns flowgraph.vertex
  (:require [flowgraph.queue :as queue]))

(def default-vertex-spec
  {:deferred? false,
   :batching? false,
   :collecting? false})

(defn- transform-spec->vertex-spec ;; TODO: misnomer
  [vertices edge]
  (as-> vertices vertices
    (update vertices (:source-name edge) (fnil identity default-vertex-spec))
    (update vertices (:dest-name edge) (fnil identity default-vertex-spec))
    (if-let [batch (:batching? edge)]
      (assoc-in vertices [(:source-name edge)
                          :batching?]
                batch)
      vertices)
    (if-let [collect (:collecting? edge)]
      (-> vertices
          (assoc-in [(:source-name edge) :collecting?] collect)
          (assoc-in [(:source-name edge) :collect-inclusive?]
                    (boolean (:collect-inclusive? edge)))
          (assoc-in [(:source-name edge) :allow-force?]
                    (if (:collect-strict? edge)
                      false
                      true)))
      vertices)
    (if (:coordinated? edge)
      (assoc-in vertices [(:dest-name edge)
                          :deferred?]
                true)
      vertices)))

(defn- duplicate-spec->vertex-spec
  [vertices edge]
  (as-> vertices vertices
    (update vertices (:source-name edge) (fnil identity default-vertex-spec))
    (reduce #(update % %2 (fnil identity default-vertex-spec)) vertices (:dest-names edge))))

(defn- discriminate-spec->vertex-spec
  [vertices edge]
  (as-> vertices vertices
    (update vertices (:source-name edge) (fnil identity default-vertex-spec))
    (reduce #(update % %2 (fnil identity default-vertex-spec)) vertices (vals (:result->destination edge)))))

(defn- commix-spec->vertex-spec
  [vertices edge]
  (as-> vertices vertices
    (update vertices (:dest-name edge) (fnil identity default-vertex-spec))
    (reduce #(update % %2 (fnil identity default-vertex-spec)) vertices (:source-names edge))))

(defn- fuse-spec->vertex-spec
  [vertices edge]
  (as-> vertices vertices
    (reduce #(update % %2 (fnil identity default-vertex-spec)) vertices (:source-names edge))
    (update vertices (:dest-name edge) (fnil identity default-vertex-spec))))

(defn- edge-spec->vertex-spec ;; TODO: misnomer
  [vertices edge]
  (case (:type edge)
    :transform (transform-spec->vertex-spec vertices edge)
    :duplicate (duplicate-spec->vertex-spec vertices edge)
    :discriminate (discriminate-spec->vertex-spec vertices edge)
    :commix (commix-spec->vertex-spec vertices edge)
    :fuse (fuse-spec->vertex-spec vertices edge)))

(defn vertex-specs
  [v-specs e-specs]
  (as-> {:source default-vertex-spec
         :sink default-vertex-spec} vertices
    (merge-with into vertices v-specs)
    (reduce edge-spec->vertex-spec vertices e-specs)))

(defn vertex-gen
  [[name spec]]
  (fn [] (assoc (queue/queue spec) :name name)))
