(defproject com.workiva/flowgraph "0.1.0"
  :description "A Clojure library for fast, concurrent, asynchronous data processing using directed graphs."
  :url "https://github.com/Workiva/flowgraph"
  :license {:name "Apache License, Version 2.0"}
  :plugins [[lein-cljfmt "0.6.4"]
            [lein-shell "0.5.0"]
            [lein-codox "0.10.3"]]
  :dependencies [[backtick "0.3.4"]
                 [com.stuartsierra/component "0.3.2"]
                 [com.workiva/recide "1.0.0"]
                 [com.workiva/tesserae "1.0.0"]
                 [com.workiva/utiliva "0.1.0"]
                 [manifold "0.1.6"]
                 [org.clojure/clojure "1.9.0-alpha17"]
                 [org.clojure/tools.macro "0.1.5"]
                 [potemkin "0.4.3"]]

  :deploy-repositories {"clojars"
                        {:url "https://repo.clojars.org"
                         :sign-releases false}}

  :source-paths      ["src"]
  :test-paths        ["test"]

  :global-vars {*warn-on-reflection* true}

  :aliases {"docs" ["do" "clean-docs," "with-profile" "docs" "codox"]
            "clean-docs" ["shell" "rm" "-rf" "./documentation"]}

  :codox {:metadata {:doc/format :markdown}
          :themes [:rdash]
          :output-path "documentation"}

  :profiles {:dev [{:dependencies [[criterium "0.4.3"]]}]
             :docs {:dependencies [[codox-theme-rdash "0.1.2"]]}})
