(defproject com.carey/weave "0.0.1"
  :description "a clojure library for performing mongo joins"
  :url "https://github.com/bluekezza/weave.git"
  :dependencies
  [[com.novemberain/monger "3.0.0"]
   [prismatic/schema "0.4.4"]
   [clojure-tools "1.1.3"]]
  :profiles
  {:test
   {:dependencies
    [[org.clojure/test.check "0.8.0"]
     [org.clojure/data.generators "0.1.2"]
     [org.clojure/test.generative "0.5.2"]]
    :resource-paths ["test/resources"]}})
