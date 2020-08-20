(ns app
  (:require [ring.middleware.reload :refer [wrap-reload]]
            [ring.util.response :refer [response status]]
            [compojure.core :refer [GET POST defroutes]]
            [clojure.java.jdbc :as jdbc]
            [cheshire.core :as json :refer [parse-string]]
            [clojure.pprint :refer [pprint]]
            [ring.adapter.jetty :refer [run-jetty]]
            [ring.middleware.json :refer [wrap-json-response wrap-json-params]]))


(def db-spec  {:classname   "org.sqlite.JDBC"
               :subprotocol "sqlite"
               :subname     "../database.db"})

(defn messages
  "Return all messages"
  []
  (->> (jdbc/query db-spec ["SELECT * FROM messages"])
       (map :body)))

(defn search
  "Search for answers!"
  [query]
  (jdbc/query db-spec ["SELECT * FROM answers where title like ?" (str \% query \%)]))

(defroutes app-routes
  (GET "/messages" []
       (response (messages)))
  (POST "/search" [query]
        (-> (response (search query)) (status 200))))

(defn app []
  (-> #'app-routes
      (wrap-json-response)
      (wrap-json-params)
      (wrap-reload {:dirs "."})))

(defn -main [& args]
  (run-jetty (app) {:port 5000 :join? false?}))
