(ns ribelo.symplex.warehouse
  (:require
   [clojure.string :as str]
   [taoensso.encore :as e]
   [hanse.danzig :as dz :refer [=>>]]
   [hanse.danzig.io :as dz.io]))

(defn- vat-parse [s]
  (if-let [vat (-> (re-find #"\d+" s) e/as-?float)]
    (/ vat 100.0)
    0.0))

(defn read-file [file-path]
  (=>> (dz.io/read-csv file-path
                       {:sep    ";"
                        :header {0 [:cg.warehouse.product/name [:string str/lower-case]]
                                 1 [:cg.warehouse.product/ean :string]
                                 2 [:cg.warehouse.product/stock :double]
                                 3 [:cg.warehouse.product/purchase-net-price :double]
                                 4 [:cg.warehouse.product/sell-net-price-1 :double]
                                 5 [:cg.warehouse.product/sell-net-price-2 :double]
                                 6 [:cg.warehouse.product/vat vat-parse]
                                 7 :cg.warehouse.product/group
                                 8 :cg.warehouse.product/category
                                 9 :cg.warehouse.product/address}})
       (dz/drop :cg.warehouse.product/ean empty?)))
