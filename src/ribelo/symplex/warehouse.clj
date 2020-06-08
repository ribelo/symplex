(ns ribelo.symplex.warehouse
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [net.cgrand.xforms :as x]
   [hanse.danzig :as d]
   [hanse.danzig.io :as dio]
   [taoensso.encore :as e]))

(defn- vat-parse [s]
  (if-let [vat (-> (re-find #"\d+" s) e/as-?float)]
    (/ vat 100.0)
    0.0))

(defn read-file [file-path]
  (->> (dio/read-csv file-path
                     {:sep    ";"
                      :header {:cg.warehouse.product/name               0
                               :cg.warehouse.product/ean                1
                               :cg.warehouse.product/stock              2
                               :cg.warehouse.product/purchase-net-price 3
                               :cg.warehouse.product/sell-net-price-1   4
                               :cg.warehouse.product/sell-net-price-2   5
                               :cg.warehouse.product/vat                6}
                      :parse  {:cg.warehouse.product/name               str/lower-case
                               :cg.warehouse.product/stock              e/as-?float
                               :cg.warehouse.product/purchase-net-price e/as-?float
                               :cg.warehouse.product/sell-net-price-1   e/as-?float
                               :cg.warehouse.product/sell-net-price-2   e/as-?float
                               :cg.warehouse.product/vat                vat-parse}})
       (into [] (filter (fn [m] (seq (:cg.warehouse.product/ean m)))))))
