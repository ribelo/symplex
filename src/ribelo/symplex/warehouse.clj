(ns ribelo.symplex.warehouse
  (:require
   [clojure.string :as str]
   [taoensso.encore :as e]
   [tech.ml.dataset :as ds]))

(defn- vat-parse [s]
  (if-let [vat (-> (re-find #"\d+" s) e/as-?float)]
    (/ vat 100.0)
    0.0))

(defn read-file [file-path]
  (->> (ds/->dataset file-path
                     {:separator   \;
                      :header-row? false
                      :key-fn      {0 :cg.warehouse.product/name
                                    1 :cg.warehouse.product/ean
                                    2 :cg.warehouse.product/stock
                                    3 :cg.warehouse.product/purchase-net-price
                                    4 :cg.warehouse.product/sell-net-price-1
                                    5 :cg.warehouse.product/sell-net-price-2
                                    6 :cg.warehouse.product/vat}
                      :parser-fn   {1 [:string str/lower-case]
                                    2 :float32
                                    3 :float32
                                    4 :float32
                                    5 :float32
                                    6 [:float32 vat-parse]}})
       (ds/filter (fn [row] (identity (get row :cg.warehouse.product/ean))))))
