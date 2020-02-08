(ns ribelo.symplex.warehouse
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [net.cgrand.xforms :as x]
   [ribelo.wombat.io :as wio]
   [taoensso.encore :as e]))

(defn- vat-parse [s]
  (if-let [vat (-> (re-find #"\d+" s) e/as-?float)]
    (/ vat 100.0)
    0.0))

(defn read-file [file-path]
  (->> (wio/read-csv file-path
                     {:sep    ";"
                      :header {:product/name                 0
                               :product/ean                  1
                               :warehouse/stock              2
                               :warehouse/purchase-net-price 3
                               :warehouse/sell-net-price-1   4
                               :warehouse/sell-net-price-2   5
                               :product/vat                  6}
                      :parse  {:product/name                 str/lower-case
                               :warehouse/stock              e/as-?float
                               :warehouse/purchase-net-price e/as-?float
                               :warehouse/sell-net-price-1   e/as-?float
                               :warehouse/sell-net-price-2   e/as-?float
                               :product/vat                  vat-parse}})
       (into {} (comp
                 (filter (fn [{:keys [product/ean]}] (seq ean)))
                 (x/by-key :product/ean (x/into {}))))))

(read-file "/home/ribelo/sync/schowek/teas/dane/mag.csv")

(defn add-orders [orders warehouse]
  (reduce
   (fn [acc [ean {:keys [^double qty]}]]
     (assoc-in acc [ean :ordered] qty))
   warehouse
   orders))

(defn calculate-supply [warehouse sales]
  (->> warehouse
       (into {}
             (comp (map (fn [[ean {:keys [^double warehouse/stock ^double warehouse/ordered]
                                   :or {ordered 0.0}
                                   :as m}]]
                          {ean (let [mean (get-in sales [ean :mean] 0.0)
                                     stock* (+ stock ordered)]
                                 (if (pos? mean)
                                   (assoc m :supply (/ stock* mean))
                                   (assoc m :supply ##Inf)))}))))))
