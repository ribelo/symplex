(ns ribelo.symplex.warehouse
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [net.cgrand.xforms.io :as xio]
   [taoensso.encore :as e]))

(defn read-file [{:keys [data-path file-name]
                  :or   {file-name "mag.csv"}}]
  (let [file-path (e/path data-path file-name)]
    (when (.exists (io/as-file ^string file-path))
      (into {}
            (comp
             (map #(str/split % #";"))
             (map (fn [{name               0
                        ean                1
                        stock              2
                        purchase-net-price 3
                        sell-net-price-1   4
                        sell-net-price-2   5}]
                    (when (not-empty ean)
                      {ean {:product/ean                  ean
                            :product/name                 (str/lower-case name)
                            :stock/qty                    (Double/parseDouble stock)
                            :warehouse/purchase-net-price (Double/parseDouble purchase-net-price)
                            :warehouse/sell-net-price-1 (Double/parseDouble sell-net-price-1)
                            :warehouse/sell-net-price-2 (Double/parseDouble sell-net-price-2)}})))
             (filter identity))
            (xio/lines-in (io/reader file-path))))))

(defn add-orders [orders warehouse]
  (reduce
   (fn [acc [ean {:keys [^double qty]}]]
     (assoc-in acc [ean :ordered] qty))
   warehouse
   orders))

(defn calculate-supply [warehouse sales]
  (->> warehouse
       (into {}
             (comp (map (fn [[ean {:keys [^double stock ^double ordered]
                                   :or {ordered 0.0}
                                   :as m}]]
                          {ean (let [mean (get-in sales [ean :mean] 0.0)
                                     stock* (+ stock ordered)]
                                 (if (pos? mean)
                                   (assoc m :supply (/ stock* mean))
                                   (assoc m :supply ##Inf)))}))))))
