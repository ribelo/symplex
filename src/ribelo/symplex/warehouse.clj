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
             (mapcat
              (fn [line]
                (let [[name ean stock price] (str/split line #";")]
                  (when (not-empty ean)
                    {ean {:ean   ean
                          :name  (str/lower-case name)
                          :stock (Double/parseDouble stock)
                          :price (Double/parseDouble price)}}))))
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
