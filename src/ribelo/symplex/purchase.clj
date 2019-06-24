(ns ribelo.symplex.purchase
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [java-time :as jt]
   [net.cgrand.xforms :as x]
   [ribelo.wombat.io :as wio]
   [taoensso.encore :as e]))

(defn read-file [file-path]
  (wio/read-csv file-path
                {:sep    ";"
                 :header {:product/name         1
                          :product/ean          2
                          :purchase/vendor      7
                          :purchase/date        12
                          :purchase/net-price   13
                          :purchase/gross-price 14
                          :purchase/qty         15}
                 :parse  {:product/name         str/lower-case
                          :purchase/vendor      str/lower-case
                          :purchase/date        #(jt/local-date "yy.MM.dd" %)
                          :purchase/net-price   e/as-?float
                          :purchase/gross-price e/as-?float
                          :purchase/qty         e/as-?float}}))


(defn read-files [{:keys [begin-date end-date data-path]}]
  (let [begin-date (cond-> begin-date (not (instance? java.time.LocalDate begin-date)) (jt/local-date))
        end-date   (cond-> end-date (not (instance? java.time.LocalDate end-date)) (jt/local-date))
        dates      (take-while #(jt/before? % (jt/plus end-date (jt/months 1)))
                               (jt/iterate jt/plus begin-date (jt/months 1)))]
    (->> dates
         (x/into []
                 (comp
                  (map #(let [date-str  (jt/format "yyyy_MM" %)
                              file-name (str "purchase_" date-str ".csv")
                              file-path (e/path data-path file-name)]
                          file-path))
                  (filter #(.exists (io/as-file %)))
                  (mapcat read-file))))))

(defn eans-by-vendor [coll]
  (->> coll
       (x/into {}
               (comp (filter (fn [{:keys [product/ean]}] (not-empty ean)))
                     (x/by-key :purchase/vendor (comp (map :product/ean) (x/into #{})))))))
