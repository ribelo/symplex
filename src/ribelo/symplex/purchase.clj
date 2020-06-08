(ns ribelo.symplex.purchase
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [java-time :as jt]
   [net.cgrand.xforms :as x]
   [hanse.danzig :as d]
   [hanse.danzig.io :as dio]
   [taoensso.encore :as e]
   [clojure.data.csv :as csv]))

(defn parse-date [s]
  (cond (re-find #"^\d{4}\.\d{2}\.\d{2}$" s) (jt/local-date "yyyy.MM.dd" s)
        (re-find #"^\d{2}\.\d{2}\.\d{2}$" s) (jt/local-date "yy.MM.dd" s)
        :else                                (throw (ex-info "bad date format" {:date s}))))

(defn read-file [file-path]
  (dio/read-csv file-path
                {:sep    ";"
                 :header {:cg.purchase.product/name        1
                          :cg.purchase.product/ean         2
                          :cg.purchase/vendor              7
                          :cg.purchase/date                12
                          :cg.purchase.product/net-price   13
                          :cg.purchase.product/gross-price 14
                          :cg.purchase/qty                 15}
                 :parse  {:cg.purchase.product/name        str/lower-case
                          :cg.purchase/vendor              str/lower-case
                          :cg.purchase/date                parse-date
                          :cg.purchase.product/net-price   e/as-?float
                          :cg.purchase.product/gross-price e/as-?float
                          :cg.purchase/qty                 e/as-?float}}))

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
