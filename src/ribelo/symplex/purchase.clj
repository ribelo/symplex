(ns ribelo.symplex.purchase
  (:require
   [clojure.string :as str]
   [clojure.java.io :as io]
   [java-time :as jt]
   [taoensso.encore :as e]
   [meander.epsilon :as m]
   [hanse.danzig :as dz :refer [=>>]]
   [hanse.danzig.io :as dz.io]))

(defn parse-date [s]
  (m/match s
    (m/re #"^\d{4}\.\d{2}\.\d{2}$") (jt/local-date "yyyy.MM.dd" s)
    (m/re #"^\d{2}\.\d{2}\.\d{2}$") (jt/local-date "yy.MM.dd" s)
    _                               (throw (ex-info "bad date format" {:date s}))))

(defn read-file [file-path]
  (when (.exists (io/file file-path))
    (=>> (dz.io/read-csv file-path
                         {:sep    ";"
                          :header {1  [:cg.purchase.product/name str/lower-case str/trim]
                                   2  [:cg.purchase.product/ean :string]
                                   7  [:cg.purchase/vendor str/lower-case]
                                   12 [:cg.purchase/date parse-date]
                                   13 [:cg.purchase.product/net-price :double]
                                   14 [:cg.purchase.product/gross-price :double]
                                   15 [:cg.purchase/qty :double]}})
         (dz/drop :cg.purchase.product/ean empty?))))

(defn read-files [{:keys [begin-date end-date data-path]}]
  (let [begin-date (cond-> begin-date (not (instance? java.time.LocalDate begin-date)) (jt/local-date))
        end-date   (cond-> end-date (not (instance? java.time.LocalDate end-date)) (jt/local-date))
        dates      (take-while #(jt/before? % (jt/plus end-date (jt/months 1)))
                               (jt/iterate jt/plus begin-date (jt/months 1)))]
    (reduce
     (fn [acc dt]
       (let [date-str  (jt/format "yyyy_MM" dt)
             file-name (str "purchase_" date-str ".csv")
             data      (read-file (e/path data-path file-name))]
         (if acc (into acc data) data)))
     nil
     dates)))
