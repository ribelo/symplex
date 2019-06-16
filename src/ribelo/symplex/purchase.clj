(ns ribelo.symplex.purchase
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [taoensso.encore :as e]
   [net.cgrand.xforms :as x]
   [net.cgrand.xforms.io :as xio]
   [java-time :as jt]))

(defn read-file [file-path]
  (when (.exists (io/as-file ^string file-path))
    (into []
          (comp (map #(str/split % #";"))
                (map (fn [{name        1
                           ean         2
                           vendor      7
                           date        12
                           net-price   13
                           gross-price 14
                           qty         15}]
                       {:name        (str/lower-case name)
                        :ean         ean
                        :vendor      (str/lower-case vendor)
                        :date        (jt/local-date "yy.MM.dd" date)
                        :net-price   (Double/parseDouble net-price)
                        :gross-price (Double/parseDouble gross-price)
                        :qty         (Double/parseDouble qty)})))
          (xio/lines-in (io/reader file-path)))))

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
               (comp (filter (fn [{:keys [ean]}] (not-empty ean)))
                     (x/by-key :vendor (comp (map :ean) (x/into #{})))))))

(read-files {:begin-date "2019-01-01"
             :end-date   "2019-01-05"
             :data-path  "/home/ribelo/sync/schowek/teas/dane"})
