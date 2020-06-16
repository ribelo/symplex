(ns ribelo.symplex.purchase
  (:require
   [clojure.string :as str]
   [clojure.java.io :as io]
   [java-time :as jt]
   [taoensso.encore :as e]
   [tech.ml.dataset :as ds]))

(defn parse-date [s]
  (cond (re-find #"^\d{4}\.\d{2}\.\d{2}$" s) (jt/local-date "yyyy.MM.dd" s)
        (re-find #"^\d{2}\.\d{2}\.\d{2}$" s) (jt/local-date "yy.MM.dd" s)
        :else                                (throw (ex-info "bad date format" {:date s}))))

(defn read-file [file-path]
  (when (.exists (io/file file-path))
    (let [data (->> (ds/->dataset file-path
                                  {:separator        \;
                                   :header-row?      false
                                   :column-whitelist [1 2 7 12 13 14 15]
                                   :key-fn           {0 :cg.purchase.product/name
                                                      1 :cg.purchase.product/ean
                                                      2 :cg.purchase/vendor
                                                      3 :cg.purchase/date
                                                      4 :cg.purchase.product/net-price
                                                      5 :cg.purchase.product/gross-price
                                                      6 :cg.purchase/qty}
                                   :parser-fn        {3 :string
                                                      4 :float32
                                                      5 :float32
                                                      6 :float32}})
                    (ds/filter (fn [row] (identity (get row :cg.purchase.product/ean)))))]
      (-> data
          (ds/update-column :cg.purchase.product/name (fn [col] (map str/lower-case col)))
          (ds/update-column :cg.purchase/vendor (fn [col] (map str/lower-case col)))
          (ds/update-column :cg.purchase/date (fn [col] (map parse-date col)))))))



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
         (if acc (ds/concat acc data) data)))
     nil
     dates)))
