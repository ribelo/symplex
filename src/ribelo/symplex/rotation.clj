(ns ribelo.symplex.rotation
  (:require
   [java-time :as jt]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [taoensso.encore :as e]
   [tech.ml.dataset :as ds]))

(defn translate-warehouse [s]
  (case s
    "M 6" "s2"
    "cg"))

(defn translate-contractor [s]
  (cond
    (re-find #"(?iu)sklep 1" s) "f01451"
    (re-find #"(?iu)sklep 2" s) "s2"
    (re-find #"(?iu)sklep 3" s) "f01450"
    (re-find #"(?iu)sklep 4" s) "f01752"
    :else                       s))

(defn translate-doc-type [s]
  (try
    (case s
      "Zakup"   :purchase/invoice
      "F.Sprz." :sales/invoice
      "Paragon" :sales/receipt
      "WZ/OCZ." :sales/slip
      "Mfilia-" :movements/out
      "Mfilia+" :movements/in
      "Mmag-"   :movements/out
      "Mmag+"   :movements/in
      "F.Kor.S" :sales/correction
      "PZ/OCZ." :purchase/slip
      "Ro.Wewn" :movements/out
      "Zak.Kor" :purchase/correction
      "KasyFis" :sales/receipt
      "Inwent." :movements/invent)
    (catch Exception e
      (throw (ex-info s {:s s})))))

(defn parse-date [s]
  (cond (re-find #"^\d{4}\.\d{2}\.\d{2}$" s) (jt/local-date "yyyy.MM.dd" s)
        (re-find #"^\d{2}\.\d{2}\.\d{2}$" s) (jt/local-date "yy.MM.dd" s)
        :else                                (throw (ex-info "bad date format" {:date s}))))

(defn read-file [file-path]
  (when (.exists (io/file file-path))
    (let [data (->> (ds/->dataset file-path
                                  {:separator        \;
                                   :header-row?      false
                                   :column-whitelist [0 2 3 6 7 8 9 10 11 12 18]
                                   :key-fn           {0  :cg.rotation/warehouse
                                                      1  :cg.rotation.product/name
                                                      2  :cg.rotation.product/ean
                                                      3  :cg.rotation/contractor
                                                      4  :cg.rotation/document-id
                                                      5  :cg.rotation/date
                                                      6  :cg.rotation.product/purchase-net-price
                                                      7  :cg.rotation.product/sell-net-price
                                                      8  :cg.rotation.product/qty
                                                      9  :cg.rotation.product/purchase-net-value
                                                      10 :cg.rotation/document-type}
                                   :parser-fn        {0  [:string translate-warehouse]
                                                      1  [:string str/lower-case]
                                                      2  :string
                                                      3  [:string translate-contractor]
                                                      4  [:string str/lower-case]
                                                      5  :string
                                                      6  :float32
                                                      7  :float32
                                                      8  :float32
                                                      9  :float32
                                                      10 [:keyword translate-doc-type]}})
                    (ds/filter (fn [row] (identity (get row :cg.rotation.product/ean))))
                    (ds/filter (fn [row] (identity (get row :cg.rotation/document-id)))))]
      (-> data
          (ds/update-column :cg.rotation/date (fn [col] (map parse-date col)))))))

(defn read-files [{:keys [begin-date end-date data-path]}]
  (let [begin-date (cond-> begin-date (not (instance? java.time.LocalDate begin-date)) (jt/local-date))
        end-date   (cond-> end-date (not (instance? java.time.LocalDate end-date)) (jt/local-date))
        dates      (take-while #(jt/before? % (jt/plus end-date (jt/months 1)))
                               (jt/iterate jt/plus begin-date (jt/months 1)))]
    (reduce
     (fn [acc dt]
       (let [date-str  (jt/format "yyyy_MM" dt)
             file-name (str "rotation_" date-str ".csv")
             data      (read-file (e/path data-path file-name))]
         (if acc (ds/concat acc data) data)))
     nil
     dates)))
