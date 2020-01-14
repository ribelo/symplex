(ns ribelo.symplex.rotation
  (:require
   [java-time :as jt]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [taoensso.encore :as e]
   [ribelo.wombat :as wb]
   [ribelo.wombat.io :as wio]
   [net.cgrand.xforms :as x]))

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
    :else                        s))

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
      "KasyFis" :sales/receipt)
    (catch Exception e
      (throw (ex-info s {:s s})))))

(defn parse-date [s]
  (cond (re-find #"^\d{4}\.\d{2}\.\d{2}$" s) (jt/local-date "yyyy.MM.dd" s)
        (re-find #"^\d{2}\.\d{2}\.\d{2}$" s) (jt/local-date "yy.MM.dd" s)
        :else                                (throw (ex-info "bad date format" {:date s}))))

(defn read-file [file-path]
  (->> (wio/read-csv file-path
                    {:sep    ";"
                     :header {:market/id                   0
                              :product/name                2
                              :product/ean                 3
                              :rotation/contractor         6
                              :rotation/document-id        7
                              :rotation/date               8
                              :rotation/purchase-net-price 9
                              :rotation/sell-net-price     10
                              :rotation/qty                11
                              :rotation/purchase-net-value 12
                              :rotation/document-type      18}
                     :parse  {:market/id                   translate-warehouse
                              :product/name                str/lower-case
                              :rotation/contractor         translate-contractor
                              :rotation/document-id        str/lower-case
                              :rotation/date               parse-date
                              :rotation/purchase-net-price e/as-?float
                              :rotation/sell-net-price     e/as-?float
                              :rotation/qty                e/as-?float
                              :rotation/purchase-net-value e/as-?float
                              :rotation/document-type      translate-doc-type}})
      (into []
            (comp
             (wb/set :rotation/sell-net-value
                     (fn [^double x ^double y]
                       (when (and (number? x) (number? y))
                         (* x y)))
                     [:rotation/sell-net-price :rotation/qty])))))


(defn read-files [{:keys [begin-date end-date data-path]}]
  (let [begin-date (cond-> begin-date (not (instance? java.time.LocalDate begin-date)) (jt/local-date))
        end-date   (cond-> end-date (not (instance? java.time.LocalDate end-date)) (jt/local-date))
        dates      (take-while #(jt/before? % (jt/plus end-date (jt/months 1)))
                               (jt/iterate jt/plus begin-date (jt/months 1)))]
    (->> dates
         (x/into []
                 (comp
                  (map #(let [date-str  (jt/format "yyyy_MM" %)
                              file-name (str "rotation_" date-str ".csv")
                              file-path (e/path data-path file-name)]
                          file-path))
                  (filter #(.exists (io/as-file %)))
                  (mapcat read-file))))))

(defn filter-contractor [name]
  (filter (fn [{:keys [rotation/contractor]}] (= contractor name))))

(defn filter-market-id [s]
  (filter (fn [{:keys [market/id]}] (= id s))))
