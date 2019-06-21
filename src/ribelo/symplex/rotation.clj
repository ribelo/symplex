(ns ribelo.symplex.rotation
  (:require
   [java-time :as jt]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [taoensso.encore :as e]
   [ribelo.wombat.io :as wio]
   [net.cgrand.xforms :as x]))

(defn translate-warehouse [s]
  (case s
    "M 6" "s2"
    "cg"))

(defn translate-contractor [s]
  (cond
    (re-find #"(?iu)sklep 1" s) "f01451"
    (re-find #"(?iu)sklep 2" s) "sklep2"
    (re-find #"(?iu)sklep 3" s) "f01450"
    (re-find #"(?iu)sklep 4" s) "f01752"
    :else                        s))

(defn translate-doc-type [s]
  (try
    (case s
      "Zakup"   :delivery
      "F.Sprz." :dispatch
      "Paragon" :dispatch
      "WZ/OCZ." :dispatch
      "Mfilia-" :dispatch
      "Mfilia+" :delivery
      "Mmag-"   :dispatch
      "Mmag+"   :delivery
      "F.Kor.S" :delivery
      "PZ/OCZ." :delivery
      "Ro.Wewn" :dispatch
      "Zak.Kor" :dispatch
      "KasyFis" :dispatch)
    (catch Exception e
      (throw (ex-info s {:s s})))))

(defn read-file [file-path]
  (wio/read-csv file-path
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
                          :rotation/purchase/net-vale  12
                          :rotation/document-type      18}
                 :parse  {:market/id                   translate-warehouse
                          :product/name                str/lower-case
                          :rotation/contractor         translate-contractor
                          :rotation/document-id        str/lower-case
                          :rotation/date               #(jt/local-date "yy.MM.dd" %)
                          :rotation/purchase-net-price e/as-?float
                          :rotation/sell-net-price     e/as-?float
                          :rotation/qty                e/as-?float
                          :rotation/purchase/net-vale  e/as-?float
                          :rotation/document-type      translate-doc-type}}))

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
