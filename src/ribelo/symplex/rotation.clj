(ns ribelo.symplex.rotation
  (:require
   [java-time :as jt]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [taoensso.encore :as e]
   [hanse.danzig :as d]
   [hanse.danzig.io :as dio]
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
  (->> (dio/read-csv file-path
                     {:sep    ";"
                      :header {:cg.rotation/warehouse                  0
                               :cg.rotation.product/name               2
                               :cg.rotation.product/ean                3
                               :cg.rotation/contractor                 6
                               :cg.rotation/document-id                7
                               :cg.rotation/date                       8
                               :cg.rotation.product/purchase-net-price 9
                               :cg.rotation.product/sell-net-price     10
                               :cg.rotation.product/qty                11
                               :cg.rotation.product/purchase-net-value 12
                               :cg.rotation/document-type              18}
                      :parse  {:cg.warehouse/id                        translate-warehouse
                               :cg.rotation.product/name               str/lower-case
                               :cg.rotation/contractor                 translate-contractor
                               :cg.rotation/document-id                str/lower-case
                               :cg.rotation/date                       parse-date
                               :cg.rotation.product/purchase-net-price e/as-?float
                               :cg.rotation.product/sell-net-price     e/as-?float
                               :cg.rotation.product/qty                e/as-?float
                               :cg.rotation.product/purchase-net-value e/as-?float
                               :cg.rotation/document-type              translate-doc-type}})
       (into []
             (comp
              (map (fn [m]
                     (e/if-lets [price (:cg.rotation.product/sell-net-price m)
                                 qty   (:cg.rotation.product/qty m)]
                       (assoc m :cg.rotation.product/sell-net-value (* ^double price ^double qty))
                       m)))))))

(defn read-files [{:keys [begin-date end-date data-path]}]
  (let [begin-date (cond-> begin-date (not (instance? java.time.LocalDate begin-date)) (jt/local-date))
        end-date   (cond-> end-date (not (instance? java.time.LocalDate end-date)) (jt/local-date))
        dates      (take-while #(jt/before? % (jt/plus end-date (jt/months 1)))
                               (jt/iterate jt/plus begin-date (jt/months 1)))]
    (->> dates
         (into []
                 (comp
                  (map #(let [date-str  (jt/format "yyyy_MM" %)
                              file-name (str "rotation_" date-str ".csv")
                              file-path (e/path data-path file-name)]
                          file-path))
                  (filter #(.exists (io/as-file %)))
                  (mapcat read-file))))))
