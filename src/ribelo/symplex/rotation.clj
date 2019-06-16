(ns ribelo.symplex.rotation
  (:require
   [java-time :as jt]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [taoensso.encore :as e]
   [net.cgrand.xforms :as x]
   [net.cgrand.xforms.io :as xio]))

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
      "Zak.Kor" :dispatch)
    (catch Exception e
      (throw (ex-info s {:s s})))))

(defn read-file [file-path]
  (when (.exists (io/as-file file-path))
    (into []
          (comp (map #(str/split % #";"))
                (map (fn [{warehouse          0
                           name               2
                           ean                3
                           contractor         6
                           doc-id             7
                           date               8
                           purchase-net-price 9
                           sell-net-price     10
                           qty                11
                           purchase-net-value 12
                           doc-type           18}]
                       (let [doc-type' (translate-doc-type doc-type)]
                         {:market-id          (translate-warehouse warehouse)
                          :product-name       (str/lower-case name)
                          :ean                ean
                          :contractor         (translate-contractor contractor)
                          :doc-id             (str/lower-case doc-id)
                          :date               (jt/local-date "yy.MM.dd" date)
                          :purchase-net-price (Double/parseDouble purchase-net-price)
                          :sell-net-price     (Double/parseDouble sell-net-price)
                          :qty                (Double/parseDouble qty)
                          :purchase-net-value (Double/parseDouble purchase-net-value)
                          :doc-type           doc-type'}))))
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
                              file-name (str "rotation_" date-str ".csv")
                              file-path (e/path data-path file-name)]
                          file-path))
                  (filter #(.exists (io/as-file %)))
                  (mapcat read-file))))))

(defn filter-contractor [name]
  (filter (fn [{:keys [contractor]}] (= contractor name))))
