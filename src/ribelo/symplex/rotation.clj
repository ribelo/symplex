(ns ribelo.symplex.rotation
  (:require
   [java-time :as jt]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [taoensso.encore :as e]
   [meander.epsilon :as m]
   [hanse.danzig :as dz :refer [=>>]]
   [hanse.danzig.io :as dz.io]))

(defn translate-warehouse [s]
  (m/match s
    "M 6" "s2"
    _     "cg"))

(defn translate-contractor [s]
  (m/match s
    (m/re #"(?iu)sklep 1") "f01451"
    (m/re #"(?iu)sklep 2") "s2"
    (m/re #"(?iu)sklep 3") "f01450"
    (m/re #"(?iu)sklep 4") "f01752"
    _                      s))

(defn translate-doc-type [s]
  (m/match s
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
    "Inwent." :movements/invent
    _         (throw (ex-info "bad doc type" {:type s}))))

(defn parse-date [s]
  (m/match s
    (m/re #"^\d{4}\.\d{2}\.\d{2}$") (jt/local-date "yyyy.MM.dd" s)
    (m/re #"^\d{2}\.\d{2}\.\d{2}$") (jt/local-date "yy.MM.dd" s)
    _                               (throw (ex-info "bad date format" {:date s}))))

(defn read-file [file-path]
  (when (.exists (io/file file-path))
    (=>> (dz.io/read-csv file-path
                         {:sep    ";"
                          :header {0  [:cg.rotation/warehouse translate-warehouse]
                                   2  [:cg.rotation.product/name str/lower-case]
                                   3  [:cg.rotation.product/ean str]
                                   6  [:cg.rotation/contractor str/lower-case]
                                   7  [:cg.rotation.document/id str/lower-case]
                                   8  [:cg.rotation/date parse-date]
                                   9  [:cg.rotation.product/purchase-net-price :double]
                                   10 [:cg.rotation.product/sell-net-price :double]
                                   11 [:cg.rotation.product/qty :double]
                                   12 [:cg.rotation.product/purchase-net-value :double]
                                   18 [:cg.rotation.document/type translate-doc-type]}})
         (dz/drop {:cg.rotation.product/ean empty?
                   :cg.rotation.document/id empty?}))))

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
         (if acc (into acc data) data)))
     nil
     dates)))
