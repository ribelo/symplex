(ns ribelo.symplex.edi
  (:require
   [java-time :as jt]
   [taoensso.encore :as e]
   [clojure.string :as str]
   [ribelo.wombat :as wb]))



(defn edi-header [{:keys [name tax-number date-time issue-date
                          bank-account city zip-code warehouse]
                   :or   {warehouse 0}}]
  (let [date-time  (cond-> date-time (not (instance? java.time.LocalDate date-time)) (jt/local-date))
        issue-date (cond-> date-time (not (instance? java.time.LocalDate issue-date)) (jt/local-date))]
    (str "[Info]\r\n"
         "Program=SMALL BUSINESS 5.2.2730.8629\r\n"
         "Kodowanie=Windows 1250\r\n"
         "Nazwa="  (str/lower-case name) "\r\n"
         "Nip=" tax-number "\r\n"
         "Konto=" bank-account "\r\n"
         "Kod=" zip-code "\r\n"
         "Miasto=" (str/lower-case city) "\r\n"
         "Data=" (jt/format "YY.MM.dd" date-time) "\r\n"
         "Godz=" (jt/format "HH.mm.ss" issue-date) "\r\n"
         "\r\n"
         "\r\n"
         "[Dokument]\r\n"
         "DataWyst=" (jt/format "YY.MM.dd" (jt/local-date-time)) "\r\n"
         "Odbiorca=1\r\n"
         "NrDok=\r\n"
         "IdentyfikatorDok=" (Long/parseLong (jt/format "YYMMddHHmmss" (jt/local-date-time))) "\r\n"
         "Magazyny=" warehouse  "\r\n"
         "\r\n"
         "\r\n"
         "[ZawartoscDokumentu]"
         "\r\n")))

(def tmp-doc
  "[Dokument]
Rejestr=Magazyn
[ZawartoscDokumentu]
[Poz1]
Mag=0
Nazwa=@ ZOTT Jogobella Kubek Classic 20x150g
Symbol=4014500006093
Indeks=1000
Jm=szt
SymbolDostawcy=12-8%
CenaZakupuNetto=0.96000
CenaZakupuBrutto=1.00800
Grupa=27
GrupaNazwa=Nabiał
Kategoria=Teas-Zott-Jog-20
Vat= 5 %
VatSpr= 5 %
StanPocz=267.000
StanKonc=267.000
PKWIU=080-A01   1p
NazwaDlaKasy=
Blokady=16779264
Opis=jogurt kubek
JmPIH=0.15000 kg
CenaSprzedazyNetto=1.00
CenaSprzedazyNetto2=1.13
CenaSprzedazyBrutto=1.05
CenaSprzedazyBrutto2=1.19
Marza=0.000
Marza2=0.000
")
(def tmp-content
  "[ZawartoscDokumentu]
[Poz1]
Mag=0
Nazwa=@ ZOTT Jogobella Kubek Classic 20x150g
Symbol=4014500006093
Indeks=1000
Jm=szt
SymbolDostawcy=12-8%
CenaZakupuNetto=0.96000
CenaZakupuBrutto=1.00800
Grupa=27
GrupaNazwa=Nabiał
Kategoria=Teas-Zott-Jog-20
Vat= 5 %
VatSpr= 5 %
StanPocz=267.000
StanKonc=267.000
PKWIU=080-A01   1p
NazwaDlaKasy=
Blokady=16779264
Opis=jogurt kubek
JmPIH=0.15000 kg
CenaSprzedazyNetto=1.00
CenaSprzedazyNetto2=1.13
CenaSprzedazyBrutto=1.05
CenaSprzedazyBrutto2=1.19
Marza=0.000
Marza2=0.000
")
(def tmp-item
  "[Poz1]
Mag=0
Nazwa=@ ZOTT Jogobella Kubek Classic 20x150g
Symbol=4014500006093
Indeks=1000
Jm=szt
SymbolDostawcy=12-8%
CenaZakupuNetto=0.96000
CenaZakupuBrutto=1.00800
Grupa=27
GrupaNazwa=Nabiał
Kategoria=Teas-Zott-Jog-20
Vat= 5 %
VatSpr= 5 %
StanPocz=267.000
StanKonc=267.000
PKWIU=080-A01   1p
NazwaDlaKasy=
Blokady=16779264
Opis=jogurt kubek
JmPIH=0.15000 kg
CenaSprzedazyNetto=1.00
CenaSprzedazyNetto2=1.13
CenaSprzedazyBrutto=1.05
CenaSprzedazyBrutto2=1.19
Marza=0.000
Marza2=0.000
")

(def edi->kv
  {"rejestr"             [:document/type str/trim]
   "mag"                 [:warehouse/id #(some-> % str/trim)]
   "nazwa"               [:product/name #(-> % str/lower-case str/trim)]
   "symbol"              [:product/ean #(some-> % str/trim)]
   "indeks"              [:product/id #(some-> % str/trim)]
   "jm"                  [:product/unit #(some-> % str/trim)]
   "cenazakypunetto"     [:product/purchase-net-price e/as-?float]
   "cenazakupubrutto"    [:product/purchase-gross-price e/as-?float]
   "grupa"               [:product/group-id #(some-> % str/trim)]
   "grupanazwa"          [:product/group #(some-> % str/trim)]
   "kategoria"           [:product/category #(some-> % str/trim)]
   "vat"                 [:product/vat #(-> (re-find #"\d+" %) ((fnil e/as-?float 0.0)) ((partial * 0.01)))]
   "stankonc"            [:product/qty e/as-?float]
   "pkwiu"               [:product/address #(some-> % str/trim)]
   "cenasprzedazynetto"  [:product/sell-net-price e/as-?float]
   "cenasprzedazybrutto" [:product/sell-gross-price e/as-?float]
   "barkod"              [:barcode/ean #(some-> % str/trim)]
   "mnoznik"             [:barcode/multiplier e/as-?float]})

(defn item->map [lines]
  (loop [[line & lines] (drop 1 lines)
         m              {}]
    (if (and line (not (str/starts-with? line "[")))
      (if-let [[k v] (-> (str/lower-case line) (str/split #"="))]
        (if-let [[k f] (get edi->kv k)]
          (do
            ;; (println :k k :v v)
            (recur lines (assoc m k (f (or v "")))))
          (recur lines m))
        (recur lines m))
      m)))

(defn content->map [lines]
  (loop [[line & lines] lines
         i              0
         m              (sorted-map)]
    (if (and line (not (str/starts-with? line "[Dokument")))
      (if (str/starts-with? line "[Poz")
        (recur lines (inc i) (assoc m i (item->map lines)))
        (recur lines i m))
      m)))

(defn document->map [lines]
  (loop [[line & lines]   lines
         i                0
         inside-document? false
         inside-content?  false
         m                (sorted-map)]
    (if line
      (if (str/starts-with? line "[ZawartoscDokumentu")
        (recur lines (inc i) true (assoc-in m [i :document/content] (content->map lines)))
        (if (and (not inside-content?) (re-find #"=" line))
          (let [[k v] (-> (str/lower-case line) (str/split #"="))
                [k f] (get edi->kv k)]
            (if k
              (recur lines i inside-content? (assoc-in m [i k] (f (or v ""))))
              (recur lines i inside-content? m)))
          (recur lines i inside-content? m)))
      m)))

(defn document->map2 [lines]
  (loop [[line & lines] lines
         ks             []
         i              0
         m              {}]
    (if line
      (cond
        (re-find #"\[info\]" (str/lower-case line))
        (recur lines [:info] i m)

        (re-find #"\[okres\]" (str/lower-case line))
        (recur lines [:period] i m)

        (re-find #"\[dokument\]" (str/lower-case line))
        (recur lines [:document i] (inc i) m)

        (re-find #"\[zawartoscdokumentu\]" (str/lower-case line))
        (recur lines [:document (dec i) :content] i m)

        (re-find #"\[poz\d+\]" (str/lower-case line))
        (let [j (-> (re-find #"\d+") e/as-int)]
          (recur lines [:document (dec i) :content j] i m))

        (re-find #"=" line)
        (let [[k v] (-> (str/lower-case line) (str/split #"="))
              [k f] (get edi->kv k)]
          (if k
            (recur lines ks i (assoc-in m (conj ks k) v))
            (recur lines ks i m))))
      m)))

(re-find #"\[poz(\d+)\]" "[poz1]")
(document->map2 (str/split-lines (slurp "/home/ribelo/sync/schowek/rk/towary.txt")))

(assoc-in [] [ :document :content] 1)
