(ns clj2010
  (:require [apricot-soup :as soup])
  (:use [clj-time.core :only (date-time plus minutes year month day-of-week)]
        [clj-time.coerce :only (to-long)]
        [clj-time.format :only (formatter unparse)]
        [incanter.core :only (save sum)]
        [incanter.charts :only (bar-chart)]
        [clojure.java.io :only (reader)]
        [clojure.contrib.string :only (trim lower-case split)])
  (:import java.io.File))

; "chouser: " -> "chouser"
(defn fix-user 
  "\"chouser: \" -> \"chouser\""
  [user]
  (trim (apply str (butlast user))))

(defn tokenize 
  "Poor man's tokenizer"
  [sentence]
  (map lower-case (re-seq #"[a-zA-Z0-9'_-]+" sentence)))

(defn str->long 
  "Parse string to long"
  [s]
  (Long/valueOf s))

(def *date-re* #"(\d+)-(\d+)-(\d+)")

(defn logfile-day 
  "\"logs/2010-01-01.html\" -> #<DateTime 2010-01-01T00:00:00.000Z>"
  [logfile]
  (let [[match year month day] (re-find *date-re* logfile)]
    (apply date-time (map str->long [year month day]))))

(defn add-time 
  "Add time to day of log"
  [day hour minute]
  (plus day (minutes (+ (* 60 hour) minute))))

(defn parse-text
  "\"21:38 chouser: great, thanks!\" -> 
      [\"21\" \"38\" \"chouser: great, thanks!\"]"
  [text]
  (rest (re-find #"^(\d+):(\d+)(.*)?" text)))

(defn find-user [p]
  (let [elems (into [] (soup/search "b" p))]
    (if (empty? elems)
      nil
      (soup/text (elems 0)))))

(defn process-p
  "Process a <p>...</p> log record, return {:time ... :tokens ... :user ..}"
  [day previous-log p]
  (let [user (find-user p)
        [hour minute text] (parse-text (soup/text p))
        tokens (tokenize text)]
    { :time (add-time day (str->long hour) (str->long minute))
      :text (trim (if user (subs text (count user)) text))
      :user (if user (fix-user user) (:user previous-log))}))

(defn process-logfile 
  [logfile]
  (let [day (logfile-day logfile)
        pp (partial process-p day)]
    (rest (reductions pp (cons nil (soup/$ (slurp logfile) p))))))

(defn log-files 
  "Return list of log files under root"
  [root]
  (let [dir (File. root)
        files (filter #(not (nil? (re-find *date-re* %))) (.list dir))]
    (map #(str root "/" %) files)))

(defn load-data [root]
  (flatten (pmap process-logfile (log-files root))))

(defn flatten1 
  "Flatten one level"
  [lst]
  (mapcat identity lst))

; mapper returns [[k1 v1] [k1 v2] [k2 v3] ...], we aggregate it to
; {k1 [v1 v2] k2 [v3] ...}
(defn map-stage [mapper records]
  (let [results (flatten1 (pmap mapper records))]
    (reduce (fn [prev [k v]] (assoc prev k (cons v (prev k)))) {} results)))

(defn reduce-stage [reducer map-result]
  (let [ks (keys map-result)]
    (zipmap ks (pmap #(reducer % (map-result %)) ks))))

(defn map-reduce [mapper reducer records]
  (reduce-stage reducer (map-stage mapper records)))

(defn month-only [time]
  (date-time (year time) (month time)))

(defn gen-chart [result job]
  (let [xs (sort (keys result))
        ys (map #(result %) xs)
        fxs (map (:x-format job identity) xs)]
    (bar-chart fxs ys :title (:title job) :x-label (:x-label job) 
               :y-label (:y-label job))))

(defn outfile [job]
  (let [ext (if (:text job) "txt" "png")]
    (format "charts/%s.%s" (:outfile job) ext)))

(defn gen-text [result job]
  (let [top (take (:max job 100) (reverse (sort-by result (keys result))))
        top-vals (map result top)]
    (with-out-str (dorun (map #(println (format "%s: %s" %1 %2)) top top-vals)))))


(defn run-job [records job]
  (let [result (map-reduce (:map job) (:reduce job) records)
        [genfn savefn] (if (:text job) 
                         [gen-text spit] 
                         [gen-chart (fn [f o] (save o f))])]
    (savefn (outfile job) (genfn result job))))

(defn time-fmt [fmt]
  (fn [dt]
    (unparse (formatter fmt) dt)))

(defn load-stop-words []
  (set (line-seq (reader "stop-words.txt"))))

(def *stop-words* (load-stop-words))

(defn stop-word? [s]
  (contains? *stop-words* s))

(defn ok-word? [token]
  (and (> (count token) 2)
       (not (stop-word? token))))

(def numlines {
    :map (fn [record] [[(month-only (:time record)) 1]])
    :reduce (fn [key values] (sum values))
    :title "Lines/Month"
    :x-label "Month"
    :y-label "Lines"
    :outfile "lines"
    :x-format (time-fmt "MMM")})

(def numusers {
    :map (fn [record] [[(month-only (:time record)) (:user record)]])
    :reduce (fn [key values] (count (set values)))
    :title "Users/Month"
    :x-label "Month"
    :y-label "Users"
    :outfile "users"
    :x-format (time-fmt "MMM")})

(def active {
    :map (fn [record] (if-let [u (:user record)] [[u 1]] []))
    :reduce (fn [key values] (sum values))
    :outfile "active"
    :text true
    :max 10})

(def words {
    :map (fn [record] 
            (map (fn [tok] [tok 1]) 
                 (filter ok-word? (tokenize (:text record)))))
    :reduce (fn [key values] (sum values))
    :text true
    :max 100
    :outfile "words"})

(defn get-thanked [text]
  (and (re-find #"(?i)thank" text)
       (re-find #"[a-zA-Z0-9_-`'|]+:" text)))

(def thanked {
  :map (fn [record]
             (if-let [user (get-thanked (:text record))]
               [[(fix-user user) 1]]
               []))
  :reduce (fn [key values] (sum values))
  :text true
  :max 100
  :outfile "thanked"})


(def daylogs {
  :map (fn [record] [[(day-of-week (:time record)) 1]])
  ; Assume we have 52 of each week day per year
  :reduce (fn [key values] (/ (sum values) 52.0))
  :title "Lines/Day"
  :x-label "Day"
  :y-label "Lines (average)"
  :outfile "daylogs"
  :x-format (zipmap (range 1 8) ["Mon" "Tue" "Wed" "Thu" "Fri" "Sat" "Sun"])})

(def *jobs* [
  numlines 
  numusers 
  active
  words 
  thanked
  daylog
])

(defn -main []
  (let [records (load-data "logs")
        run (partial run-job records)]
    (dorun (pmap run *jobs*))
    (shutdown-agents)))

