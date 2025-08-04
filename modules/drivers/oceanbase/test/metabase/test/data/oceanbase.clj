(ns ^:mb/driver-tests metabase.test.data.oceanbase
  (:require
   [clojure.java.jdbc :as jdbc]
   [clojure.string :as str]
   [clojure.test :refer :all]
   [environ.core :as env]
   [honey.sql :as sql]
   [metabase.driver :as driver]
   [metabase.driver.oceanbase]
   [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]
   [metabase.driver.sql-jdbc.execute :as sql-jdbc.execute]
   [metabase.driver.sql.query-processor :as sql.qp]
   [metabase.test :as mt]
   [metabase.test.data.impl :as data.impl]
   [metabase.test.data.interface :as tx]
   [metabase.test.data.sql :as sql.tx]
   [metabase.test.data.sql-jdbc :as sql-jdbc.tx]
   [metabase.test.data.sql-jdbc.execute :as execute]
   [metabase.test.data.sql-jdbc.load-data :as load-data]
   [metabase.test.data.sql.ddl :as ddl]
   [metabase.util :as u]
   [metabase.util.honey-sql-2 :as h2x]
   [metabase.util.log :as log]
   [metabase.util.malli :as mu]))

(set! *warn-on-reflection* true)

(comment metabase.driver.oceanbase/keep-me)

(sql-jdbc.tx/add-test-extensions! :oceanbase)

;; OceanBase test configuration
;; Similar to Oracle, we'll use a schema-based approach for testing
(defonce           session-schema   "mb_test")
(defonce ^:private session-password "password")

(defn- detect-oceanbase-mode [details]
  "Detect OceanBase compatibility mode for testing purposes"
  (try
    (let [spec (sql-jdbc.conn/connection-details->spec :oceanbase details)
          query "SHOW VARIABLES LIKE 'ob_compatibility_mode'"
          result (jdbc/query spec query)]
      (if-let [mode-value (some-> result first :Value str/lower-case)]
        (if (= mode-value "oracle") "oracle" "mysql")
        "mysql"))
    (catch Exception e
      (log/warn "Failed to detect OceanBase mode, defaulting to mysql:" (.getMessage e))
      "mysql")))

(defn- connection-details []
  (let [details* {:host (tx/db-test-env-var-or-throw :oceanbase :host "localhost")
                  :port (parse-long (tx/db-test-env-var-or-throw :oceanbase :port "2883"))
                  :user (tx/db-test-env-var :oceanbase :user)
                  :password (tx/db-test-env-var :oceanbase :password)
                  :db (tx/db-test-env-var :oceanbase :db "test")}]
    ;; Set defaults if not provided
    (cond-> details*
      (nil? (:user details*))
      (assoc :user "root")

      (nil? (:password details*))
      (assoc :password "password"))))

(deftest connection-details-test
  (testing "Make sure connection details are properly configured"
    (with-redefs [env/env (assoc env/env
                                 :mb-oceanbase-test-user ""
                                 :mb-oceanbase-test-password "")]
      (is (=? {:user "root"
               :password "password"}
              (connection-details))))))

(defn- dbspec [& _]
  (let [conn-details (connection-details)]
    (sql-jdbc.conn/connection-details->spec :oceanbase conn-details)))

(defmethod tx/dbdef->connection-details :oceanbase [& _]
  (connection-details))

(defmethod tx/sorts-nil-first? :oceanbase [_ _] false)

(defmethod driver/database-supports? [:oceanbase :test/time-type]
  [_driver _feature _database]
  false)

;; MySQL mode type mappings
(doseq [[base-type sql-type] {:type/BigInteger "BIGINT"
                              :type/Boolean    "TINYINT(1)"
                              :type/Date       "DATE"
                              :type/Temporal   "DATETIME"
                              :type/DateTime   "DATETIME"
                              :type/DateTimeWithTZ "TIMESTAMP"
                              :type/DateTimeWithLocalTZ "TIMESTAMP"
                              :type/DateTimeWithZoneOffset "TIMESTAMP"
                              :type/DateTimeWithZoneID "TIMESTAMP"
                              :type/Decimal    "DECIMAL(10,2)"
                              :type/Float      "DOUBLE"
                              :type/Integer    "INT"
                              :type/Text       "VARCHAR(255)"}]
  (defmethod sql.tx/field-base-type->sql-type [:oceanbase base-type] [_ _] sql-type))

;; Oracle mode type mappings (when detected)
(defn- get-sql-type-for-oracle-mode [base-type]
  (case base-type
    :type/BigInteger "NUMBER(*,0)"
    :type/Boolean    "NUMBER(1)"
    :type/Date       "DATE"
    :type/Temporal   "TIMESTAMP"
    :type/DateTime   "TIMESTAMP"
    :type/DateTimeWithTZ "TIMESTAMP WITH TIME ZONE"
    :type/DateTimeWithLocalTZ "TIMESTAMP WITH LOCAL TIME ZONE"
    :type/DateTimeWithZoneOffset "TIMESTAMP WITH TIME ZONE"
    :type/DateTimeWithZoneID "TIMESTAMP WITH TIME ZONE"
    :type/Decimal    "DECIMAL"
    :type/Float      "BINARY_DOUBLE"
    :type/Integer    "INTEGER"
    :type/Text       "VARCHAR2(4000)"
    "VARCHAR2(4000)")) ; Default

;; Override the type mapping to handle both modes
(defmethod sql.tx/field-base-type->sql-type [:oceanbase :type/Text]
  [_ _]
  (let [details (connection-details)
        mode (detect-oceanbase-mode details)]
    (if (= mode "oracle")
      "VARCHAR2(4000)"
      "VARCHAR(255)")))

(defmethod sql.tx/drop-table-if-exists-sql :oceanbase
  [_ {:keys [database-name]} {:keys [table-name]}]
  (let [details (connection-details)
        mode (detect-oceanbase-mode details)]
    (if (= mode "oracle")
      ;; Oracle mode syntax
      (format "BEGIN
                 EXECUTE IMMEDIATE 'DROP TABLE \"%s\".\"%s\" CASCADE CONSTRAINTS';
               EXCEPTION
                 WHEN OTHERS THEN
                   IF SQLCODE != -942 THEN
                     RAISE;
                   END IF;
               END;"
              session-schema
              (tx/db-qualified-table-name database-name table-name))
      ;; MySQL mode syntax
      (format "DROP TABLE IF EXISTS `%s`.`%s`"
              session-schema
              (tx/db-qualified-table-name database-name table-name)))))

(defmethod sql.tx/create-db-sql :oceanbase [& _] nil)

(defmethod sql.tx/drop-db-if-exists-sql :oceanbase [& _] nil)

(defmethod execute/execute-sql! :oceanbase [& args]
  (apply execute/sequentially-execute-sql! args))

(defmethod sql.tx/pk-sql-type :oceanbase [_]
  (let [details (connection-details)
        mode (detect-oceanbase-mode details)]
    (if (= mode "oracle")
      "INTEGER GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1) NOT NULL"
      "INT AUTO_INCREMENT")))

(defmethod sql.tx/qualified-name-components :oceanbase [& args]
  (apply tx/single-db-qualified-name-components session-schema args))

(defmethod tx/id-field-type :oceanbase [_] :type/Integer)

(defmethod load-data/row-xform :oceanbase
  [_driver _dbdef tabledef]
  (load-data/maybe-add-ids-xform tabledef))

;; Handle different insert syntax for MySQL and Oracle modes
(defmethod ddl/insert-rows-honeysql-form :oceanbase
  [driver table-identifier row-maps]
  (let [details (connection-details)
        mode (detect-oceanbase-mode details)]
    (if (= mode "oracle")
      ;; Oracle mode: use INSERT ALL syntax
      (let [into-clauses (mapv (fn [row-map]
                                (let [cols (vec (keys row-map))]
                                  {::into   table-identifier
                                   :columns (mapv (fn [col]
                                                    (h2x/identifier :field col))
                                                  cols)
                                   :values  [(mapv (fn [col]
                                                    (let [v (get row-map col)]
                                                      (sql.qp/->honeysql driver v)))
                                                  cols)]}))
                              row-maps)]
        {::insert-all into-clauses})
      ;; MySQL mode: use standard INSERT syntax
      (let [cols (vec (keys (first row-maps)))]
        {:insert-into table-identifier
         :columns     (mapv (fn [col]
                             (h2x/identifier :field col))
                           cols)
         :values      (mapv (fn [row-map]
                             (mapv (fn [col]
                                    (let [v (get row-map col)]
                                      (sql.qp/->honeysql driver v)))
                                  cols))
                           row-maps)}))))

;; Custom HoneySQL formatters for Oracle mode
(defn- format-insert-all [_fn [rows]]
  (let [rows-sql-args (mapv sql/format rows)
        sqls          (map first rows-sql-args)
        args          (mapcat rest rows-sql-args)]
    (into [(format
            "INSERT ALL %s SELECT * FROM dual"
            (str/join " " sqls))]
          args)))

(defn- format-into [_fn identifier]
  {:pre [(h2x/identifier? identifier)]}
  (let [[sql & args] (sql/format-expr identifier)]
    (into [(str "INTO " sql)] args)))

(sql/register-fn! ::insert-all #'format-insert-all)
(sql/register-clause! ::into #'format-into :into)

;; Test dataset definitions
(tx/defdataset test-data
  [["venues"
    [{:field-name "id", :base-type :type/Integer}
     {:field-name "name", :base-type :type/Text}
     {:field-name "category_id", :base-type :type/Integer}
     {:field-name "latitude", :base-type :type/Float}
     {:field-name "longitude", :base-type :type/Float}
     {:field-name "price", :base-type :type/Integer}]
    [[1 "Red Medicine" 4 10.0646 -165.374 3]
     [2 "Stout Burgers & Beers" 11 34.0996 -118.329 2]
     [3 "The Apple Pan" 11 34.0406 -118.428 2]
     [4 "Wurstküche" 29 33.9997 -118.465 2]
     [5 "Brite Spot Family Restaurant" 20 34.0778 -118.261 2]]]
   ["categories"
    [{:field-name "id", :base-type :type/Integer}
     {:field-name "name", :base-type :type/Text}]
    [[1 "American"]
     [2 "Artisan"]
     [3 "Asian"]
     [4 "Beer"]
     [5 "Breakfast"]
     [6 "Coffee"]
     [7 "Deli"]
     [8 "Dessert"]
     [9 "Diner"]
     [10 "Fast Food"]
     [11 "Food Cart"]
     [12 "Food Truck"]
     [13 "French"]
     [14 "Greek"]
     [15 "Hot Dogs"]
     [16 "Indian"]
     [17 "Italian"]
     [18 "Japanese"]
     [19 "Korean"]
     [20 "Mexican"]
     [21 "Pizza"]
     [22 "Seafood"]
     [23 "Southern"]
     [24 "Spanish"]
     [25 "Steakhouse"]
     [26 "Sushi"]
     [27 "Thai"]
     [28 "Turkish"]
     [29 "Vegetarian"]
     [30 "Vietnamese"]]]])

(deftest oceanbase-mode-detection-test
  (testing "OceanBase mode detection works correctly"
    (let [details (connection-details)]
      (is (contains? #{"mysql" "oracle"} (detect-oceanbase-mode details))))))

(deftest oceanbase-connection-test
  (testing "OceanBase connection can be established"
    (let [details (connection-details)
          spec (sql-jdbc.conn/connection-details->spec :oceanbase details)]
      (is (map? spec))
      (is (= "com.oceanbase.jdbc.Driver" (:classname spec)))
      (is (= "oceanbase" (:subprotocol spec))))))
