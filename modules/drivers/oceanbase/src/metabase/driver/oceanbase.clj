(ns metabase.driver.oceanbase
  (:require
   [clojure.java.jdbc :as jdbc]
   [clojure.string :as str]
   [honey.sql :as sql]
   [metabase.driver :as driver]
   [metabase.driver.common :as driver.common]
   [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]
   [metabase.driver.sql-jdbc.sync :as sql-jdbc.sync]
   [metabase.driver.sql-jdbc.sync.common :as sql-jdbc.sync.common]
   [metabase.driver.sql-jdbc.common :as sql-jdbc.common]
   [metabase.driver-api.core :as driver-api]
   [clojure.set :as set]
   [metabase.driver.sql-jdbc.sync.describe-table :as sql-jdbc.describe-table]
   [metabase.driver.sql-jdbc.sync.interface :as sql-jdbc.sync.interface]
   [metabase.driver.sql.query-processor :as sql.qp]
   [metabase.driver.sql.query-processor.empty-string-is-null :as sql.qp.empty-string-is-null]
   [metabase.util :as u]
   [metabase.util.honey-sql-2 :as h2x]
   [metabase.util.log :as log]
   [metabase.util.malli :as mu]
   [metabase.util.malli.registry :as mr])
  (:import [java.sql
            Connection
            DatabaseMetaData
            ResultSet]))

(set! *warn-on-reflection* true)

(driver/register! :oceanbase, :parent #{:sql-jdbc
                                        ::sql.qp.empty-string-is-null/empty-string-is-null})

(defmethod metabase.app-db.spec/spec :oceanbase
  [_ {:keys [host port db]
      :or   {host "localhost", port 2881, db ""}
      :as   opts}]
  (merge
   {:classname   "com.oceanbase.jdbc.Driver"
    :subprotocol "oceanbase"
    :subname     (metabase.app-db.spec/make-subname host (or port 2881) db)}
   (dissoc opts :host :port :db)))

(def supported-features
  {:datetime-diff           true
                              :expression-literals     true
                              :now                     true
                              :identifiers-with-spaces true
                              :convert-timezone        true
                              :expressions/date        true
                              :describe-fields         true
   :database-routing        false})

(doseq [[feature supported?] supported-features]
  (defmethod driver/database-supports? [:oceanbase feature] [_driver _feature _db] supported?))

(def ^:private mode-cache (atom {}))

(defn- get-connection-key [spec]
  (str (:host spec) ":" (:port spec) "/" (:dbname spec)))

(defn- get-mode [spec]
  (let [conn-key (get-connection-key spec)]
    (if-let [cached-mode (@mode-cache conn-key)]
      cached-mode
      (try
        (let [query "SELECT @@ob_compatibility_mode as mode"
              result (jdbc/query spec [query] {:as-arrays? false :max-rows 1})
              mode (if (seq result)
                     (let [value (str/lower-case (get-in (first result) [:mode]))]
                       (if (str/includes? value "oracle") "oracle" "mysql"))
                     "mysql")]
          (swap! mode-cache assoc conn-key mode)
          (log/debug "Detected OceanBase mode:" mode "for connection:" conn-key)
          mode)
        (catch Exception e
          (log/warn "Failed to detect OceanBase mode, defaulting to mysql. Error:" (.getMessage e))
          (swap! mode-cache assoc conn-key "mysql")
          "mysql")))))

(defn- clear-mode-cache! []
  "Clear the OceanBase mode cache. Useful for testing or when connection details change."
  (reset! mode-cache {}))



(def ^:private database-type->base-type
  (sql-jdbc.sync/pattern-based-database-type->base-type
   [;; MySQL mode types
    [#"BIGINT"      :type/BigInteger]
    [#"BINARY"      :type/*]
    [#"BIT"         :type/Boolean]
    [#"BLOB"        :type/*]
    [#"CHAR"        :type/Text]
    [#"DATE"        :type/Date]
    [#"DATETIME"    :type/DateTime]
    [#"DECIMAL"     :type/Decimal]
    [#"DOUBLE"      :type/Float]
    [#"ENUM"        :type/Text]
    [#"FLOAT"       :type/Float]
    [#"INT"         :type/Integer]
    [#"INTEGER"     :type/Integer]
    [#"JSON"        :type/JSON]
    [#"LONGBLOB"    :type/*]
    [#"LONGTEXT"    :type/Text]
    [#"MEDIUMBLOB"  :type/*]
    [#"MEDIUMINT"   :type/Integer]
    [#"MEDIUMTEXT"  :type/Text]
    [#"NUMERIC"     :type/Decimal]
    [#"REAL"        :type/Float]
    [#"SET"         :type/Text]
    [#"SMALLINT"    :type/Integer]
    [#"TEXT"        :type/Text]
    [#"TIME"        :type/Time]
    [#"TIMESTAMP"   :type/DateTime]
    [#"TINYBLOB"    :type/*]
    [#"TINYINT"     :type/Integer]
    [#"TINYTEXT"    :type/Text]
    [#"VARBINARY"   :type/*]
    [#"VARCHAR"     :type/Text]
    [#"YEAR"        :type/Integer]
    ;; Oracle mode types
    [#"ANYDATA"     :type/*]
    [#"ANYTYPE"     :type/*]
    [#"ARRAY"       :type/*]
    [#"BFILE"       :type/*]
    [#"BLOB"        :type/*]
    [#"CLOB"        :type/Text]
    [#"DATE"        :type/DateTime]
    [#"DOUBLE"      :type/Float]
    [#"FLOAT"       :type/Float]
    [#"INTERVAL"    :type/DateTime]
    [#"LONG RAW"    :type/*]
    [#"LONG"        :type/Text]
    [#"NUMBER"      :type/Decimal]
    [#"RAW"         :type/*]
    [#"REAL"        :type/Float]
    [#"REF"         :type/*]
    [#"ROWID"       :type/*]
    [#"STRUCT"      :type/*]
    [#"TIMESTAMP(\(\d\))? WITH TIME ZONE"       :type/DateTimeWithTZ]
    [#"TIMESTAMP(\(\d\))? WITH LOCAL TIME ZONE" :type/DateTimeWithLocalTZ]
    [#"TIMESTAMP"   :type/DateTime]
    [#"VARCHAR2"    :type/Text]
    [#"XML"         :type/*]]))

(defmethod sql-jdbc.sync/database-type->base-type :oceanbase
  [_ column-type]
  (database-type->base-type column-type))



(defmethod sql.qp/quote-style :oceanbase
  [driver]
  :oracle)

(defmethod driver/db-start-of-week :oceanbase
  [_driver]
  :sunday)

(mr/def ::details
  "OceanBase database details map."
  [:map
   [:use-connection-uri            {:optional true} [:maybe boolean?]]
   [:connection-uri                {:optional true} [:maybe string?]]
   [:host                          {:optional true} [:maybe string?]]
   [:port                          {:optional true} [:maybe integer?]]
   [:user                          {:optional true} [:maybe string?]]
   [:password                      {:optional true} [:maybe string?]]
   [:db                            {:optional true} [:maybe string?]]
   [:schema-filters-type           {:optional true} [:enum "inclusion" "exclusion"]]
   [:schema-filters-patterns       {:optional true} [:maybe string?]]
   [:ssl                           {:optional true} [:maybe boolean?]]])

(def ^:private default-connection-args
  "Map of args for the OceanBase JDBC connection string."
  {:useUnicode           true
   :characterEncoding    "UTF-8"
   :autoReconnect        true
   :failOverReadOnly     false
   :maxReconnects        3
   :initialTimeout       2
   :connectTimeout       30000
   :socketTimeout        30000
   :serverTimezone       "UTC"
   :allowPublicKeyRetrieval true
   :useSSL               false
   :requireSSL           false
   :verifyServerCertificate false
   :trustServerCertificate false
   :useProxy             false
   :proxyHost            ""
   :proxyPort            ""
   :proxyUser            ""
   :proxyPassword        ""
   :autoCommit           true
   :readOnly             false
   :transactionIsolation "READ_COMMITTED"})

(defn- maybe-add-program-name-option [jdbc-spec additional-options-map]
  (let [set-prog-nm-fn (fn []
                         (let [prog-name (str/replace driver-api/mb-version-and-process-identifier "," "_")]
                           (assoc jdbc-spec :connectionAttributes (str "program_name:" prog-name))))]
    (if-let [conn-attrs (get additional-options-map "connectionAttributes")]
      (if (str/includes? conn-attrs "program_name")
        jdbc-spec
        (set-prog-nm-fn))
      (set-prog-nm-fn))))

(defmethod sql-jdbc.conn/connection-details->spec :oceanbase
  [_ {ssl? :ssl, :keys [additional-options ssl-cert host port user password db connection-uri use-connection-uri], :as details}]
  (let [addl-opts-map (sql-jdbc.common/additional-options->map additional-options :url "=" false)
        ;; Disable SSL by default unless explicitly set by user
        ssl?          (and ssl? (not= ssl? false))
        ssl-cert?     (and ssl? (some? ssl-cert))]
    (if (and use-connection-uri connection-uri)
      ;; If use-connection-uri is true and a complete JDBC URL is provided, use it directly
      (do
        (log/debug "OceanBase connection - Using provided JDBC URL:" (str/replace connection-uri password "***"))
        {:connection-uri connection-uri})
      ;; Otherwise build the JDBC URL
      (let [;; Ensure user and password are not nil
            user          (or user "")
            password      (or password "")
            ;; Build JDBC URL using OceanBase driver with proxy disabled settings
            jdbc-url (str "jdbc:oceanbase://" (or host "localhost") ":" (or port 2881) "/" (or db "")
                          "?user=" user
                          "&password=" password
                          "&useUnicode=true"
                          "&characterEncoding=UTF-8"
                          "&autoReconnect=true"
                          "&failOverReadOnly=false"
                          "&maxReconnects=3"
                          "&initialTimeout=2"
                          "&connectTimeout=30000"
                          "&socketTimeout=30000"
                          "&serverTimezone=UTC"
                          "&allowPublicKeyRetrieval=true"
                          "&useSSL=false"
                          "&requireSSL=false"
                          "&verifyServerCertificate=false"
                          "&trustServerCertificate=false"
                          "&useProxy=false"
                          "&proxyHost="
                          "&proxyPort="
                          "&proxyUser="
                          "&proxyPassword="
                          "&autoCommit=true"
                          "&readOnly=false"
                          "&transactionIsolation=READ_COMMITTED")]
        (log/debug "OceanBase connection - JDBC URL:" (str/replace jdbc-url password "***"))
        {:connection-uri jdbc-url}))))

(defmethod driver/can-connect? :oceanbase
  [driver details]
  (try
    (let [spec (sql-jdbc.conn/connection-details->spec driver details)]
      (log/debug "Testing OceanBase connection with spec:" (dissoc spec :password))
      (sql-jdbc.conn/can-connect-with-spec? spec))
    (catch Exception e
      (log/error e "Failed to connect to OceanBase database")
      false)))

(defn- format-mod [_fn [x y]]
  (let [[x-sql & x-args] (sql/format-expr x {:nested true})
        [y-sql & y-args] (sql/format-expr y {:nested true})]
    (into [(format "mod(%s, %s)" x-sql y-sql)]
          cat
          [x-args y-args])))

(sql/register-fn! ::mod #'format-mod)

(defn- trunc
  [format-template v]
  (-> [:trunc v (h2x/literal format-template)]
      (h2x/with-database-type-info "date")))

(defmethod sql.qp/->honeysql [:oceanbase :substring]
  [driver [_ arg start & [length]]]
  (if length
    [:substring (sql.qp/->honeysql driver arg) (sql.qp/->honeysql driver start) (sql.qp/->honeysql driver length)]
    [:substring (sql.qp/->honeysql driver arg) (sql.qp/->honeysql driver start)]))

(defmethod sql.qp/->honeysql [:oceanbase :concat]
  [driver [_ & args]]
  (transduce
   (map (partial sql.qp/->honeysql driver))
   (completing
    (fn [x y]
      (if x
        [:concat x y]
        y)))
   nil
   args))

(defmethod sql.qp/->honeysql [:oceanbase :regex-match-first]
  [driver [_ arg pattern]]
  [:regexp_substr (sql.qp/->honeysql driver arg) (sql.qp/->honeysql driver pattern)])

(defmethod sql.qp/->honeysql [:oceanbase ::sql.qp/cast-to-text]
  [driver [_ expr]]
  (sql.qp/->honeysql driver [::sql.qp/cast expr "varchar2(256)"]))

(defmethod sql.qp/date [:oceanbase :second-of-minute]
  [_driver _unit v]
  (let [t (h2x/->timestamp v)]
    (h2x/->integer [:floor [::h2x/extract :second t]])))

(defmethod sql.qp/date [:oceanbase :minute]           [_ _ v] (trunc :mi v))
(defmethod sql.qp/date [:oceanbase :minute-of-hour]   [_ _ v] [::h2x/extract :minute (h2x/->timestamp v)])
(defmethod sql.qp/date [:oceanbase :hour]             [_ _ v] (trunc :hh v))
(defmethod sql.qp/date [:oceanbase :hour-of-day]      [_ _ v] [::h2x/extract :hour (h2x/->timestamp v)])
(defmethod sql.qp/date [:oceanbase :day]              [_ _ v] (trunc :dd v))
(defmethod sql.qp/date [:oceanbase :day-of-month]     [_ _ v] [::h2x/extract :day v])
(defmethod sql.qp/date [:oceanbase :month]            [_ _ v] (trunc :month v))
(defmethod sql.qp/date [:oceanbase :month-of-year]    [_ _ v] [::h2x/extract :month v])
(defmethod sql.qp/date [:oceanbase :quarter]          [_ _ v] (trunc :q v))
(defmethod sql.qp/date [:oceanbase :year]             [_ _ v] (trunc :year v))
(defmethod sql.qp/date [:oceanbase :year-of-era]      [_ _ v] [::h2x/extract :year v])

(defmethod sql.qp/date [:oceanbase :week]
  [driver _ v]
  (sql.qp/adjust-start-of-week driver (partial trunc :day) v))

(defmethod sql.qp/date [:oceanbase :week-of-year-iso]
  [_ _ v]
  (h2x/->integer [:to_char v (h2x/literal :iw)]))

(defmethod sql.qp/date [:oceanbase :day-of-year]
  [driver _ v]
  (h2x/inc (h2x/- (sql.qp/date driver :day v) (trunc :year v))))

(defmethod sql.qp/date [:oceanbase :quarter-of-year]
  [driver _ v]
  (h2x// (h2x/+ (sql.qp/date driver :month-of-year (sql.qp/date driver :quarter v))
                2)
         3))

(defmethod sql.qp/date [:oceanbase :day-of-week]
  [driver _ v]
  (sql.qp/adjust-day-of-week
   driver
   (h2x/->integer [:to_char v (h2x/literal :d)])
   (driver.common/start-of-week-offset driver)
   (fn mod-fn [& args]
     (into [::mod] args))))

(defmethod sql.qp/apply-top-level-clause [:oceanbase :limit]
  [driver _ honeysql-query {value :limit}]
  {:pre [(number? value)]}
  ((get-method sql.qp/apply-top-level-clause [:sql-jdbc :limit]) driver :limit honeysql-query {:limit value}))

(defmethod sql.qp/apply-top-level-clause [:oceanbase :page]
  [driver _ honeysql-query {{:keys [items page]} :page}]
  {:pre [(number? items) (number? page)]}
  ((get-method sql.qp/apply-top-level-clause [:sql-jdbc :page]) driver :page honeysql-query {:page {:items items :page page}}))

(defmethod driver/humanize-connection-error-message :oceanbase
  [_ message]
  (cond
    (str/includes? message "Connection refused")
    "Unable to connect to OceanBase. Please check your host, port, and ensure the database is running."
    (str/includes? message "Access denied")
    "Invalid username or password. Please check your credentials."
    (str/includes? message "Unknown database")
    "Database not found. Please check your database name."
    (str/includes? message "(or sid service-name)")
    "You must specify the SID and/or the Service Name."
    :else
    message))

(defmethod sql-jdbc.sync/excluded-schemas :oceanbase
  [_]
  #{"information_schema" "mysql" "performance_schema" "sys" "oceanbase"
    "ANONYMOUS" "APEX_040200" "APPQOSSYS" "AUDSYS" "CTXSYS" "DBSNMP" "DIP" "DVSYS"
    "GSMADMIN_INTERNAL" "GSMCATUSER" "GSMUSER" "LBACSYS" "MDSYS" "OLAPSYS" "ORDDATA"
    "ORDSYS" "OUTLN" "RDSADMIN" "SYS" "SYSBACKUP" "SYSDG" "SYSKM" "SYSTEM" "WMSYS"
    "XDB" "XS$NULL"})

(defmethod driver/escape-entity-name-for-metadata :oceanbase
  [_ entity-name]
  (str/replace entity-name "/" "//"))

(defmethod sql-jdbc.describe-table/get-table-pks :oceanbase
  [_driver ^Connection conn _db-name-or-nil table]
  (let [^DatabaseMetaData metadata (.getMetaData conn)]
    (into [] (sql-jdbc.sync.common/reducible-results
              #(.getPrimaryKeys metadata nil nil (:name table))
              (fn [^ResultSet rs] #(.getString rs "COLUMN_NAME"))))))

(defmethod sql-jdbc.sync.interface/filtered-syncable-schemas :oceanbase
  [_driver conn _metadata _schema-inclusion-filters _schema-exclusion-filters]
  (try
    (let [test-spec {:connection conn}
          mode-query "SELECT @@ob_compatibility_mode as mode"
          mode-result (jdbc/query test-spec [mode-query] {:as-arrays? false :max-rows 1})
          mode (if (seq mode-result)
                 (let [value (str/lower-case (get-in (first mode-result) [:mode]))]
                   (if (str/includes? value "oracle") "oracle" "mysql"))
                 "mysql")]
      (log/debug "OceanBase schema filtering - detected mode:" mode)
      (case mode
        "oracle"
        (let [user-query "SELECT USER as user FROM DUAL"
              user-result (jdbc/query test-spec [user-query] {:as-arrays? false :max-rows 1})
                current-user (when (seq user-result)
                              (get-in (first user-result) [:user]))]
            (if current-user
                  [current-user]
                  []))
        "mysql"
        []
        []))
    (catch Exception e
      (log/warn "Error in OceanBase schema filtering, returning empty list. Error:" (.getMessage e))
      [])))

(defn- build-oracle-sql [table-names]
  (let [base-sql "SELECT c.column_name as name,
                         c.column_id - 1 as database_position,
                         c.table_name as table_name,
                         UPPER(c.data_type) as database_type,
                         CASE WHEN c.nullable = 'Y' THEN 'false' ELSE 'true' END as database_required,
                         'false' as database_is_auto_increment,
                         CASE WHEN pk.column_name IS NOT NULL THEN 'true' ELSE 'false' END as pk_field,
                         cc.comments as field_comment
                  FROM user_tab_columns c
                  LEFT JOIN user_col_comments cc ON (c.table_name = cc.table_name AND c.column_name = cc.column_name)
                  LEFT JOIN (
                    SELECT pcc.table_name, pcc.column_name
                    FROM user_cons_columns pcc
                    JOIN user_constraints puc ON (pcc.constraint_name = puc.constraint_name)
                    WHERE puc.constraint_type = 'P'
                  ) pk ON (c.table_name = pk.table_name AND c.column_name = pk.column_name)"
        where-clause (when (seq table-names)
                       (str " WHERE LOWER(c.table_name) IN ("
                            (clojure.string/join "," (map #(str "'" % "'") (map u/lower-case-en table-names)))
                            ")"))
        final-sql (str base-sql where-clause " ORDER BY c.table_name, c.column_id")]
    [final-sql]))

(defn- build-mysql-sql [table-names db-name]
  (let [base-sql "SELECT c.column_name AS name,
                         c.ordinal_position - 1 AS database_position,
                         c.table_name AS table_name,
                         UPPER(c.data_type) AS database_type,
                         CASE WHEN c.is_nullable = 'NO' AND c.extra != 'auto_increment' THEN 'true' ELSE 'false' END AS database_required,
                         CASE WHEN c.extra = 'auto_increment' THEN 'true' ELSE 'false' END AS database_is_auto_increment,
                         CASE WHEN c.column_key = 'PRI' THEN 'true' ELSE 'false' END AS pk_field,
                         c.column_comment AS field_comment
                  FROM information_schema.columns c
                  WHERE c.table_schema = ?"
        where-clause (when (seq table-names)
                       (str " AND LOWER(c.table_name) IN ("
                            (clojure.string/join "," (map #(str "'" % "'") (map u/lower-case-en table-names)))
                            ")"))
        final-sql (str base-sql where-clause " ORDER BY c.table_name, c.ordinal_position")]
    [final-sql db-name]))

(defmethod sql-jdbc.sync/describe-fields-sql :oceanbase
  [driver & {:keys [table-names details]}]
  (try
    (let [spec (sql-jdbc.conn/connection-details->spec driver details)
          mode (get-mode spec)
          db-name (:dbname spec)]
      (log/debug "OceanBase describe fields - mode:" mode "db-name:" db-name)
    (case mode
        "oracle" (build-oracle-sql table-names)
        "mysql"  (build-mysql-sql table-names db-name)
        (build-oracle-sql table-names)))
    (catch Exception e
      (log/error e "Error in OceanBase describe-fields-sql, falling back to Oracle mode")
      (build-oracle-sql table-names))))

(defmethod sql-jdbc.sync/describe-fields-pre-process-xf :oceanbase
  [_driver _db & _args]
  (map (fn [col]
         (try
           (-> col
               (assoc :database-type (:database_type col))
               (assoc :database-position (:database_position col))
               (assoc :table-name (:table_name col))
               (assoc :database-required (= (:database_required col) "true"))
               (assoc :database-is-auto-increment (= (:database_is_auto_increment col) "true"))
               (assoc :pk? (= (:pk_field col) "true"))
               (assoc :field-comment (when-not (str/blank? (:field_comment col)) (:field_comment col)))
               (assoc :description (or (:field_comment col) ""))
               (dissoc :pk_field :database_type :database_position :table_name :field_comment :database_required :database_is_auto_increment))
      (catch Exception e
             (log/warn "Error processing field column:" col "Error:" (.getMessage e))
             col)))))

(defmethod driver/execute-reducible-query :oceanbase
  [driver query context respond]
  ((get-method driver/execute-reducible-query :sql-jdbc) driver query context respond))

(log/info "OceanBase driver loaded. Supports both MySQL and Oracle compatibility modes with automatic detection and caching. Connection parameters optimized for stability.")
