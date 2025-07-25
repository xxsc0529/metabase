(ns metabase.test.util
  "Helper functions and macros for writing unit tests."
  (:require
   [clojure.java.io :as io]
   [clojure.set :as set]
   [clojure.string :as str]
   [clojure.test :refer :all]
   [clojure.walk :as walk]
   [clojurewerkz.quartzite.scheduler :as qs]
   [colorize.core :as colorize]
   [environ.core :as env]
   [iapetos.operations :as ops]
   [iapetos.registry :as registry]
   [java-time.api :as t]
   [mb.hawk.assert-exprs.approximately-equal :as =?]
   [mb.hawk.parallel]
   [metabase.analytics.prometheus :as prometheus]
   [metabase.audit-app.core :as audit]
   [metabase.classloader.core :as classloader]
   [metabase.collections.models.collection :as collection]
   [metabase.config.core :as config]
   [metabase.content-verification.models.moderation-review :as moderation-review]
   [metabase.permissions.core :as perms]
   [metabase.permissions.models.data-permissions.graph :as data-perms.graph]
   [metabase.permissions.test-util :as perms.test-util]
   [metabase.premium-features.test-util :as premium-features.test-util]
   [metabase.query-processor.util :as qp.util]
   [metabase.search.core :as search]
   [metabase.settings.core :as setting]
   [metabase.settings.models.setting]
   [metabase.settings.models.setting.cache :as setting.cache]
   [metabase.task.core :as task]
   [metabase.task.impl :as task.impl]
   [metabase.test-runner.assert-exprs]
   [metabase.test.data :as data]
   [metabase.test.fixtures :as fixtures]
   [metabase.test.initialize :as initialize]
   [metabase.test.util.log]
   [metabase.timeline.models.timeline-event :as timeline-event]
   [metabase.util :as u]
   [metabase.util.files :as u.files]
   [metabase.util.json :as json]
   [metabase.util.random :as u.random]
   [methodical.core :as methodical]
   [toucan2.core :as t2]
   [toucan2.model :as t2.model]
   [toucan2.tools.before-update :as t2.before-update]
   [toucan2.tools.transformed :as t2.transformed]
   [toucan2.tools.with-temp :as t2.with-temp])
  (:import
   (java.io File FileInputStream)
   (java.net ServerSocket)
   (java.util Locale)
   (java.util.concurrent CountDownLatch TimeoutException)
   (org.eclipse.jetty.server Server)
   (org.quartz CronTrigger JobDetail JobKey Scheduler Trigger)
   (org.quartz.impl StdSchedulerFactory)))

(set! *warn-on-reflection* true)

(comment
  metabase.test-runner.assert-exprs/keep-me
  metabase.test.util.log/keep-me)

(use-fixtures :once (fixtures/initialize :db))

(defn boolean-ids-and-timestamps
  "Useful for unit test comparisons. Converts map keys found in `data` satisfying `pred` with booleans when not nil."
  ([data]
   (boolean-ids-and-timestamps
    (every-pred (some-fn keyword? string?)
                (some-fn #{:id :created_at :updated_at :last_analyzed :created-at :updated-at :field-value-id :field-id
                           :date_joined :date-joined :last_login :dimension-id :human-readable-field-id :timestamp
                           :entity_id}
                         #(str/ends-with? % "_id")
                         #(str/ends-with? % "_at")))
    data))

  ([pred data]
   (walk/prewalk (fn [maybe-map]
                   (if (map? maybe-map)
                     (reduce-kv (fn [acc k v]
                                  (if (pred k)
                                    (assoc acc k (some? v))
                                    (assoc acc k v)))
                                {} maybe-map)
                     maybe-map))
                 data)))

(defn- user-id [username]
  (classloader/require 'metabase.test.data.users)
  ((resolve 'metabase.test.data.users/user->id) username))

(defn- rasta-id [] (user-id :rasta))

(defn- default-updated-at-timestamped
  [x]
  (assoc x :updated_at (t/zoned-date-time)))

(defn- default-created-at-timestamped
  [x]
  (assoc x :created_at (t/zoned-date-time)))

(def ^:private default-timestamped
  (comp default-updated-at-timestamped default-created-at-timestamped))

(def ^:private with-temp-defaults-fns
  {:model/Card
   (fn [_] (default-timestamped
            {:creator_id             (rasta-id)
             :database_id            (data/id)
             :dataset_query          {}
             :display                :table
             :entity_id              (u/generate-nano-id)
             :name                   (u.random/random-name)
             :visualization_settings {}}))

   :model/Collection
   (fn [_] (default-created-at-timestamped {:name (u.random/random-name)}))

   :model/Action
   (fn [_] {:creator_id (rasta-id)})

   :model/Channel
   (fn [_] (default-timestamped
            {:name    (u.random/random-name)
             :type    "channel/metabase-test"
             :details {}}))

   :model/ChannelTemplate
   (fn [_] (default-timestamped
            {:name         (u.random/random-name)
             :channel_type "channel/metabase-test"}))

   :model/Dashboard
   (fn [_] (default-timestamped
            {:creator_id (rasta-id)
             :name       (u.random/random-name)}))

   :model/DashboardCard
   (fn [_] (default-timestamped
            {:row    0
             :col    0
             :size_x 4
             :size_y 4}))

   :model/DashboardCardSeries
   (constantly {:position 0})

   :model/DashboardTab
   (fn [_]
     (default-timestamped
      {:name     (u.random/random-name)
       :position 0}))

   :model/Database
   (fn [_] (default-timestamped
            {:details                     {}
             :engine                      :h2
             :is_sample                   false
             :name                        (u.random/random-name)
             :metadata_sync_schedule      "0 50 * * * ? *"
             :cache_field_values_schedule "0 50 0 * * ? *"
             :settings                    {:database-source-dataset-name "test-data"}}))

   :model/Dimension
   (fn [_] (default-timestamped
            {:name (u.random/random-name)
             :type "internal"}))

   :model/Field
   (fn [_] (default-timestamped
            {:database_type "VARCHAR"
             :base_type     :type/Text
             :name          (u.random/random-name)
             :position      1
             :table_id      (data/id :checkins)}))

   :model/LoginHistory
   (fn [_] {:device_id          "129d39d1-6758-4d2c-a751-35b860007002"
            :device_description "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML like Gecko) Chrome/89.0.4389.86 Safari/537.36"
            :ip_address         "0:0:0:0:0:0:0:1"
            :timestamp          (t/zoned-date-time)})

   :model/NativeQuerySnippet
   (fn [_] (default-timestamped
            {:creator_id (user-id :crowberto)
             :name       (u.random/random-name)
             :content    "1 = 1"}))

   :model/Notification
   (fn [_] (default-timestamped
            {:payload_type :notification/system-event
             :active       true}))

   :model/NotificationCard
   (fn [_] (default-timestamped
            {:send_once      false
             :send_condition :has_result}))

   :model/NotificationSubscription
   (fn [_] (default-created-at-timestamped
            {}))

   :model/QueryExecution
   (fn [_] {:hash         (qp.util/query-hash {})
            :running_time 1
            :result_rows  1
            :native       false
            :executor_id  nil
            :card_id      nil
            :context      :ad-hoc})

   :model/PersistedInfo
   (fn [_] {:question_slug (u.random/random-name)
            :query_hash    (u.random/random-hash)
            :definition    {:table-name        (u.random/random-name)
                            :field-definitions (repeatedly
                                                4
                                                #(do {:field-name (u.random/random-name) :base-type "type/Text"}))}
            :table_name    (u.random/random-name)
            :active        true
            :state         "persisted"
            :refresh_begin (t/zoned-date-time)
            :created_at    (t/zoned-date-time)
            :creator_id    (rasta-id)})

   :model/PermissionsGroup
   (fn [_] {:name (u.random/random-name)})

   :model/PermissionsGroupMembership
   (fn [_] {:__test-only-sigil-allowing-direct-insertion-of-permissions-group-memberships true})

   :model/Pulse
   (fn [_] (default-timestamped
            {:creator_id (rasta-id)
             :name       (u.random/random-name)}))

   :model/PulseCard
   (fn [_] {:position    0
            :include_csv false
            :include_xls false})

   :model/PulseChannel
   (fn [_] (default-timestamped
            {:channel_type  :email
             :details       {}
             :schedule_type :daily
             :schedule_hour 15}))

   :model/Revision
   (fn [_] {:user_id      (rasta-id)
            :is_creation  false
            :is_reversion false
            :timestamp    (t/zoned-date-time)})

   :model/Segment
   (fn [_] (default-timestamped
            {:creator_id  (rasta-id)
             :definition  {}
             :description "Lookin' for a blueberry"
             :name        "Toucans in the rainforest"
             :table_id    (data/id :checkins)}))

   ;; TODO - `with-temp` doesn't return `Sessions`, probably because their ID is a string?
   ;; Tech debt issue: #39329

   :model/Table
   (fn [_] (default-timestamped
            {:db_id  (data/id)
             :active true
             :name   (u.random/random-name)}))

   :model/TaskHistory
   (fn [_]
     (let [started (t/zoned-date-time)
           ended   (t/plus started (t/millis 10))]
       {:db_id      (data/id)
        :task       (u.random/random-name)
        :started_at started
        :status     :success
        :ended_at   ended
        :duration   (.toMillis (t/duration started ended))}))

   :model/Timeline
   (fn [_]
     (default-timestamped
      {:name       "Timeline of bird squawks"
       :default    false
       :icon       timeline-event/default-icon
       :creator_id (rasta-id)}))

   :model/TimelineEvent
   (fn [_]
     (default-timestamped
      {:name         "default timeline event"
       :icon         timeline-event/default-icon
       :timestamp    (t/zoned-date-time)
       :timezone     "US/Pacific"
       :time_matters true
       :creator_id   (rasta-id)}))

   :model/User
   (fn [_] {:first_name  (u.random/random-name)
            :last_name   (u.random/random-name)
            :email       (u.random/random-email)
            :password    (u.random/random-name)
            :date_joined (t/zoned-date-time)
            :updated_at  (t/zoned-date-time)})})

(defn- set-with-temp-defaults! []
  (doseq [[model defaults-fn] with-temp-defaults-fns]
    (methodical/defmethod t2.with-temp/with-temp-defaults model
      [model]
      (defaults-fn model))))

(set-with-temp-defaults!)

;;; ------------------------------------------------- Other Util Fns -------------------------------------------------

(defn obj->json->obj
  "Convert an object to JSON and back again. This can be done to ensure something will match its serialized +
  deserialized form, e.g. keywords that aren't map keys, record types vs. plain map types, or timestamps vs ISO-8601
  strings:

     (obj->json->obj {:type :query}) -> {:type \"query\"}"
  [obj]
  (json/decode+kw (json/encode obj)))

(defn- ->lisp-case-keyword [s]
  (-> (name s)
      (str/replace #"_" "-")
      u/lower-case-en
      keyword))

(defn do-with-temp-env-var-value!
  "Impl for [[with-temp-env-var-value!]] macro."
  [env-var-keyword value thunk]
  (mb.hawk.parallel/assert-test-is-not-parallel "with-temp-env-var-value!")
  ;; app DB needs to be initialized if we're going to play around with the Settings cache.
  (initialize/initialize-if-needed! :db)
  (let [value (str value)]
    (testing (colorize/blue (format "\nEnv var %s = %s\n" env-var-keyword (pr-str value)))
      (try
        ;; temporarily override the underlying environment variable value
        (with-redefs [env/env (assoc env/env env-var-keyword value)]
          ;; flush the Setting cache so it picks up the env var value for the Setting (if applicable)
          (setting.cache/restore-cache!)
          (thunk))
        (finally
          ;; flush the cache again so the original value of any env var Settings get restored
          (setting.cache/restore-cache!))))))

(defmacro with-temp-env-var-value!
  "Temporarily override the value of one or more environment variables and execute `body`. Resets the Setting cache so
  any env var Settings will see the updated value, and resets the cache again at the conclusion of `body` so the
  original values are restored.

    (with-temp-env-var-value! [mb-send-email-on-first-login-from-new-device \"FALSE\"]
      ...)"
  [[env-var value & more :as bindings] & body]
  {:pre [(vector? bindings) (even? (count bindings))]}
  `(do-with-temp-env-var-value!
    ~(->lisp-case-keyword env-var)
    ~value
    (fn [] ~@(if (seq more)
               [`(with-temp-env-var-value! ~(vec more) ~@body)]
               body))))

(setting/defsetting with-temp-env-var-value-test-setting
  "Setting for the `with-temp-env-var-value-test` test."
  :visibility :internal
  :setter     :none
  :default    "abc")

(deftest with-temp-env-var-value-test
  (is (= "abc"
         (with-temp-env-var-value-test-setting)))
  (with-temp-env-var-value! [mb-with-temp-env-var-value-test-setting "def"]
    (testing "env var value"
      (is (= "def"
             (env/env :mb-with-temp-env-var-value-test-setting))))
    (testing "Setting value"
      (is (= "def"
             (with-temp-env-var-value-test-setting)))))
  (testing "original value should be restored"
    (testing "env var value"
      (is (= nil
             (env/env :mb-with-temp-env-var-value-test-setting))))
    (testing "Setting value"
      (is (= "abc"
             (with-temp-env-var-value-test-setting)))))

  (testing "override multiple env vars"
    (with-temp-env-var-value! [some-fake-env-var 123, "ANOTHER_FAKE_ENV_VAR" "def"]
      (testing "Should convert values to strings"
        (is (= "123"
               (:some-fake-env-var env/env))))
      (testing "should handle CAPITALS/SNAKE_CASE"
        (is (= "def"
               (:another-fake-env-var env/env))))))

  (testing "validation"
    (are [form] (thrown?
                 clojure.lang.Compiler$CompilerException
                 (macroexpand form))
      (list `with-temp-env-var-value! '[a])
      (list `with-temp-env-var-value! '[a b c]))))

(defn- upsert-raw-setting!
  [original-value setting-k value]
  (if original-value
    (t2/update! :model/Setting setting-k {:value value})
    (t2/insert! :model/Setting :key setting-k :value value))
  (setting.cache/restore-cache!))

(defn- restore-raw-setting!
  [original-value setting-k]
  (if original-value
    (t2/update! :model/Setting setting-k {:value original-value})
    (t2/delete! :model/Setting :key setting-k))
  (setting.cache/restore-cache!))

(defn do-with-temporary-setting-value!
  "Temporarily set the value of the Setting named by keyword `setting-k` to `value` and execute `f`, then re-establish
  the original value. This works much the same way as [[binding]].

  If an env var value is set for the setting, this acts as a wrapper around [[do-with-temp-env-var-value!]].

  If `raw-setting?` is `true`, this works like [[with-temp*]] against the `Setting` table, but it ensures no exception
  is thrown if the `setting-k` already exists.

  If `skip-init?` is `true`, this will skip any `:init` function on the setting, and return `nil` if it is not defined.

  Prefer the macro [[with-temporary-setting-values]] or [[with-temporary-raw-setting-values]] over using this function
  directly."
  [setting-k value thunk & {:keys [raw-setting? skip-init?]}]
  ;; plugins have to be initialized because changing `report-timezone` will call driver methods
  (mb.hawk.parallel/assert-test-is-not-parallel "do-with-temporary-setting-value")
  (initialize/initialize-if-needed! :db :plugins)
  (let [setting-k     (name setting-k)
        setting       (try
                        (#'setting/resolve-setting setting-k)
                        (catch Exception e
                          (when-not raw-setting?
                            (throw e))))]
    (if (and (not raw-setting?) (setting/env-var-value setting-k))
      (do-with-temp-env-var-value! (setting/setting-env-map-name setting-k) value thunk)
      (let [original-value (if raw-setting?
                             (t2/select-one-fn :value :model/Setting :key setting-k)
                             (if skip-init?
                               (setting/read-setting setting-k)
                               (setting/get setting-k)))]
        (try
          (try
            (if raw-setting?
              (upsert-raw-setting! original-value setting-k value)
              ;; bypass the feature check when setting up mock data
              (with-redefs [metabase.settings.models.setting/has-feature? (constantly true)]
                (setting/set! setting-k value :bypass-read-only? true)))
            (catch Throwable e
              (throw (ex-info (str "Error in with-temporary-setting-values: " (ex-message e))
                              {:setting  setting-k
                               :location (symbol (name (:namespace setting)) (name setting-k))
                               :value    value}
                              e))))
          (testing (colorize/blue (format "\nSetting %s = %s\n" (keyword setting-k) (pr-str value)))
            (thunk))
          (finally
            (try
              (if raw-setting?
                (restore-raw-setting! original-value setting-k)
                ;; bypass the feature check when reset settings to the original value
                (with-redefs [metabase.settings.models.setting/has-feature? (constantly true)]
                  (setting/set! setting-k original-value :bypass-read-only? true)))
              (catch Throwable e
                (throw (ex-info (str "Error restoring original Setting value: " (ex-message e))
                                {:setting        setting-k
                                 :location       (symbol (name (:namespace setting)) setting-k)
                                 :original-value original-value}
                                e))))))))))

;;; TODO FIXME -- either rename this to `with-temporary-setting-values!` or fix it and make it thread-safe
#_{:clj-kondo/ignore [:metabase/test-helpers-use-non-thread-safe-functions]}
(defmacro with-temporary-setting-values
  "Temporarily bind the site-wide values of one or more `Settings`, execute body, and re-establish the original values.
  This works much the same way as `binding`.

     (with-temporary-setting-values [google-auth-auto-create-accounts-domain \"metabase.com\"]
       (google-auth-auto-create-accounts-domain)) -> \"metabase.com\"

  If an env var value is set for the setting, this will change the env var rather than the setting stored in the DB.
  To temporarily override the value of *read-only* env vars, use [[with-temp-env-var-value!]]."
  [[setting-k value & more :as bindings] & body]
  (assert (even? (count bindings)) "mismatched setting/value pairs: is each setting name followed by a value?")
  (if (empty? bindings)
    `(do ~@body)
    `(do-with-temporary-setting-value!
      ~(keyword setting-k) ~value
      (fn []
        (with-temporary-setting-values ~more
          ~@body)))))

;;; TODO FIXME -- either rename this to `with-temporary-raw-setting-values!` or fix it and make it thread-safe
#_{:clj-kondo/ignore [:metabase/test-helpers-use-non-thread-safe-functions]}
(defmacro with-temporary-raw-setting-values
  "Like [[with-temporary-setting-values]] but works with raw value and it allows settings that are not defined
  using [[metabase.settings.models.setting/defsetting]]."
  [[setting-k value & more :as bindings] & body]
  (assert (even? (count bindings)) "mismatched setting/value pairs: is each setting name followed by a value?")
  (if (empty? bindings)
    `(do ~@body)
    `(do-with-temporary-setting-value!
      ~(keyword setting-k) ~value
      (fn []
        (with-temporary-raw-setting-values ~more
          ~@body))
      :raw-setting? true)))

(defn do-with-discarded-setting-changes! [settings thunk]
  (initialize/initialize-if-needed! :db :plugins)
  ((reduce
    (fn [thunk setting-k]
      (fn []
        (let [value (setting/read-setting setting-k)]
          (do-with-temporary-setting-value! setting-k value thunk :skip-init? true))))
    thunk
    settings)))

;;; TODO FIXME -- either rename this to `with-discarded-setting-changes!` or fix it and make it thread-safe
#_{:clj-kondo/ignore [:metabase/test-helpers-use-non-thread-safe-functions]}
(defmacro discard-setting-changes
  "Execute `body` in a try-finally block, restoring any changes to listed `settings` to their original values at its
  conclusion.

    (discard-setting-changes [site-name]
      ...)"
  {:style/indent 1}
  [settings & body]
  `(do-with-discarded-setting-changes! ~(mapv keyword settings) (fn [] ~@body)))

(defn- maybe-merge-original-values
  "For some map columns like `Database.settings` or `User.settings`, merge the original values with the temp ones to
  preserve Settings that aren't explicitly overridden."
  [model column->unparsed-original-value column->temp-value]
  (letfn [(column-transform-fn [model column]
            (get-in (t2.transformed/transforms model) [column :out]))
          (parse-original-value [model column unparsed-original-value]
            (some-> unparsed-original-value
                    ((column-transform-fn model column))))
          (merge-original-value? [model column]
            (and (#{:model/Database :model/User} model)
                 (= column :settings)))
          (maybe-merge-original-value [model column unparsed-original-value temp-value]
            (if (merge-original-value? model column)
              (let [original-value (parse-original-value model column unparsed-original-value)]
                (merge original-value temp-value))
              temp-value))]
    (into {}
          (map (fn [[column temp-value]]
                 [column (maybe-merge-original-value model column (get column->unparsed-original-value column) temp-value)]))
          column->temp-value)))

(defn do-with-temp-vals-in-db
  "Implementation function for [[with-temp-vals-in-db]] macro. Prefer that to using this directly."
  [model object-or-id column->temp-value f]
  (mb.hawk.parallel/assert-test-is-not-parallel "with-temp-vals-in-db")
  ;; use low-level `query` and `execute` functions here, because Toucan `select` and `update` functions tend to do
  ;; things like add columns like `common_name` that don't actually exist, causing subsequent update to fail
  (let [model                  (t2.model/resolve-model model)
        original-column->value (t2/query-one {:select (keys column->temp-value)
                                              :from   [(t2/table-name model)]
                                              :where  [:= :id (u/the-id object-or-id)]})
        _                      (assert original-column->value
                                       (format "%s %d not found." (name model) (u/the-id object-or-id)))
        column->temp-value     (maybe-merge-original-values model original-column->value column->temp-value)]
    (try
      (t2/update! model (u/the-id object-or-id) column->temp-value)
      (f)
      (finally
        (t2/query-one
         {:update (t2/table-name model)
          :set    original-column->value
          :where  [:= :id (u/the-id object-or-id)]})))))

;;; TODO -- we can make this parallel test safe pretty easily by doing stuff inside a transaction
;;; unless [[metabase.test/test-helpers-set-global-values!]] is in effect
(defmacro with-temp-vals-in-db
  "Temporary set values for an `object-or-id` in the application database, execute `body`, and then restore the
  original values. This is useful for cases when you want to test how something behaves with slightly different values
  in the DB for 'permanent' rows (rows that live for the life of the test suite, rather than just a single test). For
  example, Database/Table/Field rows related to the test DBs can be temporarily tweaked in this way.

    ;; temporarily make Field 100 a FK to Field 200 and call (do-something)
    (with-temp-vals-in-db Field 100 {:fk_target_field_id 200, :semantic_type \"type/FK\"}
      (do-something))

  There is some special case behavior that merges existing values into the temp values for map columns such as
  `Database` or `User` `:settings` -- the existing Settings map is merged into the user-specified one, so only the
  Settings you explicitly specify are overridden. See [[maybe-merge-original-values]]."
  {:style/indent 3}
  [model object-or-id column->temp-value & body]
  `(do-with-temp-vals-in-db ~model ~object-or-id ~column->temp-value (fn [] ~@body)))

(defn postwalk-pred
  "Transform `form` by applying `f` to each node where `pred` returns true"
  [pred f form]
  (walk/postwalk (fn [node]
                   (if (pred node)
                     (f node)
                     node))
                 form))

(defn round-all-decimals
  "Uses [[postwalk-pred]] to crawl `data`, looking for any double values, will round any it finds"
  [decimal-place data]
  (postwalk-pred (some-fn double? decimal?)
                 #(u/round-to-decimals decimal-place %)
                 data))

;;; TODO -- we should generalize this to `let-testing` (`testing-let`?) that is a version of `let` that adds `testing`
;;; context
(defmacro let-url
  "Like normal `let`, but adds `testing` context with the `url` you've bound."
  {:style/indent 1}
  [[url-binding url] & body]
  `(let [url# ~url
         ~url-binding url#]
     (testing (str "\nGET /api/" url# "\n")
       ~@body)))

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                                   SCHEDULER                                                    |
;;; +----------------------------------------------------------------------------------------------------------------+

;; Various functions for letting us check that things get scheduled properly. Use these to put a temporary scheduler
;; in place and then check the tasks that get scheduled

(def in-memory-scheduler-thread-count 1)

(defn- in-memory-scheduler
  "An in-memory Quartz Scheduler separate from the usual database-backend one we normally use. Every time you call this
  it returns the same scheduler! So make sure you shut it down when you're done using it."
  ^org.quartz.impl.StdScheduler []
  (.getScheduler
   (StdSchedulerFactory.
    (doto (java.util.Properties.)
      (.setProperty StdSchedulerFactory/PROP_SCHED_INSTANCE_NAME (str `in-memory-scheduler))
      (.setProperty StdSchedulerFactory/PROP_JOB_STORE_CLASS (.getCanonicalName org.quartz.simpl.RAMJobStore))
      (.setProperty (str StdSchedulerFactory/PROP_THREAD_POOL_PREFIX ".threadCount") (str in-memory-scheduler-thread-count))))))

(defn do-with-unstarted-temp-scheduler! [thunk]
  (let [temp-scheduler (in-memory-scheduler)
        already-bound? (identical? @task.impl/*quartz-scheduler* temp-scheduler)]
    (if already-bound?
      (thunk)
      (try
        (assert (not (qs/started? temp-scheduler))
                "temp in-memory scheduler already started: did you use it elsewhere without shutting it down?")
        (binding [task.impl/*quartz-scheduler* (atom temp-scheduler)]
          (with-redefs [qs/initialize (constantly temp-scheduler)
                        ;; prevent shutting down scheduler during thunk because some custom migration shutdown scheduler
                        ;; after it's done, but we need the scheduler for testing
                        qs/shutdown   (constantly nil)]
            (thunk)))
        (finally
          (qs/shutdown temp-scheduler))))))

(defn do-with-temp-scheduler! [thunk]
  ;; not 100% sure we need to initialize the DB anymore since the temp scheduler is in-memory-only now.
  (classloader/the-classloader)
  (initialize/initialize-if-needed! :db)
  (do-with-unstarted-temp-scheduler!
   (^:once fn* []
     (qs/start @task.impl/*quartz-scheduler*)
     (thunk))))

(defmacro with-temp-scheduler!
  "Execute `body` with a temporary scheduler in place.
  This does not initialize the all the jobs for performance reasons, so make sure you init it yourself!

    (with-temp-scheduler
      (task.sync-databases/job-init) ;; init the jobs
      (do-something-to-schedule-tasks)
      ;; verify that the right thing happened
      (scheduler-current-tasks))"
  {:style/indent 0}
  [& body]
  `(do-with-temp-scheduler! (fn [] ~@body)))

(defn scheduler-current-tasks
  "Return information about the currently scheduled tasks (jobs+triggers) for the current scheduler. Intended so we
  can test that things were scheduled correctly."
  []
  (when-let [^Scheduler scheduler (task/scheduler)]
    (vec
     (sort-by
      :key
      (for [^JobKey job-key (.getJobKeys scheduler nil)]
        (let [^JobDetail job-detail (.getJobDetail scheduler job-key)
              triggers              (.getTriggersOfJob scheduler job-key)]
          {:description (.getDescription job-detail)
           :class       (.getJobClass job-detail)
           :key         (.getName job-key)
           :data        (into {} (.getJobDataMap job-detail))
           :triggers    (vec (for [^Trigger trigger triggers]
                               (merge
                                {:key (.getName (.getKey trigger))}
                                (when (instance? CronTrigger trigger)
                                  {:cron-schedule (.getCronExpression ^CronTrigger trigger)
                                   :data          (into {} (.getJobDataMap trigger))}))))}))))))

(defmulti with-model-cleanup-additional-conditions
  "Additional conditions that should be used to restrict which instances automatically get deleted by
  `with-model-cleanup`. Conditions should be a HoneySQL `:where` clause."
  {:arglists '([model])}
  identity)

(defmethod with-model-cleanup-additional-conditions :default
  [_]
  nil)

(defmethod with-model-cleanup-additional-conditions :model/Collection
  [_]
  ;; NEVER delete personal collections for the test users.
  [:or
   [:= :personal_owner_id nil]
   [:not-in :personal_owner_id (set (map (requiring-resolve 'metabase.test.data.users/user->id)
                                         @(requiring-resolve 'metabase.test.data.users/usernames)))]])

(defmethod with-model-cleanup-additional-conditions :model/User
  [_]
  ;; Don't delete the internal user
  [:not= :id config/internal-mb-user-id])

(defmethod with-model-cleanup-additional-conditions :model/Database
  [_]
  ;; Don't delete the audit database
  [:not= :id audit/audit-db-id])

(defmulti with-max-model-id-additional-conditions
  "Additional conditions applied to the query to find the max ID for a model prior to a test run. This can be used to
  exclude rows which intentionally use non-sequential IDs, like the internal user."
  {:arglists '([model])}
  t2/resolve-model)

(defmethod with-max-model-id-additional-conditions :default
  [_]
  nil)

(defmethod with-max-model-id-additional-conditions :model/User
  [_]
  [:not= :id config/internal-mb-user-id])

(defmethod with-max-model-id-additional-conditions :model/Database
  [_]
  [:not= :id audit/audit-db-id])

(defn- model->model&pk [model]
  (if (vector? model)
    model
    [model (first (t2/primary-keys model))]))

;; It is safe to call `search/reindex!` when we are in a `with-temp-index-table` scope.
#_{:clj-kondo/ignore [:metabase/test-helpers-use-non-thread-safe-functions]}
(defn do-with-model-cleanup [models f]
  {:pre [(sequential? models) (every?
                               ;; to support [[:model/Model :updated_at]] syntax
                               #(isa? (t2/resolve-model
                                       (if (sequential? %)
                                         (first %)
                                         %))
                                      :metabase/model)
                               models)]}
  (mb.hawk.parallel/assert-test-is-not-parallel "with-model-cleanup")
  (initialize/initialize-if-needed! :db)
  (let [models (map model->model&pk models)
        model->old-max-id (into {} (for [[model pk] models
                                         :let [conditions (with-max-model-id-additional-conditions model)]]
                                     [model (:max-id (t2/select-one [model [[:max pk] :max-id]]
                                                                    {:where (or conditions true)}))]))]
    (try
      (testing (str "\n" (pr-str (cons 'with-model-cleanup (map (comp name first) models))) "\n")
        (f))
      (finally
        (doseq [[model pk] models
                ;; might not have an old max ID if this is the first time the macro is used in this test run.
                :let  [old-max-id            (get model->old-max-id model)
                       max-id-condition      (if old-max-id [:> pk old-max-id] true)
                       additional-conditions (with-model-cleanup-additional-conditions model)]]
          (t2/query-one
           {:delete-from (t2/table-name model)
            :where       [:and max-id-condition additional-conditions]}))
        ;; TODO we don't (currently) have index update hooks on deletes, so we need this to ensure rollback happens.
        (search/reindex! {:in-place? true})))))

(defmacro with-model-cleanup
  "Execute `body`, then delete any *new* rows created for each model in `models`. Calls `delete!`, so if the model has
  defined any `pre-delete` behavior, that will be preserved.

  It's preferable to use `with-temp` instead, but you can use this macro if `with-temp` wouldn't work in your
  situation (e.g. when creating objects via the API).

    (with-model-cleanup [Card]
      (create-card-via-api!)
      (is (= ...)))

    (with-model-cleanup [[QueryCache :updated_at]]
      (created-query-cache!)
      (is cached?))

  Only works for models that have a numeric primary key e.g. `:id`."
  [models & body]
  `(do-with-model-cleanup ~models (fn [] ~@body)))

(deftest with-model-cleanup-test
  (testing "Make sure the with-model-cleanup macro actually works as expected"
    #_{:clj-kondo/ignore [:discouraged-var]}
    (t2.with-temp/with-temp [:model/Card other-card]
      (let [card-count-before (t2/count :model/Card)
            card-name         (u.random/random-name)]
        (with-model-cleanup [:model/Card]
          (t2/insert! :model/Card (-> other-card (dissoc :id :entity_id) (assoc :name card-name)))
          (testing "Card count should have increased by one"
            (is (= (inc card-count-before)
                   (t2/count :model/Card))))
          (testing "Card should exist"
            (is (t2/exists? :model/Card :name card-name))))
        (testing "Card should be deleted at end of with-model-cleanup form"
          (is (= card-count-before
                 (t2/count :model/Card)))
          (is (not (t2/exists? :model/Card :name card-name)))
          (testing "Shouldn't delete other Cards"
            (is (pos? (t2/count :model/Card)))))))))

(defn do-with-verified-cards!
  "Impl for [[with-verified-cards!]]."
  [card-or-ids thunk]
  (with-model-cleanup [:model/ModerationReview]
    (doseq [card-or-id card-or-ids]
      (doseq [status ["verified" nil "verified"]]
        ;; create multiple moderation review for a card, but the end result is it's still verified
        (moderation-review/create-review!
         {:moderated_item_id   (u/the-id card-or-id)
          :moderated_item_type "card"
          :moderator_id        ((requiring-resolve 'metabase.test.data.users/user->id) :rasta)
          :status              status})))
    (thunk)))

(defmacro with-verified-cards!
  "Execute the body with all `card-or-ids` verified."
  [card-or-ids & body]
  `(do-with-verified-cards! ~card-or-ids (fn [] ~@body)))

(deftest with-verified-cards-test
  #_{:clj-kondo/ignore [:discouraged-var]}
  (t2.with-temp/with-temp
    [:model/Card {card-id :id} {}]
    (with-verified-cards! [card-id]
      (is (=? #{{:moderated_item_id   card-id
                 :moderated_item_type :card
                 :most_recent         true
                 :status              "verified"}
                {:moderated_item_id   card-id
                 :moderated_item_type :card
                 :most_recent         false
                 :status              nil}
                {:moderated_item_id   card-id
                 :moderated_item_type :card
                 :most_recent         false
                 :status              "verified"}}
              (t2/select-fn-set #(select-keys % [:moderated_item_id :moderated_item_type :most_recent :status])
                                :model/ModerationReview
                                :moderated_item_id card-id
                                :moderated_item_type "card"))))
    (testing "everything is cleaned up after the macro"
      (is (= 0 (t2/count :model/ModerationReview
                         :moderated_item_id card-id
                         :moderated_item_type "card"))))))

(defn call-with-paused-query
  "This is a function to make testing query cancellation eaiser as it can be complex handling the multiple threads
  needed to orchestrate a query cancellation.

  This function takes `f` which is a function of 4 arguments:
     - query-thunk         - No-arg function that will invoke a query.
     - query promise       - Promise used to validate the query function was called.  Deliver something to this once the
                             query has started running
     - cancel promise      - Promise used to validate a cancellation function was called. Deliver something to this once
                             the query was successfully canceled.
     - pause query promise - Promise used to hang the query function, allowing cancellation. Wait for this to be
                             delivered to hang the query.

  `f` should return a future that can be canceled."
  [f]
  (let [called-cancel? (promise)
        called-query?  (promise)
        pause-query    (promise)
        query-thunk    (fn []
                         (data/run-mbql-query checkins
                           {:aggregation [[:count]]}))
        ;; When the query is ran via the datasets endpoint, it will run in a future. That future can be canceled,
        ;; which should cause an interrupt
        query-future   (f query-thunk called-query? called-cancel? pause-query)]
    ;; The cancelled-query? and called-cancel? timeouts are very high and are really just intended to
    ;; prevent the test from hanging indefinitely. It shouldn't be hit unless something is really wrong
    (when (= ::query-never-called (deref called-query? 10000 ::query-never-called))
      (throw (TimeoutException. "query should have been called by now.")))
    ;; At this point in time, the query is blocked, waiting for `pause-query` do be delivered. Cancel still should
    ;; not have been called yet.
    (assert (not (realized? called-cancel?)) "cancel still should not have been called yet.")
    ;; If we cancel the future, it should throw an InterruptedException, which should call the cancel
    ;; method on the prepared statement
    (future-cancel query-future)
    (when (= ::cancel-never-called (deref called-cancel? 10000 ::cancel-never-called))
      (throw (TimeoutException. "cancel should have been called by now.")))
    ;; This releases the fake query function so it finishes
    (deliver pause-query true)
    ::success))

(defmacro throw-if-called!
  "Redefines `fn-var` with a function that throws an exception if it's called"
  {:style/indent 1}
  [fn-symb & body]
  {:pre [(symbol? fn-symb)]}
  `(with-redefs [~fn-symb (fn [& ~'_]
                            (throw (RuntimeException. ~(format "%s should not be called!" fn-symb))))]
     ~@body))

(defn do-with-discarded-collections-perms-changes [collection-or-id f]
  (initialize/initialize-if-needed! :db)
  (let [read-path                   (perms/collection-read-path collection-or-id)
        readwrite-path              (perms/collection-readwrite-path collection-or-id)
        groups-with-read-perms      (t2/select-fn-set :group_id :model/Permissions :object read-path)
        groups-with-readwrite-perms (t2/select-fn-set :group_id :model/Permissions :object readwrite-path)]
    (mb.hawk.parallel/assert-test-is-not-parallel "with-discarded-collections-perms-changes")
    (try
      (f)
      (finally
        (t2/delete! :model/Permissions :object [:in #{read-path readwrite-path}])
        (doseq [group-id groups-with-read-perms]
          (perms/grant-collection-read-permissions! group-id collection-or-id))
        (doseq [group-id groups-with-readwrite-perms]
          (perms/grant-collection-readwrite-permissions! group-id collection-or-id))))))

(defmacro with-discarded-collections-perms-changes
  "Execute `body`; then, in a `finally` block, restore permissions to `collection-or-id` to what they were originally."
  [collection-or-id & body]
  `(do-with-discarded-collections-perms-changes ~collection-or-id (fn [] ~@body)))

(declare with-discard-model-updates!)

(defn do-with-discard-model-updates!
  "Impl for [[with-discard-model-updates!]]."
  [models thunk]
  (mb.hawk.parallel/assert-test-is-not-parallel "with-discard-model-changes")
  (if (= (count models) 1)
    (let [model             (first models)
          pk->original      (atom {})
          method-unique-key (str (random-uuid))
          before-method-fn  (fn [_model row]
                              (swap! pk->original assoc (u/the-id row) (t2/original row))
                              row)]
      (methodical/add-aux-method-with-unique-key! #'t2.before-update/before-update :before model before-method-fn method-unique-key)
      (try
        (thunk)
        (finally
          (methodical/remove-aux-method-with-unique-key! #'t2.before-update/before-update :before model method-unique-key)
          (doseq [[id original-val] @pk->original]
            (t2/update! model id original-val)))))
    (with-discard-model-updates! (rest models)
      (thunk))))

(defmacro with-discard-model-updates!
  "Exceute `body` and makes sure that every updates operation on `models` will be reverted."
  [models & body]
  (if (> (count models) 1)
    (let [[model & more] models]
      `(with-discard-model-updates! [~model]
         (with-discard-model-updates! [~@more]
           ~@body)))
    `(do-with-discard-model-updates! ~models (fn [] ~@body))))

(deftest with-discard-model-changes-test
  #_{:clj-kondo/ignore [:discouraged-var]}
  (t2.with-temp/with-temp
    [:model/Card      {card-id :id :as card} {:name "A Card"}
     :model/Dashboard {dash-id :id :as dash} {:name "A Dashboard"}]
    (let [count-aux-method-before (set (methodical/aux-methods t2.before-update/before-update :model/Card :before))]

      (testing "with single model"
        (with-discard-model-updates! [:model/Card]
          (t2/update! :model/Card card-id {:name "New Card name"})
          (testing "the changes takes affect inside the macro"
            (is (= "New Card name" (t2/select-one-fn :name :model/Card card-id)))))

        (testing "outside macro, the changes should be reverted"
          (is (= card (t2/select-one :model/Card card-id)))))

      (testing "with multiple models"
        (with-discard-model-updates! [:model/Card :model/Dashboard]
          (testing "the changes takes affect inside the macro"
            (t2/update! :model/Card card-id {:name "New Card name"})
            (is (= "New Card name" (t2/select-one-fn :name :model/Card card-id)))

            (t2/update! :model/Dashboard dash-id {:name "New Dashboard name"})
            (is (= "New Dashboard name" (t2/select-one-fn :name :model/Dashboard dash-id)))))

        (testing "outside macro, the changes should be reverted"
          (is (= (dissoc card :updated_at)
                 (dissoc (t2/select-one :model/Card card-id) :updated_at)))
          (is (= (dissoc dash :updated_at)
                 (dissoc (t2/select-one :model/Dashboard dash-id) :updated_at)))))

      (testing "make sure that we cleaned up the aux methods after"
        (is (= count-aux-method-before
               (set (methodical/aux-methods t2.before-update/before-update :model/Card :before))))))))

;;; TODO (Cam 6/17/25) -- these should have an `!` after them
(defn do-with-non-admin-groups-no-collection-perms [collection f]
  (mb.hawk.parallel/assert-test-is-not-parallel "with-non-admin-groups-no-collection-perms")
  (try
    (do-with-discarded-collections-perms-changes
     collection
     (fn []
       (t2/delete! :model/Permissions
                   :object [:in #{(perms/collection-read-path collection) (perms/collection-readwrite-path collection)}]
                   :group_id [:not= (u/the-id (perms/admin-group))])
       (f)))
    ;; if this is the default namespace Root Collection, then double-check to make sure all non-admin groups get
    ;; perms for it at the end. This is here mostly for legacy reasons; we can remove this but it will require
    ;; rewriting a few tests.
    (finally
      (when (and (:metabase.collections.models.collection.root/is-root? collection)
                 (not (:namespace collection)))
        (doseq [group-id (t2/select-pks-set :model/PermissionsGroup :id [:not= (u/the-id (perms/admin-group))])]
          (when-not (t2/exists? :model/Permissions :group_id group-id, :object "/collection/root/")
            (perms/grant-collection-readwrite-permissions! group-id collection/root-collection)))))))

(defmacro with-non-admin-groups-no-root-collection-perms
  "Temporarily remove Root Collection perms for all Groups besides the Admin group (which cannot have them removed). By
  default, all Groups have full readwrite perms for the Root Collection; use this macro to test situations where an
  admin has removed them.

  Only affects the Root Collection for the default namespace. Use
  [[with-non-admin-groups-no-root-collection-for-namespace-perms]] to do the same thing for the Root Collection of other
  namespaces."
  [& body]
  `(do-with-non-admin-groups-no-collection-perms collection/root-collection (fn [] ~@body)))

(defmacro with-non-admin-groups-no-root-collection-for-namespace-perms
  "Like `with-non-admin-groups-no-root-collection-perms`, but for the Root Collection of a non-default namespace."
  [collection-namespace & body]
  `(do-with-non-admin-groups-no-collection-perms
    (assoc collection/root-collection
           :namespace (name ~collection-namespace))
    (fn [] ~@body)))

(defmacro with-non-admin-groups-no-collection-perms
  "Temporarily remove perms for the provided collection for all Groups besides the Admin group (which cannot have them
  removed)."
  [collection & body]
  `(do-with-non-admin-groups-no-collection-perms ~collection (fn [] ~@body)))

(defn do-with-all-users-permission
  "Call `f` without arguments in a context where the ''All Users'' group
  is granted the permission specified by `permission-path`.

  For most use cases see the macro [[with-all-users-permission]]."
  [permission-path f]
  #_{:clj-kondo/ignore [:discouraged-var]}
  (t2.with-temp/with-temp [:model/Permissions _ {:group_id (:id (perms/all-users-group))
                                                 :object permission-path}]
    (f)))

(defn do-with-all-user-data-perms-graph!
  "Implementation for [[with-all-users-data-perms]]"
  [graph f]
  (let [all-users-group-id  (u/the-id (perms/all-users-group))]
    (premium-features.test-util/with-additional-premium-features #{:advanced-permissions}
      (perms.test-util/with-no-data-perms-for-all-users!
        (perms.test-util/with-restored-perms!
          (perms.test-util/with-restored-data-perms!
            (data-perms.graph/update-data-perms-graph!* {all-users-group-id graph})
            (f)))))))

(defmacro with-all-users-data-perms-graph!
  "Runs `body` with data perms for the All Users group temporarily set to the values in `graph`."
  [graph & body]
  `(do-with-all-user-data-perms-graph! ~graph (fn [] ~@body)))

(defmacro with-all-users-permission
  "Run `body` with the ''All Users'' group being granted the permission
  specified by `permission-path`.

  (mt/with-all-users-permission (perms/app-root-collection-permission :read)
    ...)"
  [permission-path & body]
  `(do-with-all-users-permission ~permission-path (fn [] ~@body)))

(defn doall-recursive
  "Like `doall`, but recursively calls doall on map values and nested sequences, giving you a fully non-lazy object.
  Useful for tests when you need the entire object to be realized in the body of a `binding`, `with-redefs`, or
  `with-temp` form."
  [x]
  (cond
    (map? x)
    (into {} (for [[k v] (doall x)]
               [k (doall-recursive v)]))

    (sequential? x)
    (mapv doall-recursive (doall x))

    :else
    x))

(defn call-with-locale!
  "Sets the default locale temporarily to `locale-tag`, then invokes `f` and reverts the locale change"
  [locale-tag f]
  (mb.hawk.parallel/assert-test-is-not-parallel "with-locale")
  (let [current-locale (Locale/getDefault)]
    (try
      (Locale/setDefault (Locale/forLanguageTag locale-tag))
      (f)
      (finally
        (Locale/setDefault current-locale)))))

(defmacro with-locale!
  "Allows a test to override the locale temporarily"
  [locale-tag & body]
  `(call-with-locale! ~locale-tag (fn [] ~@body)))

;;; TODO -- this could be made thread-safe if we made [[with-temp-vals-in-db]] thread-safe which I think is pretty
;;; doable (just do it in a transaction?)
(defn do-with-column-remappings! [orig->remapped thunk]
  (transduce
   identity
   (fn
     ([] thunk)
     ([thunk] (thunk))
     ([thunk [original-column-id remap]]
      (let [original       (t2/select-one :model/Field :id (u/the-id original-column-id))
            describe-field (fn [{table-id :table_id, field-name :name}]
                             (format "%s.%s" (t2/select-one-fn :name :model/Table :id table-id) field-name))]
        (if (integer? remap)
          ;; remap is integer => fk remap
          (let [remapped (t2/select-one :model/Field :id (u/the-id remap))]
            (fn []
              #_{:clj-kondo/ignore [:discouraged-var]}
              (t2.with-temp/with-temp [:model/Dimension _ {:field_id                (:id original)
                                                           :name                    (format "%s [external remap]" (:display_name original))
                                                           :type                    :external
                                                           :human_readable_field_id (:id remapped)}]
                (testing (format "With FK remapping %s -> %s\n" (describe-field original) (describe-field remapped))
                  (thunk)))))
          ;; remap is sequential or map => HRV remap
          (let [values-map (if (sequential? remap)
                             (zipmap (range 1 (inc (count remap)))
                                     remap)
                             remap)]
            (fn []
              (let [preexisting-id (t2/select-one-pk :model/FieldValues
                                                     :field_id (:id original)
                                                     :type :full)
                    testing-thunk (fn []
                                    (testing (format "With human readable values remapping %s -> %s\n"
                                                     (describe-field original) (pr-str values-map))
                                      (thunk)))]
                #_{:clj-kondo/ignore [:discouraged-var]}
                (t2.with-temp/with-temp [:model/Dimension _ {:field_id (:id original)
                                                             :name     (format "%s [internal remap]" (:display_name original))
                                                             :type     :internal}]
                  (if preexisting-id
                    (with-temp-vals-in-db :model/FieldValues preexisting-id {:values (keys values-map)
                                                                             :human_readable_values (vals values-map)}
                      (testing-thunk))
                    #_{:clj-kondo/ignore [:discouraged-var]}
                    (t2.with-temp/with-temp [:model/FieldValues _ {:field_id              (:id original)
                                                                   :values                (keys values-map)
                                                                   :human_readable_values (vals values-map)}]
                      (testing-thunk)))))))))))
   orig->remapped))

(defn- col-remappings-arg [x]
  (cond
    (and (symbol? x)
         (not (str/starts-with? x "%")))
    `(data/$ids ~(symbol (str \% x)))

    (and (seqable? x)
         (= (first x) 'values-of))
    (let [[_ table+field] x
          [table field]   (str/split (str table+field) #"\.")]
      `(into {} (get-in (data/run-mbql-query ~(symbol table)
                          {:fields [~'$id ~(symbol (str \$ field))]})
                        [:data :rows])))

    :else
    x))

;;; TODO FIXME -- either rename this to `with-column-remappings!` or fix it and make it thread-safe.
#_{:clj-kondo/ignore [:metabase/test-helpers-use-non-thread-safe-functions]}
(defmacro with-column-remappings
  "Execute `body` with column remappings in place. Can create either FK \"external\" or human-readable-values
  \"internal\" remappings:

  FK 'external' remapping -- pass a column to remap to (either as a symbol, or an integer ID):

    (with-column-remappings [reviews.product_id products.title]
      ...)

  Symbols are normally converted to [[metabase.test/id]] forms e.g.

    reviews.product_id -> (mt/id :reviews :product_id)

  human-readable-values 'internal' remappings: pass a vector or map of values. Vector just sets the first `n` values
  starting with 1 (for common cases where the column is an FK ID column)

    (with-column-remappings [venues.category_id [\"My Cat 1\" \"My Cat 2\"]]
      ...)

  equivalent to:

    (with-column-remappings [venues.category_id {1 \"My Cat 1\", 2 \"My Cat 2\"}]
      ...)

  You can also do a human-readable-values 'internal' remapping using the values from another Field by using the
  special `values-of` form:

    (with-column-remappings [venues.category_id (values-of categories.name)]
      ...)"
  {:arglists '([[original-col source-col & more-remappings] & body])}
  [cols & body]
  `(do-with-column-remappings!
    ~(into {} (comp (map col-remappings-arg)
                    (partition-all 2))
           cols)
    (fn []
      ~@body)))

(defn find-free-port
  "Finds and returns an available port number on the current host. Does so by briefly creating a ServerSocket, which
  is closed when returning."
  []
  (with-open [socket (ServerSocket. 0)]
    (.getLocalPort socket)))

;;; TODO -- we could make this thread-safe if we introduced a new dynamic variable to replace [[env/env]]
(defn do-with-env-keys-renamed-by!
  "Evaluates the thunk with the current core.environ/env being redefined, its keys having been renamed by the given
  rename-fn. Prefer to use the with-env-keys-renamed-by macro version instead."
  [rename-fn thunk]
  (let [orig-e     env/env
        renames-fn (fn [m k _]
                     (let [k-str (name k)
                           new-k (rename-fn k-str)]
                       (if (not= k-str new-k)
                         (assoc m k (keyword new-k))
                         m)))
        renames    (reduce-kv renames-fn {} orig-e)
        new-e      (set/rename-keys orig-e renames)]
    (testing (colorize/blue (format "\nRenaming env vars by map: %s\n" (u/pprint-to-str renames)))
      (with-redefs [env/env new-e]
        (thunk)))))

;;; TODO FIXME -- either rename this to `with-env-keys-renamed-by!` or fix it and make it thread-safe
#_{:clj-kondo/ignore [:metabase/test-helpers-use-non-thread-safe-functions]}
(defmacro with-env-keys-renamed-by
  "Evaluates body with the current core.environ/env being redefined, its keys having been renamed by the given
  rename-fn."
  {:arglists '([rename-fn & body])}
  [rename-fn & body]
  `(do-with-env-keys-renamed-by! ~rename-fn (fn [] ~@body)))

(defn do-with-temp-file
  "Impl for `with-temp-file` macro."
  [filename f]
  {:pre [(or (string? filename) (nil? filename))]}
  (let [filename (if (string? filename)
                   filename
                   (u.random/random-name))
        filename (str (u.files/get-path (System/getProperty "java.io.tmpdir") filename))]
    ;; delete file if it already exists
    (io/delete-file (io/file filename) :silently)
    (try
      (f filename)
      (finally
        (io/delete-file (io/file filename) :silently)))))

(defmacro with-temp-file
  "Execute `body` with a path for temporary file(s) in the system temporary directory. You may optionally specify the
  `filename` (without directory components) to be created in the temp directory; if `filename` is nil, a random
  filename will be used. The file will be deleted if it already exists, but will not be touched; use `spit` to load
  something in to it. The file, if created, will be deleted in a `finally` block after `body` concludes.

  DOES NOT CREATE A FILE!

    ;; create a random temp filename. File is deleted if it already exists.
    (with-temp-file [filename]
      ...)

    ;; get a temp filename ending in `parrot-list.txt`
    (with-temp-file [filename \"parrot-list.txt\"]
      ...)"
  [[filename-binding filename-or-nil & more :as bindings] & body]
  {:pre [(vector? bindings) (>= (count bindings) 1)]}
  `(do-with-temp-file
    ~filename-or-nil
    (fn [~(vary-meta filename-binding assoc :tag `String)]
      ~@(if (seq more)
          [`(with-temp-file ~(vec more) ~@body)]
          body))))

(deftest with-temp-file-test
  (testing "random filename"
    (let [temp-filename (atom nil)]
      (with-temp-file [filename]
        (is (string? filename))
        (is (not (.exists (io/file filename))))
        (spit filename "wow")
        (reset! temp-filename filename))
      (testing "File should be deleted at end of macro form"
        (is (not (.exists (io/file @temp-filename)))))))

  (testing "explicit filename"
    (with-temp-file [filename "parrot-list.txt"]
      (is (string? filename))
      (is (not (.exists (io/file filename))))
      (is (str/ends-with? filename "parrot-list.txt"))
      (spit filename "wow")
      (testing "should delete existing file"
        (with-temp-file [filename "parrot-list.txt"]
          (is (not (.exists (io/file filename))))))))

  (testing "multiple bindings"
    (with-temp-file [filename nil, filename-2 "parrot-list.txt"]
      (is (string? filename))
      (is (string? filename-2))
      (is (not (.exists (io/file filename))))
      (is (not (.exists (io/file filename-2))))
      (is (not (str/ends-with? filename "parrot-list.txt")))
      (is (str/ends-with? filename-2 "parrot-list.txt"))))

  (testing "should delete existing file"
    (with-temp-file [filename "parrot-list.txt"]
      (spit filename "wow")
      (with-temp-file [filename "parrot-list.txt"]
        (is (not (.exists (io/file filename)))))))

  (testing "validation"
    (are [form] (thrown?
                 clojure.lang.Compiler$CompilerException
                 (macroexpand form))
      `(with-temp-file [])
      `(with-temp-file (+ 1 2)))))

(defn do-with-temp-dir
  "Impl for [[with-temp-dir]] macro."
  [temp-dir-name f]
  (do-with-temp-file
   temp-dir-name
   (^:once fn* [path]
     (let [file (io/file path)]
       (when (.exists file)
         (org.apache.commons.io.FileUtils/deleteDirectory file)))
     (u.files/create-dir-if-not-exists! (u.files/get-path path))
     (f path))))

(defmacro with-temp-dir
  "Like [[with-temp-file]], but creates a new temporary directory in the system temp dir. Deletes existing directory if
  it already exists."
  [[directory-binding dir-name] & body]
  `(do-with-temp-dir ~dir-name (^:once fn* [~directory-binding] ~@body)))

(defn do-with-user-in-groups
  ([f groups-or-ids]
   #_{:clj-kondo/ignore [:discouraged-var]}
   (t2.with-temp/with-temp [:model/User user]
     (do-with-user-in-groups f user groups-or-ids)))
  ([f user [group-or-id & more]]
   (if group-or-id
     #_{:clj-kondo/ignore [:discouraged-var]}
     (t2.with-temp/with-temp [:model/PermissionsGroupMembership _ {:group_id (u/the-id group-or-id), :user_id (u/the-id user)}]
       (do-with-user-in-groups f user more))
     (f user))))

(defmacro with-user-in-groups
  "Create a User (and optionally PermissionsGroups), add user to a set of groups, and execute `body`.

    ;; create a new User, add to existing group `some-group`, execute body`
    (with-user-in-groups [user [some-group]]
      ...)

    ;; create a Group, then create a new User and add to new Group, then execute body
    (with-user-in-groups [new-group {:name \"My New Group\"}
                          user      [new-group]]
      ...)"
  {:arglists '([[group-binding-and-definition-pairs* user-binding groups-to-put-user-in?] & body]), :style/indent :defn}
  [[& bindings] & body]
  (if (> (count bindings) 2)
    (let [[group-binding group-definition & more] bindings]
      #_{:clj-kondo/ignore [:discouraged-var]}
      `(t2.with-temp/with-temp [:model/PermissionsGroup ~group-binding ~group-definition]
         (with-user-in-groups ~more ~@body)))
    (let [[user-binding groups-or-ids-to-put-user-in] bindings]
      `(do-with-user-in-groups (fn [~user-binding] ~@body) ~groups-or-ids-to-put-user-in))))

(defn secret-value-equals?
  "Checks whether a secret's `value` matches an `expected` value. If `expected` is a string, then the value's bytes are
  interpreted as a UTF-8 encoded string, then compared to `expected. Otherwise, the individual bytes of each are
  compared."
  {:added "0.42.0"}
  [expected ^bytes value]
  (cond (string? expected)
        (= expected (String. value "UTF-8"))

        (bytes? expected)
        (= (seq expected) (seq value))

        :else
        (throw (ex-info "expected parameter must be a string or bytes" {:expected expected :value value}))))

(defn select-keys-sequentially
  "`expected` is a vector of maps, and `actual` is a sequence of maps.  Maps a function over all items in `actual` that
  performs `select-keys` on the map at the equivalent index from `expected`.  Note the actual values of the maps in
  `expected` don't matter here, only the keys.

  Sample invocation:
  (select-keys-sequentially [{:a nil :b nil}
                             {:a nil :b nil}
                             {:b nil :x nil}] [{:a 1 :b 2}
                                               {:a 3 :b 4 :c 5 :d 6}
                                               {:b 7 :x 10 :y 11}])
  => ({:a 1, :b 2} {:a 3, :b 4} {:b 7, :x 10})

  This function can be used to help assert that certain (but not necessarily all) key/value pairs in a
  `connection-properties` vector match against some expected data."
  [expected actual]
  (cond (vector? expected)
        (map-indexed (fn [idx prop]
                       (reduce-kv (fn [acc k v]
                                    (assoc acc k (if (map? v)
                                                   (select-keys-sequentially (get (nth expected idx) k) v)
                                                   v)))
                                  {}
                                  (select-keys prop (keys (nth expected idx)))))
                     actual)

        (map? expected)
    ;; recursive case (ex: to turn value that might be a flatland.ordered.map into a regular Clojure map)
        (select-keys actual (keys expected))

        :else
        actual))

(defn file->bytes
  "Reads a file completely into a byte array, returning that array."
  [^File file]
  (let [ary (byte-array (.length file))]
    (with-open [is (FileInputStream. file)]
      (.read is ary)
      ary)))

(defn file-path->bytes
  "Reads a file at `file-path` completely into a byte array, returning that array."
  [^String file-path]
  (let [f (File. file-path)]
    (file->bytes f)))

(defn bytes->base64-data-uri
  "Encodes bytes in base64 and wraps with data-uri similar to mimic browser uploads."
  [^bytes bs]
  (str "data:application/octet-stream;base64," (u/encode-base64-bytes bs)))

(defn works-after
  "Returns a function which works as `f` except that on the first `n` calls an
  exception is thrown instead.

  If `n` is not positive, the returned function will not throw any exceptions
  not thrown by `f` itself."
  [n f]
  (let [a (atom n)]
    (fn [& args]
      (swap! a #(dec (max 0 %)))
      (if (neg? @a)
        (apply f args)
        (throw (ex-info "Not yet" {:remaining @a}))))))

(defn latest-audit-log-entry
  "Returns the latest audit log entry, optionally filters on `topic`."
  ([] (latest-audit-log-entry nil nil))
  ([topic] (latest-audit-log-entry topic nil))
  ([topic model-id]
   (t2/select-one [:model/AuditLog :topic :user_id :model :model_id :details]
                  {:order-by [[:id :desc]]
                   :where [:and (when topic [:= :topic (name topic)])
                           (when model-id [:= :model_id model-id])]})))

(defn repeat-concurrently
  "Run `f` `n` times concurrently. Returns a vector of the results of each invocation of `f`."
  [n f]
  ;; Use a latch to ensure that the functions start as close to simultaneously as possible.
  (let [latch   (CountDownLatch. n)
        futures (atom [])]
    (dotimes [_ n]
      (swap! futures conj (future (.countDown latch)
                                  (.await latch)
                                  (f))))
    (mapv deref @futures)))

(defn ordered-subset?
  "Test if all the elements in `xs` appear in the same order in `ys` (but `ys` could have additional entries as
  well). Search results in this test suite can be polluted by local data, so this is a way to ignore extraneous
  results.

  Uses the equality function if provided (otherwise just `=`)"
  ([xs ys]
   (ordered-subset? xs ys =))
  ([[x & rest-x :as xs] [y & rest-y :as ys] eq?]
   (or (empty? xs)
       (and (boolean (seq ys))
            (if (eq? x y)
              (recur rest-x rest-y eq?)
              (recur xs rest-y eq?))))))

(defmacro call-with-map-params
  "Execute `f` with each `binding` available by name in a params map. This is useful in conjunction with
  `with-anaphora` (below)."
  [f bindings]
  (let [binding-map (into {} (for [b bindings] [(keyword b) b]))]
    (list f binding-map)))

(defmacro with-anaphora
  "Execute the body with the given bindings plucked out of a params map (which was probably created by
  `call-with-map-params` above."
  [bindings & body]
  `(fn [{:keys ~(mapv (comp symbol name) bindings)}]
     ~@body))

(defn do-poll-until [^Long timeout-ms thunk]
  (let [result-prom (promise)
        _timeouter (future (Thread/sleep timeout-ms) (deliver result-prom ::timeout))
        _runner (future (loop []
                          (if-let [thunk-return (try (thunk) (catch Exception e e))]
                            (deliver result-prom thunk-return)
                            (recur))))
        result @result-prom]
    (cond (= result ::timeout) (throw (ex-info (str "Timeout after " timeout-ms "ms")
                                               {:timeout-ms timeout-ms}))
          (instance? Throwable result) (throw result)
          :else result)))

(defmacro poll-until
  "A macro that continues to call the given body until it returns a truthy value or the timeout is reached.
  Returns the truthy body, or re-throws any exception raised in body.

  Hence, this cannot return nil, false, or a Throwable. [[thunk]] can check for those instead.

  Pro tip: wrap your body with `time` macro to get a feel for how many calls to [[poll-body]] are made."
  [timeout-ms & body]
  `(do-poll-until
    ~timeout-ms
    (fn ~'poll-body [] ~@body)))

(methodical/defmethod =?/=?-diff [(Class/forName "[B") (Class/forName "[B")]
  [expected actual]
  (=?/=?-diff (seq expected) (seq actual)))

(defmacro with-prometheus-system!
  "Run tests with a prometheus web server and registry. Provide binding symbols in a tuple of [port system]. Port will
  be bound to the random port used for the metrics endpoint and system will be a [[PrometheusSystem]] which has a
  registry and web-server."
  [[port system] & body]
  `(let [~system ^metabase.analytics.prometheus.PrometheusSystem
         (#'prometheus/make-prometheus-system 0 (name (gensym "test-registry")))
         server#  ^Server (.web-server ~system)
         ~port   (.. server# getURI getPort)]
     (with-redefs [prometheus/system ~system]
       (try ~@body
            (finally (prometheus/stop-web-server ~system))))))

(defn metric-value
  "Return the value of `metric` in `system`'s registry."
  ([system metric]
   (metric-value system metric nil))
  ([system metric labels]
   (some-> system
           :registry
           (registry/get
            {:name      (name metric)
             :namespace (namespace metric)}
            (#'prometheus/qualified-vals labels))
           ops/read-value)))

(defn- transitive*
  "Borrows heavily from clojure.core/derive. Notably, however, this intentionally permits circular dependencies."
  [h child parent]
  (let [td (:descendants h {})
        ta (:ancestors h {})
        tf (fn [source sources target targets]
             (reduce (fn [ret k]
                       (assoc ret k
                              (reduce conj (get targets k #{}) (cons target (targets target)))))
                     targets (cons source (sources source))))]
    {:ancestors   (tf child td parent ta)
     :descendants (tf parent ta child td)}))

(defn transitive
  "Given a mapping from (say) parents to children, return the corresponding mapping from parents to descendants."
  [adj-map]
  (:descendants (reduce-kv (fn [h p children] (reduce #(transitive* %1 %2 p) h children)) nil adj-map)))
