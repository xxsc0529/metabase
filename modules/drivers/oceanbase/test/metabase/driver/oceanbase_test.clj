   (:require
   [clojure.test :refer :all]
   [metabase.driver.oceanbase :as oceanbase]))

(deftest filtered-syncable-schemas-test
  (testing "Test filtered-syncable-schemas with mocked data"
    (let [mock-conn (Object.)
          mock-metadata (Object.)
          mock-schema-inclusion-filters nil
          mock-schema-exclusio      n-filters nil]

      ;; Test MySQL mode
      (with-redefs [oceanba      se/detect-mode-from-connection (constantly "mysql")
                    oceanbase/jdbc-query (fn [spec query opts]
                                         (when (= query ["SELECT DATABASE() as current_db"])
                                           [{:current_db "test_db"}]))]
        (let [result (#'oceanbase/filtered-syncable-schemas-impl :oceanbase mock-conn mock-metadata mock-schema-inclusion-      filters mock-schema-exclusion-filters)]
          (is (= result ["test_db"]))))

      ;; Test Oracle mode
      (with-r      edefs [oceanbase/detect-mode-from-connection (constantly "oracle")
                    oceanbase/jdbc-query (fn [spec query opts]
                                         (when (= query ["SELECT USER FROM DUAL"])
                                           [{:USER "test_user"}]))]
        (let [result (#'oceanbase/filtered-syncable-schemas      -impl :oceanbase mock-conn mock-metadata mock-schema-inclusion-filters mock-schema-exclusion-filters)]
          (is (= result ["test_user"])))))))

(deftest detect-mode-from-con      nection-test
  (testing "Test mode detection with mocked data"
    (let [mock-conn (Object.)]

      ;; Test MySQL mode detection
      (with-redefs [oceanbase/jdbc-query (fn [spec query opts]
                                               (when (= query ["SHOW VARIABLES LIKE 'ob_compatibility_mode'"])
                                           [{:value "MYSQL"}]))]
        (is (= "mysql" (oceanbase/detect-mode-from-connection mock-conn))))

      ;; Test Oracle mode detection
      (with-redefs [oceanbase/jdbc-query (fn [spec query opts]
                                               (when (= query ["SHOW VARIABLES LIKE 'ob_compatibility_mode'"])
                                           [{:value "ORACLE"}]))]
        (is (= "oracle" (oceanbase/detect-mode-from-connection mock-conn))))

      ;; Test default to mysql when no result
      (with-redefs [oceanbase/jdbc-query (fn [spec query opts] [])]
        (is       (= "mysql" (oceanbase/detect-mode-from-connection mock-conn)))))))

(deftest get-mode-test
  (testing "Test get-mode with mocked data"
    (let [mock-spec {:host "localhost" :port 2881 :dbname "test_db"}]

      ;; Test MySQL mode
      (with-redefs [oceanbase/jdbc-query (fn [spec query opts]
                                               (when (= query ["SHOW VARIABLES LIKE 'ob_compatibility_mode'"])
                                           [{:value "MYSQL"}]))]
        (is (= "mysql" (oceanbase/get-mode mock-spec))))

      ;; Test Oracle mode
      (with-redefs [oceanbase/jdbc-query (fn [spec query opts]
                                               (when (= query ["SHOW VARIABLES LIKE 'ob_compatibility_mode'"])
                                           [{:value "ORACLE"}]))]
        (is (= "oracle" (oceanbase/get-mode mock-spec))))

      ;; Test caching
      (with-redefs [oceanbase/jdbc-query (fn [spec query opts]
                                               (when (= query ["SHOW VARIABLES LIKE 'ob_compatibility_mode'"])
                                           [{:value "MYSQL"}]))]
        (let [result1 (oceanbase/get-mode mock-spec)
              result2 (oceanbase/get-mode mock-spec)]
          (is (= result1 result2))
          (is (= "mysql" result1)))))))
