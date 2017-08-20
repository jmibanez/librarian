{
 :profiles/dev  {:env {:db-connection-uri "jdbc:postgresql://localhost/librarian_dev",
                       :db-connection-user "librarian",
                       :db-connection-password "librarian"
                       :db-pool-initial "4"
                       :db-pool-max "20"

                       :log-level "debug"}}

 :profiles/test {:env {:db-connection-uri "jdbc:postgresql://localhost/librarian_test",
                       :db-connection-user "librarian",
                       :db-connection-password "librarian"
                       :db-pool-initial "1"
                       :db-pool-max "4"

                       :log-level "info"}}
}
