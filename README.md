# T-FUN
An example of functions that run on the Datomic main query group (the transactor) which synchronize the Datomic data with an external data source, Cloudsearch in this case. The Cloudsearch cluster and other infrastructure are created or updated on each deploy of the application.

## Deploy
`clj -A:dev -m datomic.ion.dev '{:op :push}'`
and follow the instructions to deploy to a query group.

## Running Tests
`clj -A:midje`

## Running in REPL
If you need the infrastructure to work, run the REPL with the following environmental variable substituting in the correct deployment group:

`DATOMIC_APP_INFO_MAP='{:app-name "t-fun" :deployment-group "<deployment-group>"}''`

Copyright Hotel JV Services 2019

Released as open source June 12, 2020.
