# T-FUN
Functions that run on the Datomic main query group.

## Deploy
`clj -A:dev -m datomic.ion.dev '{:op :push}'`
and follow the instructions to deploy to a query group.

## Running Tests
`clj -A:midje`

## Running in REPL
If you need the infrastructure to work, run the REPL with the following environmental variable substituting in the correct deployment group:

`DATOMIC_APP_INFO_MAP='{:app-name "t-fun" :deployment-group "<deployment-group>"}''`

For work developing on infrastructure, and connecting to a nrepl.

`DATOMIC_ION_APPLICATION_INFO='{:deployment-group "dc-[production|qa|development|test]-compute-main" :app-name "t-fun"}' clj -A:nrepl`
