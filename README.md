# T-FUN
Functions that run on the Datomic main query group.

## Deploy
`clj -A:dev -m datomic.ion.dev '{:op :push}'`
and follow the instructions to deploy to a query group.

## Running Tests
`clj -A:midje`
