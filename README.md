# views

Eventually consistent external materialized views for SQL databases.

## Design

TODO

## Usage

TODO

## Testing

You will need to set up the test db to run the tests:

```bash
$ psql -Upostgres < test/views/test_db.sql
CREATE ROLE
CREATE DATABASE
$
```

This will create a role `views_user` and a database owned by that user called `views_test`.

(You can change the database settings if you'd like by editing that file and checking the config in `test/views/fixtures.clj`.)

Then, to run all tests in the repl:

```clojure
user=> (require '[views.all-tests :as at])
user=> (at/run-all-tests)
```

## License

Copyright © 2014 DiligenceEngine

Authors Dave Della Costa (https://github.com/ddellacosta) and Alex Hudek (https://github.com/akhudek)

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
