# views

A Clojure library designed to ... well, that part is up to you.


## Design

Subscription

* subscribe to a view
* unsubscribe from a view
* remove all subscriptions (disconnect)
* get set of subscribed views

Deltas

* send deltas
* broadcast deltas
* receive deltas  ( receive deltas from any other clients and broadcast out to subscribers a.k.a. broadcast deltas )

DB

* calculates pre-delta checks (heuristic optimization)
* calculates actual deltas
* different heuristics for inserts, updates, and deletes
* different properties for outer joins
* pre-checks and delta calculations are coupled:
   - we don't know if we need to calculate insert deltas until we do the insert and see if the result affects a view 

How should DB code work?

- separate namespaces for insert, update, delete?
- low-level db actions (execute/query/transaction/etc.) should be separate namespace?
- initial view computing (view map, args, etc.) should be separate namespace?





## Usage

FIXME

## License

Copyright © 2014 FIXME

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
