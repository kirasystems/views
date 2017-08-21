# views
[![Build Status](https://travis-ci.org/kirasystems/views.svg?branch=master)](https://travis-ci.org/kirasystems/views)
[![Dependencies Status](https://jarkeeper.com/kirasystems/views/status.svg)](https://jarkeeper.com/kirasystems/views)

Eventually consistent external materialized views.

[![Clojars Project](http://clojars.org/kirasystems/views/latest-version.svg)](http://clojars.org/kirasystems/views)

## Version 2

Version 2.0.0 represents the merging of Gered King's branch with 
ours. A big thanks to Gered whose changes make the view library 
much more user friendly and better documented.

## Basic Concepts

The views library allows you to manage a **view system** which is a 
collection of **views** and a list of **subscribers** to those views. 
Subscribers will get sent **view refreshes** in realtime when the data 
represented by the views they are subscribed to changes. Relevant 
changes are found through the use of **hints** which are added to the 
view system by anything that is actually changing the data at the 
instant it is changed.

A **view** is similar in concept to a [materialized view][2], though in
practice it may not actually keep a copy of the underlying data 
represented by the view and instead just keep a copy of a query or the 
location of where the data can be retrieved from when it is needed 
(e.g. when view refrehses need to be sent out).

[2]: https://en.wikipedia.org/wiki/Materialized_view

A view is represented by the protocol `views.protocols/IView`:

```clj
(defprotocol IView
  (data [this namespace parameters])
  (relevant? [this namespace parameters hints])
  (id [this]))
```

`id` simply returns a unique identifier for this view. `data` returns a
copy of the underlying data represented by this view. `relevant?` 
determines if a collection of hints is relevant to the view and is 
called by the view system whenever new hints are received to determine 
if view refreshes need to be sent out for this view.

A **hint** is a map of the form:

```clj
{:namespace ...
 :type ...
 :hint ...}
```

`:type` represents the type of view (e.g. `:sql-table-name`) and is 
defined by the view implementation that this hint is intended for. 
`:hint` is the hint information and its contents will differ depending
on the type of view it's intended for. As an example, it may be a list
of database table names for an SQL-based view.


**Namespaces** can be used to isolate multiple sets of the same type of
data being represented by the views within the view system. As an 
example, for SQL views a namespace could be used to represent the 
database to connect to if your system is comprised of multiple similar 
databases. A view is not specifically tied to a namespace, however the 
hints processed by the view system are only relevant for the namespace 
specified in the hint.

Int determining the relevance of any given hint, a view's `relevant?` 
predicate will compare all the properties of a hint, including the 
namespace and type to ensure that view refreshes aren't issued
incorrectly or too frequently.

**Subscribers** can be registered within the view system. A 
subscription can be created within the view system by specifying the 
view to subscribe to, identified by its view ID, a namespace, 
and any parameters that the view might take. These 3 properties go 
together to form a **view signature** or **view sig**. A view sig is 
represented by a map:

```clj
{:namespace ...
 :view-id ...
 :parameters ...}
```

Subscriptions are considered unique for a subscriber based on all 3 of
these properties combined. As such, a subscriber can have multiple 
concurrent subscriptions to the same view if the namespace and/or 
parameters are different for all of them.

A subscriber is uniquely identified by its **subscriber key**. Common
subscriber key values include user ID, Session ID, or other 
identifiers like client ID used in libraries such as Sente for websocket
connections.

When hints are processed by the view system and found to be relevant 
for any of the views (through the use of the `relevant?` check 
mentioned earlier), **view refreshes** are sent out to all of the 
subscribers of the view. Up-to-date data for the view is retrieved via 
the view's `data` function and then sent out.

Whenever data is refreshed, a hash is kept and is compared to on each 
refresh to make sure that we don't send out another refresh if the data
is unchanged from the last refresh sent.


## Usage

To explain basic usage of the views library, we'll walk through an
example building up a simple system so you can see how it works
interactively.

To begin, we'll need to use functions from the `views.core` namespace.

```clj
(require '[views.core :as views])
```

### View System Initialization

We first need to create the view system. This will be kept in an atom
and will be passed around to the different views library functions also
*as an atom* as the views system needs to maintain it's own internal
state.

```clj
(def view-system (atom {}))
```

For a fully working view system, we also need to provide a function
that will be used to send view refreshes to subscribers. For now we'll 
just print view refreshes out in the REPL, but in a real system you'd
want it to send them to a connected Websocket client, or out
over some kind of distributed messaging service, etc.

```clj
(defn send-fn
  [subscriber-key [view-sig view-data]]
  (println "view refresh" subscriber-key view-sig view-data))
```

Now we're ready to actually create the view system. To do this, we call 
`init!` which takes a set of options. We provide our send function 
above using the `:send-fn` option. For a description of all the options
available, see `views.core/default-options`.

```clj
(views/init! view-system {:send-fn send-fn})
```

At this point, the view system is ready.

Right now there are some background threads running, one of which is
the *refresh watcher* which handles incoming hints and checks them for
relevancy. When relevant hints are found, view refresh requests are
dispatched to one or more *refresh worker* threads which actually
perform the work of retrieving updated view data and sending it off to
subscribers.

Now, let's talk about setting up some views, as we have none in
our view system.

### Adding Views

For demonstration purposes, we'll set up views for an in-memory
datastore:

```clj
(def memory-datastore
  (atom {:a {:foo 1 
             :bar 200 
             :baz [1 2 3]}
         :b {:foo 2 
             :bar 300 
             :baz [2 3 4]}}))
```

To retrieve or modify data within this memory datastore, we'd likely
want to use a path made up of keywords, e.g. `[:a :bar]` would
correspond with the value `200`, and `[:b :baz 2]` with the value `4` 
using the initial data defined above.

So, let's create a `MemoryView`:

```clj
(require '[views.protocols :refer [IView]])

(def memory-view-hint-type :ks-path)

(defrecord MemoryView [id ks]
  IView
  (id [_] id)
  (data [_ namespace parameters]
    (get-in @memory-datastore 
            (-> [namespace]
                (into ks)
                (into parameters))))
  (relevant? [_ namespace parameters hints]
    (some #(and (= namespace (:namespace %))
                (= ks (:hint %))
                (= memory-view-hint-type (:type %)))
          hints)))
```

Nothing particularly special here, `data` simply returns a value from 
`memory-datastore` using a path made by combining `namespace` with a 
sequence of keywords `ks` and then finally adding `parameters` (which 
is a collection of parameters) to the end of the path.

Note that with this method of referencing data within 
`memory-datastore`, the keys `:a` and `:b` are being used as 
namespaces.

`relevant?` simply compares all 3 values of each of the hints passed in
to make sure they all match. `memory-view-hint-type` is, as its name
implies, a value that is used to identify hints as being those intended
for memory views and not for, e.g. SQL views (if we had a view system 
with multiple different types of views in it). The function returns 
true if at least one of the passed in hints was found to be relevant.

Now we can add some views to our view system:

```clj
(views/add-views!
  view-system
  [(MemoryView. :foo [:foo])
   (MemoryView. :bar [:bar])
   (MemoryView. :baz [:baz])])
```

We now have 3 views, `:foo`, `:bar` and `:baz` which each refer to data
under that same path. Note that these views do not define a namespace.
That is for subscribers to specify when they register a subscription. 
As well, code that updates `memory-datastore` will create hints for the
view system as we'll soon see, and at that time it will include a 
namespace in any created hints.

> Most applications will probably want to just pass in a list of views
> via `views.core/init!` through the `:views` option. However, there is
> nothing wrong with using `add-views!` like this if you prefer, or if 
> you need to change views on the fly.
> 
> Keep in mind though that adding views via `add-views!` will replace 
> existing views in the view system with the same ID. We only
> recommend doing this during development.

### Subscribing to Views

As mentioned previously, view subscriptions are keyed by a **view
signature** or view sig, which we can create using a helper function if
we wish:

```clj
(views/->view-sig :a :foo [])
=> {:namespace :a, :view-id :foo, :parameters []}
```

We create a subscription by calling `views.core/subscribe!`. For this
demonstration, we'll simply make up a subscriber key. The last argument
is where we could pass in some application/user context data that would
be helpful to use when doing subscription authorization (which we'll 
discuss later and just ignore for now). For now, we'll just pass in 
`nil` context.

```clj
(views/subscribe! 
  view-system                        ; view system atom
  (views/->view-sig :a :foo [])      ; view sig of the view to subscribe to
  123                                ; subscriber key
  nil)                               ; context
```

`subscribe!` returns a `future` which will be realized when the 
subscription finishes. Whenever a new subscription is added, the 
subscriber is sent an initial set of data for the view. This view 
refresh is done in a separate thread via a `future`.

We can see that a view refresh was sent out as a result of this 
subscription as our `send-fn` function from before was called and the 
following output should have appeared

```
view refresh 123 {:view-id :foo, :parameters []} 1
```

right away after the call to `subscribe!`. The `1` at the end 
corresponds to the data in `memory-datastore` under the path 
`[:a :foo]`.

> Note that an initial view refresh is **always** sent out to the 
> subscriber when a subscription is first created. This happens even 
> if the view data has not changed since the last refresh for this view
> occurred, as obviously the new subscriber was not part of that 
> refresh.

### Hints and View Refreshes

Adding hints to the view system triggers refreshes of views for which 
they are relevant towards. Our application code that changes data which
these views are based on needs to have a way of adding views to the 
view system.

#### Hints

As mentioned previously, a hint is simply a map that contains a 
namespace, a type and some data that will differ based on the types of
views in the view system. There is a helper function to create this 
map:

```clj
(views/hint :a [:foo] memory-view-hint-type)
=> {:namespace :a, :hint [:foo], :type :ks-path}
```

Generally speaking the `:type` value will be the same for all hints 
which are intended for the same types of views. For example, all of our
`MemoryView` views expect the type to be `:ks-path`, because the
`:hint` values they expect to compare against are all keyword paths.

#### Adding Hints to the View System

There are two main ways to do this:

1. Queue hints which will be picked up by the refresh watcher thread on
   a regular interval (set by the option `:refresh-interval`).
2. Immediately trigger a refresh for a list of hints.

Using option 2 all the time generally does result in much more
responsive feeling system from the user's perspective. But you should
also consider just how frequently your code could end up triggering
refreshes.

Queueing hints as in option 1 will help to guard against duplicate 
hints triggering excessive view refreshes as duplicate hints added to 
the queue are dropped. But queued hints are not processed until the 
refresh watcher thread runs at the next `:refresh-interval`, so you 
lose some responsiveness by going this route.

There are more factors to consider in addition to all of this though. 
As hints are processed, they are internally turned into view refresh 
requests and dispatched to the refresh worker threads by adding them to
an internal queue. This refresh queue also drops duplicate requests, 
but only if there is a backlog of refresh requests waiting in the queue
(which would happen if some views are taking too long to refresh, e.g. 
slow SQL queries, overloaded server/network, not enough worker threads,
etc). If the worker threads are able to process refresh requests very 
quickly, then the internal queue will usually be empty or near-empty 
and some or all duplicate refresh requests might make it through.

Also, keep in mind that hashes of view data are computed and 
compared each time a view refresh is about to be sent out. While 
the underlying view data must still be retrieved on each refresh
request, the data will not be sent out to subscribers if the hash
of the new data matches the stored hash.

Ultimately, there isn't really a right or wrong answer as to which 
method you choose. It will usually make the most sense to default 
to option 2 for most actions that need to add hints to the view 
system. This will generally result in a more responsive system. 
But you'll want to continually evaluate whether some actions 
should possibly be switched over to queue up hints instead.

#### Option 1: Queueing Hints

Use `queue-hints!` and pass in a collection of hints. They will be 
added to the queue and the refresh watcher thread will process them on 
the next refresh interval.

```clj
(views/queue-hints! 
  view-system 
  [(views/hint :a [:foo] memory-view-hint-type)])
```

#### Option 2: Immediately Trigger Refreshes From Hints

Use `refresh-views!` and pass in a collection of hints. They will be 
processed immediately and refresh requests will be dispatched for all 
views for which there were relevant hints (and subscribers) for.

```clj
(views/refresh-views! 
  view-system 
  [(views/hint :a [:foo] memory-view-hint-type)])
```

#### But Wait -- View Data Must Be Changed First!

If you were following along and tried the above examples out, you would
have noticed that our `send-fn` function was never called. As mentioned
previously, each time a view refresh is processed a hash is taken of 
the data and compared against the previous refresh's hash. Only if the 
data is found to have been changed is a refresh sent out.

We haven't changed any of the data in `memory-datastore` yet, so none 
of the hints we add to the system will trigger a view refresh to be 
sent. This is a good thing!

Normally in your application you'll want to add hints to the view 
system at the same place you do some operation that changes data. So, 
we can add a function to allow us to change the data in 
`memory-datastore` and add an appropriate hint about what was changed 
to the view system at the same time:

```clj
(defn memdb-assoc-in!
  [vs namespace ks v]
  (let [path  (into [namespace] ks)
        hints [(views/hint namespace ks memory-view-hint-type)]]
    (swap! memory-datastore assoc-in path v)
    (views/refresh-views! vs hints)))
```

And then we can use it to change data relevant to the view we're 
subscribed to (`:foo`):

```clj
(memdb-assoc-in! view-system :a [:foo] 42)
```

As soon as you run this you should see that `send-fn` was called to 
send out a view refresh:

```
view refresh 123 {:view-id :foo, :parameters []} 42
```

And of course, `memory-datastore` was updated correctly at the same 
time:

```clj
@memory-datastore
=> {:a {:foo 42, :bar 200, :baz [1 2 3]}, :b {:foo 2, :bar 300, :baz [2 3 4]}}
```

As we would expect given the current subscriptions in our view system, 
view refreshes will only be sent out if we change the data under 
`[:a :foo]` as refreshes are only processed if there are subscribers 
for a view.

### Unsubscribing

Unsubscribing a subscriber is done through `views.core/unsubscribe!` 
and the arguments are the same:

```clj
(views/unsubscribe! 
  view-system                        ; view system atom
  (views/->view-sig :a :foo [])      ; view sig of the view to unsubscribe from
  123                                ; subscriber key
  nil)                               ; context
```

Remember that subscriptions are keyed by view sig, so to unsubscribe 
from a view, you must use the exact same namespace and parameters that 
was used to subscribe to it in the first place.

If you need to unsubscribe from all of a subscriber's current 
subscriptions, you can use `views.core/unsubscribe-all!` which 
essentially completely removes a subscriber from the views system.

```clj
(views/unsubscribe-all! view-system 123)   ; where '123' is the subscriber key
```

### Shutting Down the Views System

You can stop the views system by simply calling `views.core/shutdown!`

```clj
(views/shutdown! view-system)
```

This function will by default block until the refresh watcher and all 
refresh worker threads have finished (they are sent interrupt signals 
when `shutdown!` is called). If for some reason you do not wish to 
block, you can pass an additional argument to `shutdown!`:

```clj
(views/shutdown! view-system true)   ; don't block waiting for threads to terminate
```

## Subscription Authorization

By default, no subscriptions require authorization. If you wish for 
some or all views to require some kind of authorization, you should 
provide an `:auth-fn` option to `views.core/init!`.

This is a function of the form:

```clj
(fn [view-sig subscriber-key context]
  ; ...
  )
```

It should return true if the subscription is authorized. `context` is 
the exact value that was passed in as the context argument to 
`subscribe!`. You might wish to pass in a Ring request map or a user 
profile for example.

If subscription authorization fails, `subscribe!` returns `nil`.

You can also provide the `:on-unauth-fn` option to `views.core/init!` 
and set it to a function that will be called in the event that 
subscription authorization failed. This function takes the same 
arguments as `:auth-fn`. The return value is not used.

Your application may or may not need this depending on how you have 
things set up (the fact that `subscribe!` returns `nil` if unauthorized
may be enough for you). It is just provided as an extra convenience.

## Namespaces

As has been mentioned already, namespaces can be used to isolate 
subscriptions to views and view refreshes. Typical use of namespaces 
within a views system would be to set them to something that specifies 
which database to retrieve view data from when you have multiple 
databases all with an identical structure.

Namespace information is not included in the actual view refresh data 
that gets sent to subscribers. It is just considered to be a 
server-side concern.

Depending on your application, you may be perfectly ok with just 
passing in the specific namespace needed when creating view 
subscriptions. However, you can also specify a `:namespace-fn` option 
in your call to `views.core/init!` and provide a function that will 
return the namespace to use for all calls to `subscribe!` and 
`unsubscribe!` that get passed a view sig which **does not** include a 
namespace in it.

The `:namespace-fn` function should be of the form:

```clj
(fn [view-sig subscriber-key context]
  ; ...
  )
```

`context` will be whatever was passed in as the context argument to 
`subscribe!`/`unsubscribe!`.

It bears repeating that `:namespace-fn` will **not** be called even if
it was set if you use a view sig that includes a `:namespace` key.

For this reason the helper function `->view-sig` includes an extra 
overload that does not set a namespace.

```clj
; a view sig that will result in namespace-fn being called (if one is set)
(views/->view-sig :foo [])
=> {:view-id :foo, :parameters []}

; a view sig that will always use :a as the namespace, even if a namespace-fn is set
(views/->view-sig :a :foo [])
=> {:namespace :a, :view-id :foo, :parameters []}
```

## View System Initialization Options

There are a number of options that can be provided to 
`views.core/init!`. The only one that absolutely must be provided for a
working system is `:send-fn` while all the other default options will
generally suffice for a non-distributed relatively low-load
application.

The default options are defined in `views.core/default-options`.

#### `:send-fn`

A function that is used to send view refresh data to subscribers.

```clj
(fn [subscriber-key [view-sig view-data]]
  ; ...
  )
```

#### `:views`

A list of `IView` instances. These are the views that can be 
subscribed to. Views can also be added/replaced in the system after 
initialization by calling `views.core/add-views!`.

#### `:put-hints-fn`

A function that typically will be used by the different views plugin 
libraries providing view implementations (such as 
[views.sql](https://github.com/gered/views.sql) or
[views.honeysql](https://github.com/gered/views.honeysql)) to add 
hints to the view system. 

This function is used as a common configurable way for these different
plugin libraries to add hints because the application can provide an
alternate implementation to e.g. send hints out over a
distributed messaging service and it will affect all views in the
system (which would not be possible if all or just some were hard-coded
to use `queue-hints!` or `refresh-views!`).

```clj
(fn [^Atom view-system hints]
  ; ...
  )
```

The default implementation is:

```clj
(fn [^Atom view-system hints]
  (refresh-views! view-system hints))
```

#### `:refresh-queue-size`

The size of the internal refresh request queue used to hold refresh
requests for the refresh worker threads. If you notice some refresh 
requests being dropped, you may wish to increase this (after of course
seeing if you have some slow views that could be improved).

Default is `1000`.

#### `:refresh-interval`

An interval in milliseconds at which the refresh watcher thread will 
check for queued up hints and dispatch relevant view refresh requests
to the refresh worker threads.

Default is `1000`.

#### `:worker-threads`

The number of refresh worker threads that continually poll for refresh
requests and handle sending view refreshes to subscribers.

Default is `8`.

#### `:auth-fn`

A function that authorizes view subscriptions. It should return true
if the subscription is authorized. If this function is not set, no view
subscriptions will require authorization.

```clj
(fn [view-sig subscriber-key context]
  ; ...
  )
```

#### `:on-unauth-fn`

A function that is called when subscription authorization fails. The
return value of this function is not used.

```clj
(fn [view-sig subscriber-key context]
  ; ...
  )
```

#### `:namespace-fn`

A function that is used during subscription and unsubscription **only**
if no namespace is specified in the view sig passed in. This function
should return the namespace to be used for the 
subscription/unsubscription.

```clj
(fn [view-sig subscriber-key context]
  ; ...
  )
```

#### `:stats-log-interval`

Interval in milliseconds at which a logger will output an INFO log
entry with some view system statistics (refreshes/sec, 
dropped-refreshes/sec, duplicate-refreshes/sec). If not set, no
logging is done.


## Considerations for Distributed Systems

If you're looking to use a views system with an application that will 
be running on multiple servers, all you really need to do to get the 
views system working consistently across all the nodes is to make sure
that when new hints are to be added to the views system, they are sent
to all application nodes.

For example, you can set up a messaging service (such as RabbitMQ, etc)
and when you need to add hints to the views system, instead of calling
`queue-hints!` or `refresh-views!` with the new hints, you simply send
them to the messaging service. 

Most of the views plugin libraries providing view implementations 
(such as views.sql) will call `views.core/put-hints!` to add hints to
the system. `put-hints!` uses whatever the `:put-hints-fn` function was
set to in the options passed to `views.core/init!`. The default 
`:put-hints-fn` implementation simply calls `refresh-views!`, but you
can easily provide an alternative function that sends the hints to a
messaging service.

Then, your application nodes need to listen for hints being received
from the messaging service. You should then call `queue-hints!` or
`refresh-views!` with the hints received this way.


## Contributors

In alphabetical order:
* Alexander Hudek (https://github.com/akhudek)
* Dave Della Costa (https://github.com/ddellacosta)  
* Gered King (https://github.com/gered)

## License

Copyright © 2015-2017 Kira Inc.

Distributed under the MIT License.