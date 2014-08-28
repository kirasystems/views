(ns views.persistence.core)

(defprotocol IPersistence
  (subscribe! [this templates namespace view-sig subscriber-key]
    "Subscribes a subscriber with subscriber-key to a view with signature
     view-sig. Templates is a map of all defined view templates and db
     is a jdbc transcation handle for the database from which initial
     view data will be retrieved.

     This function must return the view-data for the subscribed view.")

  (unsubscribe! [this namespace view-sig subscriber-key]
    "Unsubscribes a subscriber with key 'subscriber-key' from the view
    with signature 'view-sig' in namespace 'namespace'.")

  (unsubscribe-all! [this namespace subscriber-key]
    "Unsubscribes the subscriber with key 'subscriber-key' from ALL views
    in namespace 'namespace'.")

  (view-data [this namespace table-name]
    "Return all the view data that references a table name in a namespace.")

  (subscriptions [this namespace signatures]
   "Return all subscribers for all signatures in the list 'signatures' in
   a namespace."))
