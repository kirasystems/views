# Notes

## Note 1

There is one hash stored per view signature. If a view is refreshed, a hash of
its data is checked against the previous data and if they match it is not sent
out. Otherwise it is sent out and the hash is replaced. On subscription, we
also store a hash of the view data, but only if it doesn't exist. This is important
because if we always stored it, the following situation would be a problem.


* u1 - subscribes v1, hash is stored
* p1 - updates data requiring v1 to be updated
* u2 - subscribes v1, hash is stored
* t1 - update thread runs and decides v1 needs to be updated because of the hint
        supplied by p1, however, the hash is now the same because of u2 and no
        refresh is sent out to u1.

## Note 2

Similar to Note 1, you also want to invalidate the cached hash when unsubscribing. The following scenario revolves around a view called `view` and two different parameters (`1` and `2`). Updating the data to `view 1` will impact `view 2` and vice-versa.

* The user navigates to a page that subscribes to `view 1`. A hash of the current data is stored into the cache. To keep it simple, we'll call this hash `view-1-hash`.
* The user then navigates to a second page. It will unsubscribe to `view 1`, subscribe to `view 2`, compute the hash of the data for `view 2` and store it in the cache.
* The user updates the data related to `view 2`. The corresponding hash in the cache will be updated and the user will be notified. At this point, the cache will look like 
```    
view 1: view-1-hash  <- OUPS, this one is not valid, but wasn't 
                        updated because the user isn't subscribed.
view 2: newly computed hash
```
* The user navigates back to the first page. It will unsubscribe to `view 2` and subscribe to `view 1`. The computed the hash is different, but because of Note 1, it won't update it.
* The user updates the related data and put it back to how it was initially. The computed hash will be equal to what was in the cache (`view-1-hash`) so no message will be sent to the client. It's problematic here since the data was modified two times and the view system didn't notice any change.

**Solution:** Clearing the cache when unsubscribing. The worst case will happen when multiple clients are connected and subscribed to a view, it may send one (useless) refresh message.