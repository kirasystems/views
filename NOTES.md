# Notes

## Note 1

There is one hash stored per view signature. If a view is refreshed, a hash of
its data is checked against the previous data and if they match it is not sent
out. Otherwise it is sent out and the hash is replaced. On subscription, we
also store a hash of the view data, but only if it doesn't exist. This is important
because if we always stored it, the following situation would be a problem.


   u1 - subscribes v1, hash is stored
   p1 - updates data requiring v1 to be updated
   u2 - subscribes v1, hash is stored
   t1 - update thread runs and decides v1 needs to be updated because of the hint
        supplied by p1, however, the hash is now the same because of u2 and no
        refresh is sent out to u1.

        