# autocomplete

Autocomplete is an http server that manages recency based
lists of keywords. Keywords are normalized according to
user supplied locale and are searchable by prefix. For
unique tests, keys may be accompanied by a user supplied
id. This id is also searchable. An arbitrary data section
is also provided.

## deps

1. libevent 2.x
2. json-c
3. libicu

## installation

make && make install

## running

./autocomplete -d /var/autocomplete

## api

*GET /put*

### args

namespace (req) - high level aggregation, typically user  
key (req) - key for future searches  
locale (opt) - locale used to normalize key ( see libicu )  
id (opt) - secondary element for uniq  
ts (opt) - utc timestamp of last used  

### side effects

Either creates a new element or updates an existing elements
position in the recency list. The namespace is also marked
dirty for subsequent flushing to disk.

### response

200 OK  
500 INTERNAL  
400 BAD_REQUEST  

