# autocomplete

Autocomplete is an http server that manages recency based
lists of keywords. Keywords are normalized according to
user supplied locale and are searchable by prefix. For
unique tests, keys may be accompanied by a user supplied
id. This id is also searchable. An arbitrary data section
is also provided.

## deps

1. [http://libevent.org/](libevent 2.x)
2. [https://github.com/json-c/json-c.git](json-c)
3. [http://site.icu-project.org/download](libicu)

*N.B. compile libicu with utf-8 define*

## installation

    make && make install

## running

./autocomplete -d /var/autocomplete

## api

###*GET /put*

#### args

`namespace` (req) - high level aggregation, typically user  
`key` (req) - key for future searches  
`locale` (opt) - locale used to normalize key ( see libicu )  
`id` (opt) - secondary element for uniq  
`data` (opt) - opaque data element returned on search  
`ts` (opt) - utc timestamp of last used  

#### side effects

Either creates a new element or updates an existing elements
position in the recency list. The namespace is also marked
dirty for subsequent flushing to disk.

Initial access of namespace can force a read from disk.

#### response

200 OK  
500 INTERNAL  
400 BAD_REQUEST  


###*GET /del*

#### args

`namespace` (req) - high level aggregation, typically user  
`key` (req) - key for future searches  
`locale` (opt) - locale used to normalize key ( see libicu )  
`id` (opt) - secondary element for uniq  

#### side effects

Deletes an element identified by key and id. The namespace is
marked dirty for subsequent flushing to disk.

#### response

200 OK  
400 BAD_REQUEST  


###*GET /nuke*

#### args

`namespace` (req) - high level aggregation, typically user
`key` (req) - key prefix
`locale` (opt) - locale used to normalize key ( see libicu )

#### side effects

Deletes elements matching key prefix. The namespace is
marked dirty for subsequent flushing to disk.

#### response

200 OK
400 BAD_REQUEST


###*GET /search*

#### args

`namespace` (req) - high level aggregation, typically user  
`key` (req) - key for future searches  
`locale` (opt) - locale used to normalize key ( see libicu )  
`id` (opt) - secondary element for uniq  
`limit` (opt:1,000) - max records to return

#### side effects

Initial access of namespace can force a read from disk.

#### response

200 OK  
```json
{ "results": [ { "key": "twit", "id": "123", "when": 1352840225, "count": 40, "data": "twenty" } ] }
```
400 BAD_REQUEST  

