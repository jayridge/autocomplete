#!/bin/bash

for k in {'one','two','three','four','ten','twenty','twenty'}; do
    curl "localhost:8080/put?namespace=foo&data=dork&key=${k}"
    curl "localhost:8080/put?namespace=foo&key=twit&id=123&data=${k}"
done
curl "localhost:8080/del?namespace=foo&key=twenty"
curl "localhost:8080/search?namespace=foo&key=tw"
