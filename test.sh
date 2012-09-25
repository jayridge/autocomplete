#!/bin/bash

for k in {'one','two','three','four','ten','twenty','twenty'}; do
    curl "localhost:8080/put?namespace=ass&data=dork&key=${k}"
    #curl "localhost:8080/put?namespace=ass&key=${k}"
done
curl "localhost:8080/decr?namespace=ass&key=twenty&value=10"
curl "localhost:8080/search?namespace=ass&key=tw"
