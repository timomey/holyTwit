#!/bin/bash

for file in  *.json; do
    jq --slurp . "$file" > "cleaned/$file.json"
done
