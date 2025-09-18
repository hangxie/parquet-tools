#!/bin/bash

set -euo pipefail

mkdir -p org/example/ lib/
for D in $(cat jar.list); do
    JAR=$(echo $D | sed 's@.*\/@@')
    test -f lib/${JAR} || curl -o lib/${JAR} $D
done

rm -f ./old-style-list.parquet
javac -cp "lib/*" NestedListStructure.java
mv NestedListStructure.class org/example/
java -cp ".:lib/*" org.example.NestedListStructure
