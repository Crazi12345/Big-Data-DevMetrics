#!/bin/bash
#install postgresql
 helm install postgresql \
  --version=12.1.5 \
  --set auth.username=root \
  --set auth.password=brunkage \
  --set auth.database=brunkage \
  --set primary.extendedConfiguration="password_encryption=md5" \
  --repo https://charts.bitnami.com/bitnami \
  postgresql



#TODO log into sqoop pod
 sqoop list-databases \
  --connect "jdbc:postgresql://postgresql:5432/brunkage" \
  --username root \
  --password brunkage


  sqoop import \
--connect "jdbc:postgresql://postgresql:5432/brunkage" \
--username root \
--password brunkage \
--table employees \
--target-dir /brunkage \
--direct \
--m 1
