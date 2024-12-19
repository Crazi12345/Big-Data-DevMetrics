#!/bin/bash

#Set up PostgreSQL for metastore
helm install hive-postgresql \
  --version=12.1.5 \
  --set auth.username=root \
  --set auth.password=pwd1234 \
  --set auth.database=hive \
  --set primary.extendedConfiguration="password_encryption=md5" \
  --repo https://charts.bitnami.com/bitnami \
  postgresql

#Apply yaml files
kubectl apply -f ../services/hive-metastore.yaml
kubectl apply -f ../services/hive/hive.yaml

#Portforward the UI
# kubectl port-forward svc/hiveserver2 10002:10002

#Connect to database using tool such as Data grip
#Run the following: 
<# drop table users;
CREATE EXTERNAL TABLE users (
    hirable BOOLEAN,
    public_repos BIGINT,
    is_suspicious BOOLEAN,
    updated_at STRING,
    id BIGINT,
    blog STRING,
    followers BIGINT,
    location STRING,
    follower_list int,
    type STRING,
    commit_list int,
    bio STRING,
    commits BIGINT,
    company STRING,
    following_list ARRAY<STRING>,
    public_gists BIGINT,
    name STRING,
    created_at STRING,
    email STRING,
    followings BIGINT,
    login STRING,
    repo_list int
)
    row format delimited
    fields terminated by ','
    location 'hdfs://namenode:9000/user';

#>
