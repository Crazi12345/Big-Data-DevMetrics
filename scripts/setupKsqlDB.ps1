#!/bin/bash

#Enter KsqlDB cli
kubectl exec --stdin --tty deployment/kafka-ksqldb-cli -- ksql http://kafka-ksqldb-server:8088

#Also process data already in the topics
SET 'auto.offset.reset' = 'earliest';

#Drop joined_stream and delete the topic
DROP STREAM JOINED_STREAM DELETE TOPIC;

#Drop streams (no topics exist for these)
DROP STREAM users_stream;
DROP STREAM questions_stream;

#Create new questions stream with relevant values
CREATE STREAM questions_stream (
    Id INT,
    PostTypeId INT,
    CreationDate VARCHAR,
    ViewCount INT,
    OwnerUserId INT,
    Tags VARCHAR
) WITH (
KAFKA_TOPIC = 'Question',
VALUE_FORMAT = 'JSON'
);

#Create new users stream with relevant values
CREATE STREAM users_stream (
    Id INT,
    Reputation INT,
    LastAccessDate VARCHAR,
    Location VARCHAR
) WITH (
    KAFKA_TOPIC = 'Users',
    VALUE_FORMAT = 'JSON'
);

#Join the streams
#The join only looks at records that have entered the two streams within 7 days of each other
#Will only output values where the user record has a matching question record - i.e. only active users
#The KEY in kafka is set to null 
CREATE STREAM joined_stream AS
SELECT 
    us.Id AS UserId,
    qs.Id AS QuestionId,
    qs.PostTypeId AS PostTypeId,
    qs.CreationDate AS QuestionCreationDate,
    qs.ViewCount AS QuestionViewCount,
    qs.Tags AS QuestionTags,
    us.Reputation AS UserReputation,
    us.LastAccessDate AS UserLastAccessDate,
    us.Location AS UserLocation
FROM users_stream us
INNER JOIN questions_stream qs
WITHIN 7 DAYS
ON us.Id = qs.OwnerUserId
PARTITION BY NULL 
EMIT CHANGES;

#Print the first 10 values to console as a test :)
PRINT 'JOINED_STREAM' FROM BEGINNING LIMIT 10;


