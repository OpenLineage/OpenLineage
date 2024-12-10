#!/bin/bash

psql -d postgres -h postgres -U postgres -c "CREATE TABLE IF NOT EXISTS source_event(event_uid varchar(255) not null primary key, content text, created_at timestamp);"
psql -d postgres -h postgres -U postgres -c "CREATE TABLE IF NOT EXISTS sink_event(event_uid varchar(255) not null primary key, content text, created_at timestamp);"
psql -d postgres -h postgres -U postgres -c "INSERT INTO source_event (event_uid, content, created_at) VALUES ('uuid1', 'started', now());"
psql -d postgres -h postgres -U postgres -c "INSERT INTO source_event (event_uid, content, created_at) VALUES ('uuid2', 'ended', now());"