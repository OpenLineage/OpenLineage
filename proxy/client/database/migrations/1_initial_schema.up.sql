CREATE TABLE partitions (
    name       text     NOT NULL,
    size       integer  NOT NULL,
    created_at datetime NOT NULL,
    is_current boolean  NOT NULL
);

CREATE TABLE checkpoint (
    partition_id integer NOT NULL,
    offset       integer NOT NULL,
    FOREIGN KEY (partition_id) REFERENCES partitions (rowid)
);
