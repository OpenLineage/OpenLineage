package io.openlineage.sql;

import org.apache.commons.lang3.builder.HashCodeBuilder;

public class DbTableMeta {
    private final String database;
    private final String schema;
    private final String name;
    
    public DbTableMeta(String database, String schema, String name) {
        this.database = database;
        this.schema = schema;
        this.name = name;
    }

    public String database() {
        return database;
    }

    public String schema() {
        return schema;
    }

    public String name() {
        return name;
    }

    public String qualifiedName() {
        return String.format(
            "%s%s%s",
            database != null ? database + "." : "",
            schema != null ? schema + "." : "",
            name
        );
    }

    @Override
    public String toString() {
        return qualifiedName();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }

        if (!(o instanceof DbTableMeta)) {
            return false;
        }

        DbTableMeta other = (DbTableMeta)o;
        return qualifiedName().equals(other.qualifiedName());
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
            .append(database)
            .append(schema)
            .append(name)
            .toHashCode();
    }
}
