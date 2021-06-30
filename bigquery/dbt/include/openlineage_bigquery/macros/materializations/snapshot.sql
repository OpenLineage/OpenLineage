{% macro openlineage_bigquery__snapshot_hash_arguments(args) -%}
  to_hex(md5(concat({%- for arg in args -%}
    coalesce(cast({{ arg }} as string), ''){% if not loop.last %}, '|',{% endif -%}
  {%- endfor -%}
  )))
{%- endmacro %}

{% macro openlineage_bigquery__create_columns(relation, columns) %}
  {{ adapter.alter_table_add_columns(relation, columns) }}
{% endmacro %}

{% macro openlineage_bigquery__post_snapshot(staging_relation) %}
  -- Clean up the snapshot temp table
  {% do drop_relation(staging_relation) %}
{% endmacro %}
