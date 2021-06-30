
{% macro openlineage_bigquery__handle_existing_table(full_refresh, old_relation) %}
    {%- if full_refresh -%}
      {{ adapter.drop_relation(old_relation) }}
    {%- else -%}
      {{ exceptions.relation_wrong_type(old_relation, 'view') }}
    {%- endif -%}
{% endmacro %}


{% materialization view, adapter='openlineage_bigquery' -%}

    {%- set openlineage_run_id = adapter.emit_start(model, run_started_at.isoformat()) -%}

    {% set to_return = create_or_replace_view(run_outside_transaction_hooks=False) %}

    {% set target_relation = this.incorporate(type='view') %}
    {% do persist_docs(target_relation, model) %}

    {% if config.get('grant_access_to') %}
      {% for grant_target_dict in config.get('grant_access_to') %}
        {% do adapter.grant_access_to(this, 'view', None, grant_target_dict) %}
      {% endfor %}
    {% endif %}

    {%- set _ = adapter.emit_complete(openlineage_run_id) -%}

    {% do return(to_return) %}

{%- endmaterialization %}
