{#
  By default dbt builds custom schemas as <target_schema>_<custom_schema> (e.g. analytics_staging).
  For a teaching project we want clean names in dev/prod: staging, intermediate, marts, seeds, snapshots.
  BUT a CI run must never overwrite those tables, so for the `ci` target every custom schema gets the
  target schema as a prefix: ci_staging, ci_marts, ci_seeds, ci_snapshots, ...
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
  {%- if custom_schema_name is none -%} {{ target.schema }}
  {%- elif target.name == 'ci' -%} {{ target.schema }}_{{ custom_schema_name | trim }}
  {%- else -%} {{ custom_schema_name | trim }}
  {%- endif -%}
{%- endmacro %}
