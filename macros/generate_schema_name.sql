{#
  Where a model's dataset comes from.

  prod target : the +schema from dbt_project.yml, used AS-IS -> the layer
                datasets of the redesign plan (flights_l1, hotels_l2, ...).
  any other   : everything lands in the target's own dataset (dev ->
                dbt_learning), so development and CI stay sandboxed in one
                place no matter which layer a model belongs to.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if target.name == 'prod' and custom_schema_name is not none -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ target.schema }}
    {%- endif -%}
{%- endmacro %}
