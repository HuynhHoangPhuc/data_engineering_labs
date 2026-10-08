{#
  Custom GENERIC test: fails for every row where the column is negative
  (or zero when allow_zero=false). Use it in YAML like:
      data_tests:
        - positive_value
        - positive_value:
            arguments: {allow_zero: false}
#}
{% test positive_value(model, column_name, allow_zero=true) %}
select {{ column_name }} as bad_value
from {{ model }}
where {{ column_name }} {{ '<' if allow_zero else '<=' }} 0
{% endtest %}
