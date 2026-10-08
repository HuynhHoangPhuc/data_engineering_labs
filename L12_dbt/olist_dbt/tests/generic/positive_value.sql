{#
  TODO (step 7): write a custom GENERIC test named positive_value.
  A generic test is a macro wrapped in {% test name(model, column_name, ...) %} ... {% endtest %}
  that SELECTs the failing rows (0 rows = pass).
  Requirements:
    * fail rows where the column is < 0
    * optional argument allow_zero (default true); when false also fail rows = 0
  Usage in YAML:
      data_tests:
        - positive_value
        - positive_value:
            arguments: {allow_zero: false}
#}
{% test positive_value(model, column_name, allow_zero=true) %}
select 1 as todo where false   -- replace me
{% endtest %}
