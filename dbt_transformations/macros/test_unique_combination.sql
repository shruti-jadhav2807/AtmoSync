{% test unique_combination(model, combination_of_columns) %}
SELECT
    {{ combination_of_columns | join(', ') }}
FROM {{ model }}
GROUP BY {{ combination_of_columns | join(', ') }}
HAVING COUNT(*) > 1
{% endtest %}
