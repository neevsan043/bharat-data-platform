{% test accepted_range(model, column_name, min_value=None, max_value=None) %}
select *
from {{ model }}
where
  {% if min_value is not none %}
    {{ column_name }} < {{ min_value }}
    {% if max_value is not none %} or {% endif %}
  {% endif %}
  {% if max_value is not none %}
    {{ column_name }} > {{ max_value }}
  {% endif %}
{% endtest %}
