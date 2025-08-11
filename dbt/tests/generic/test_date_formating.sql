{% test date_formating(model, column_name) %}

select *
from {{ model }}
where try_to_date({{ column_name }}::string, 'YYYY-MM-DD') is null

{% endtest %}
