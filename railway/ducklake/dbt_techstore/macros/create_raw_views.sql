{% macro create_raw_views() %}

{% if execute %}

{% set base = '/app/output/datalake/raw/sftp' %}

{% set tables = [
    'vtex_pedido',
    'meli_pedido',
    'meli_pickup',
    'meli_shipping',
    'type_6',
    'type_7',
    'type_8',
    'garbarino_pedido'
] %}

{% call statement('create_raw_schema', fetch_result=False) %}
    CREATE SCHEMA IF NOT EXISTS raw;
{% endcall %}

{% for tbl in tables %}
    {% set pattern = base ~ '/' ~ tbl ~ '/*/*/*/data.parquet' %}
    {% set result = run_query("SELECT count(*) AS n FROM glob('" ~ pattern ~ "')") %}
    {% if result.columns[0].values()[0] > 0 %}
        {{ log("RAW view: " ~ tbl, info=True) }}
        {% call statement('create_view_' ~ tbl, fetch_result=False) %}
            CREATE OR REPLACE VIEW raw.{{ tbl }} AS
            SELECT * FROM read_parquet(
                '{{ pattern }}',
                hive_partitioning=true,
                union_by_name=true
            );
        {% endcall %}
    {% else %}
        {{ log("SKIP (sin parquets): " ~ tbl, info=True) }}
    {% endif %}
{% endfor %}

{% endif %}

{% endmacro %}
