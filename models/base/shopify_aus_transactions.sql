{%- set selected_fields = [
    "id",
    "order_id",
    "refund_id",
    "amount",
    "created_at",
    "processed_at",
    "message",
    "kind",
    "status"
] -%}

{%- set shop_selected_fields = [
    "currency"
] -%}

{%- set schema_name,
        table_name, shop_table_name
        = 'shopify_raw_aus', 'transaction', 'shop' -%}

WITH 
    {% if var('currency') == 'USD' -%}
    shop_raw_data AS 
    ({{ dbt_utils.union_relations(relations = shop_raw_tables) }}),
        
    currency AS
    (SELECT date, conversion_rate
    FROM shop_raw_data LEFT JOIN utilities.currency USING (currency)
    WHERE date <= current_date),
    {%- endif -%}

    {%- set conversion_rate = 1 if var('currency') != 'USD' else 'conversion_rate' %}
    
    raw_table AS 
    (SELECT 

        {% for column in selected_fields -%}
        {{ get_shopify_clean_field(table_name, column)}}
        {%- if not loop.last %},{% endif %}
        {% endfor %}

    FROM {{ source(schema_name, table_name) }}),

    staging AS 
    (SELECT 
        order_id, 
        created_at::date as transaction_date,
        COALESCE(SUM(CASE WHEN kind in ('sale','authorization') THEN transaction_amount END),0)::float/{{ conversion_rate }}::float as paid_by_customer,
        COALESCE(SUM(CASE WHEN kind = 'refund' THEN transaction_amount END),0)::float/{{ conversion_rate }}::float as refunded
    FROM raw_table
    {%- if var('currency') == 'USD' %}
    LEFT JOIN currency ON raw_table.created_at::date = currency.date
    {%- endif %}
    WHERE status = 'success'
    GROUP BY order_id, transaction_date)

SELECT *,
    order_id||'_'||transaction_date as unique_key
FROM staging
