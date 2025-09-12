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

{%- set order_selected_fields = [
    "id",
    "currency"
] -%}

{%- set schema_name,
        table_name, order_table_name
        = 'shopify_raw_aus', 'transaction', 'order' -%}

{%- set transaction_raw_tables = dbt_utils.get_relations_by_pattern('shopify_raw_aus%', 'transaction') -%}
{%- set order_raw_tables = dbt_utils.get_relations_by_pattern('shopify_raw_aus%', 'order') -%}

WITH 
    transaction_raw_data AS 
    ({{ dbt_utils.union_relations(relations = transaction_raw_tables) }}),

    order_raw_data AS 
    ({{ dbt_utils.union_relations(relations = order_raw_tables) }}),

    transactions AS 
    (SELECT 
        {% for column in selected_fields -%}
        {{ get_shopify_clean_field(table_name, column)}}
        {%- if not loop.last %},{% endif %}
        {% endfor %}
    FROM transaction_raw_data
    ),

    orders AS 
    (SELECT 
        {% for column in order_selected_fields -%}
        {{ get_shopify_clean_field(order_table_name, column)}}
        {%- if not loop.last %},{% endif %}
        {% endfor %}
    FROM order_raw_data
    ),

    {% if var('sho_aus_currency') in ['USD','AUD'] -%}
    currency AS
    (SELECT 
        date,
        currency,
        conversion_rate
    FROM utilities.currency
    WHERE date <= current_date
      AND currency = 'AUD'),
    {%- endif -%}

    staging AS 
    (SELECT 
        t.order_id, 
        t.processed_at::date as transaction_date,

        -- paid_by_customer
        COALESCE(SUM(
            CASE WHEN t.kind in ('sale','authorization') THEN 
                CASE 
                    WHEN o.currency = '{{ var("sho_aus_currency") }}' THEN t.transaction_amount::float
                    WHEN o.currency = 'USD' AND '{{ var("sho_aus_currency") }}' = 'AUD' THEN t.transaction_amount::float * c.conversion_rate
                    WHEN o.currency = 'AUD' AND '{{ var("sho_aus_currency") }}' = 'USD' THEN t.transaction_amount::float / NULLIF(c.conversion_rate,0)
                END
            END
        ),0) as paid_by_customer,

        -- refunded
        COALESCE(SUM(
            CASE WHEN t.kind = 'refund' THEN 
                CASE 
                    WHEN o.currency = '{{ var("sho_aus_currency") }}' THEN t.transaction_amount::float
                    WHEN o.currency = 'USD' AND '{{ var("sho_aus_currency") }}' = 'AUD' THEN t.transaction_amount::float * c.conversion_rate
                    WHEN o.currency = 'AUD' AND '{{ var("sho_aus_currency") }}' = 'USD' THEN t.transaction_amount::float / NULLIF(c.conversion_rate,0)
                END
            END
        ),0) as refunded

    FROM transactions t
    LEFT JOIN orders o ON t.order_id = o.id
    {% if var('sho_aus_currency') in ['USD','AUD'] %}
        LEFT JOIN currency c ON t.processed_at::date = c.date
    {% endif %}
    WHERE t.status = 'success'
    GROUP BY t.order_id, transaction_date
    )

SELECT *,
    order_id||'_'||transaction_date as unique_key
FROM staging
