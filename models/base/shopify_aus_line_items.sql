{%- set schema_name,
        item_table_name, 
        item_fund_table_name,
        order_table_name,
        item_tax_table_name
        = 'shopify_raw_aus', 'order_line', 'order_line_refund', 'order', 'tax_line' -%}

{%- set item_selected_fields = [
    "order_id",
    "id",
    "product_id",
    "variant_id",
    "title",
    "variant_title",
    "name",
    "price",
    "pre_tax_price",
    "total_discount",
    "quantity",
    "sku",
    "fulfillable_quantity",
    "fulfillment_status",
    "gift_card",
    "index"
] -%}

{%- set item_refund_selected_fields = [
    "order_line_id",
    "refund_id",
    "quantity",
    "subtotal"
] -%}

{%- set order_selected_fields = [
    "id",
    "currency"
] -%}

{%- set tax_selected_fields = [
    "order_line_id",
    "price"
] -%}

{%- set order_line_raw_tables = dbt_utils.get_relations_by_pattern('shopify_raw_aus%', 'order_line') -%}
{%- set order_line_refund_raw_tables = dbt_utils.get_relations_by_pattern('shopify_raw_aus%', 'order_line_refund') -%}
{%- set order_raw_tables = dbt_utils.get_relations_by_pattern('shopify_raw_aus%', 'order') -%}
{%- set tax_line_raw_tables = dbt_utils.get_relations_by_pattern('shopify_raw_aus%', 'tax_line') -%}

WITH 
    {% if var('sho_aus_currency') in ['USD','AUD'] -%}
    currency AS
    (
        SELECT 
            date,
            currency,
            conversion_rate
        FROM utilities.currency
        WHERE date <= current_date
          AND currency = 'AUD'
    ),
    {%- endif -%}

    order_raw_data AS 
    ({{ dbt_utils.union_relations(relations = order_raw_tables) }}),

    order_staging AS 
    (SELECT 
        {% for field in order_selected_fields -%}
        {{ get_shopify_clean_field(order_table_name, field)}}
        {%- if not loop.last %},{% endif %}
        {% endfor %}
    FROM order_raw_data
    ),

    order_line_raw_data AS 
    ({{ dbt_utils.union_relations(relations = order_line_raw_tables) }}),

    items AS 
    (SELECT 
        {% for column in item_selected_fields -%}
        {{ get_shopify_clean_field(item_table_name, column)}}
        {%- if not loop.last %},{% endif %}
        {% endfor %}
    FROM order_line_raw_data
    ),

    tax_line_raw_data AS 
    ({{ dbt_utils.union_relations(relations = tax_line_raw_tables) }}),

    tax_line_raw AS 
    (SELECT 
        {% for column in tax_selected_fields -%}
        {{ get_shopify_clean_field(item_tax_table_name, column)}}
        {%- if not loop.last %},{% endif %}
        {% endfor %}
    FROM tax_line_raw_data
    ),

    tax_line AS 
    (SELECT 
        order_line_id,
        SUM(price) as tax_price
    FROM tax_line_raw
    GROUP BY order_line_id
    ),

    order_line_refund_raw_data AS 
    ({{ dbt_utils.union_relations(relations = order_line_refund_raw_tables) }}),

    refund_raw AS 
    (SELECT 
        {% for column in item_refund_selected_fields -%}
        {{ get_shopify_clean_field(item_fund_table_name, column)}}
        {%- if not loop.last %},{% endif %}
        {% endfor %}
    FROM order_line_refund_raw_data
    ),

    refund AS 
    (SELECT 
        order_line_id,
        SUM(refund_quantity) as refund_quantity,
        SUM(refund_subtotal) as refund_subtotal
    FROM refund_raw
    GROUP BY order_line_id
    )

SELECT 
    items.order_id,
    items.order_line_id,
    items.product_id,
    items.variant_id,
    items.product_title,
    items.variant_title,
    items.item_title,
    items.sku,
    items.fulfillable_quantity,
    items.fulfillment_status,
    items.gift_card,
    items.index,
    items.quantity,
    refund.refund_quantity,
    order_staging.currency AS original_currency,

    -- normalized monetary fields
    CASE 
        WHEN order_staging.currency = '{{ var("sho_aus_currency") }}' THEN (price::float*quantity::float-tax_price::float)
        WHEN order_staging.currency = 'USD' AND '{{ var("sho_aus_currency") }}' = 'AUD' THEN (price::float*quantity::float-tax_price::float) * currency.conversion_rate
        WHEN order_staging.currency = 'AUD' AND '{{ var("sho_aus_currency") }}' = 'USD' THEN (price::float*quantity::float-tax_price::float) / NULLIF(currency.conversion_rate,0)
    END AS pre_tax_price,

    CASE 
        WHEN order_staging.currency = '{{ var("sho_aus_currency") }}' THEN line_total_discount::float
        WHEN order_staging.currency = 'USD' AND '{{ var("sho_aus_currency") }}' = 'AUD' THEN line_total_discount::float * currency.conversion_rate
        WHEN order_staging.currency = 'AUD' AND '{{ var("sho_aus_currency") }}' = 'USD' THEN line_total_discount::float / NULLIF(currency.conversion_rate,0)
    END AS line_total_discount,

    quantity - COALESCE(refund_quantity,0) AS net_quantity,

    -- line price after conversion
   coalesce((pre_tax_price::float + line_total_discount::float)/NULLIF(quantity::float,0),0) AS price,

    -- refund subtotal normalized
    CASE 
        WHEN order_staging.currency = '{{ var("sho_aus_currency") }}' THEN COALESCE(refund_subtotal,0)
        WHEN order_staging.currency = 'USD' AND '{{ var("sho_aus_currency") }}' = 'AUD' THEN COALESCE(refund_subtotal,0) * currency.conversion_rate
        WHEN order_staging.currency = 'AUD' AND '{{ var("sho_aus_currency") }}' = 'USD' THEN COALESCE(refund_subtotal,0) / NULLIF(currency.conversion_rate,0)
    END AS refund_subtotal,

    -- net subtotal normalized
    (COALESCE((pre_tax_price::float + line_total_discount::float)/NULLIF(quantity::float,0),0) * quantity) - COALESCE(refund_subtotal,0) AS net_subtotal,

    '{{ var("sho_aus_currency") }}' as currency,
    items.order_line_id as unique_key

FROM items 
LEFT JOIN refund USING(order_line_id)
LEFT JOIN tax_line USING(order_line_id)
LEFT JOIN order_staging ON items.order_id = order_staging.order_id
{% if var('sho_aus_currency') in ['USD','AUD'] %}
    LEFT JOIN currency ON CURRENT_DATE = currency.date
{% endif %}
