{%- set schema_name,
        order_table_name,
        item_table_name, 
        item_fund_table_name,
        shop_table_name
        = 'shopify_raw_aus', 'order', 'order_line', 'order_line_refund', 'shop' -%}

{%- set order_selected_fields = [
    "id",
    "created_at"
] -%}
        
{%- set item_selected_fields = [
    "order_id",
    "id",
    "product_id",
    "variant_id",
    "title",
    "variant_title",
    "name",
    "price",
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

{%- set shop_selected_fields = [
    "currency"
] -%}

{%- set order_raw_tables = dbt_utils.get_relations_by_pattern('shopify_raw_aus%', 'order') -%}
{%- set order_line_raw_tables = dbt_utils.get_relations_by_pattern('shopify_raw_aus%', 'order_line') -%}
{%- set order_line_refund_raw_tables = dbt_utils.get_relations_by_pattern('shopify_raw_aus%', 'order_line_refund') -%}
{%- set shop_raw_tables = dbt_utils.get_relations_by_pattern('shopify_raw_aus%', 'shop') -%}

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
        
    order_line_raw_data AS 
    ({{ dbt_utils.union_relations(relations = order_line_raw_tables) }}),

    order_raw_data AS 
    ({{ dbt_utils.union_relations(relations = order_raw_tables) }}),

    orders AS 
    (SELECT 

        {% for field in order_selected_fields -%}
        {{ get_shopify_clean_field(order_table_name, field)}}
        {%- if not loop.last %},{% endif %}
        {% endfor %}

    FROM order_raw_data
    ),
        
    items AS 
    (SELECT 

        {% for column in item_selected_fields -%}
        {{ get_shopify_clean_field(item_table_name, column)}}
        {%- if not loop.last %},{% endif %}
        {% endfor %}

    FROM order_line_raw_data
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

{%- set item_fields = [
    "order_line_id",
    "order_id",
    "product_id",
    "variant_id",
    "product_title",
    "variant_title",
    "item_title",    
    "price",
    "quantity",
    "sku",
    "fulfillable_quantity",
    "fulfillment_status",
    "gift_card",
    "index",
    "refund_quantity",
    "refund_subtotal" 
] -%}
        
SELECT 
    {%- for field in item_fields -%}
        {%- if ('price' in field or 'refund' in field) %}
        "{{ field }}"::float/{{ conversion_rate }}::float as "{{ field }}",
        {%- else %}
        "{{ field }}",
        {%- endif -%}
    {%- endfor %}
    quantity - refund_quantity as net_quantity,
    price * quantity - refund_subtotal as net_subtotal,
    order_line_id as unique_key
FROM items 
LEFT JOIN refund USING(order_line_id)
{%- if var('currency') == 'USD' %}
    LEFT JOIN orders USING (order_id)
    LEFT JOIN currency ON orders.created_at::date = currency.date
{%- endif %}
