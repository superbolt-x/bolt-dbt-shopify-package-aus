{%- set schema_name,
        order_table_name, 
        refund_table_name,
        adjustment_table_name,
        line_refund_table_name,
        transaction_table_name,
        shop_table_name
        = 'shopify_raw_aus',
        'order',
        'refund',
        'order_adjustment',
        'order_line_refund',
        'transaction',
        'shop' -%}

{%- set order_selected_fields = [
    "id",
    "currency",
    "shipping_address_country_code"
] -%}
        
{%- set refund_selected_fields = [
    "id",
    "order_id",
    "processed_at"
] -%}

{%- set adjustment_selected_fields = [
    "refund_id",
    "amount",
    "tax_amount",
    "kind"
] -%}

{%- set line_refund_selected_fields = [
    "refund_id",
    "quantity",
    "subtotal",
    "total_tax"
] -%}

{%- set transaction_selected_fields = [
    "refund_id",
    "subtotal",
    "total_tax"
] -%}

{%- set shop_selected_fields = [
    "currency"
] -%}

{%- set order_raw_tables = dbt_utils.get_relations_by_pattern('shopify_raw_aus%', 'order') -%}
{%- set refund_raw_tables = dbt_utils.get_relations_by_pattern('shopify_raw_aus%', 'refund') -%}
{%- set adjustment_raw_tables = dbt_utils.get_relations_by_pattern('shopify_raw_aus%', 'order_adjustment') -%}
{%- set line_refund_raw_tables = dbt_utils.get_relations_by_pattern('shopify_raw_aus%', 'order_line_refund') -%}
{%- set shop_raw_tables = dbt_utils.get_relations_by_pattern('shopify_raw_aus%', 'shop') -%}

WITH 
    {% if var('sho_aus_currency') == 'USD' -%}
    shop_raw_data AS 
    ({{ dbt_utils.union_relations(relations = shop_raw_tables) }}),
        
    currency AS
    (SELECT date, conversion_rate
    FROM shop_raw_data LEFT JOIN utilities.currency USING (currency)
    WHERE date <= current_date),
    {%- endif -%}

    {%- set conversion_rate = 1 if var('sho_aus_currency') != 'USD' else 'conversion_rate' %}

    -- To tackle the signal loss between Fivetran and Shopify transformations
    stellar_signal AS 
    (SELECT _fivetran_synced
    FROM {{ source('shopify_raw_aus', 'order') }}
    LIMIT 1
    ),

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

    refund_raw_data AS 
    ({{ dbt_utils.union_relations(relations = refund_raw_tables) }}),

    refund_staging AS 
    (SELECT 
        
        {% for field in refund_selected_fields -%}
        {{ get_shopify_clean_field(refund_table_name, field)}}
        {%- if not loop.last %},{% endif %}
        {% endfor %}

    FROM refund_raw_data
    ),

    adjustment_raw_data AS 
    ({{ dbt_utils.union_relations(relations = adjustment_raw_tables) }}),

    adjustment_staging AS 
    (SELECT 
        
        {% for field in adjustment_selected_fields -%}
        {{ get_shopify_clean_field(adjustment_table_name, field)}}
        {%- if not loop.last %},{% endif %}
        {% endfor %}

    FROM adjustment_raw_data
    ),

    line_refund_raw_data AS 
    ({{ dbt_utils.union_relations(relations = line_refund_raw_tables) }}),

    line_refund_staging AS 
    (SELECT 
        
        {% for field in line_refund_selected_fields -%}
        {{ get_shopify_clean_field(line_refund_table_name, field)}}
        {%- if not loop.last %},{% endif %}
        {% endfor %}

    FROM line_refund_raw_data
    ),

    line_refund AS 
    (SELECT 
        refund_id,
        COALESCE(SUM(refund_quantity),0) as quantity_refund, 
        COALESCE(SUM(refund_subtotal),0) as subtotal_refund,
        COALESCE(SUM(refund_total_tax),0) as total_tax_refund
    FROM line_refund_staging
    GROUP BY refund_id
    ),

    refund_adjustment AS
    (SELECT
        order_id,
        refund_id,
        processed_at as refund_date,
        CASE WHEN refund_kind ~* 'refund_discrepancy' THEN COALESCE(refund_amount,0) ELSE 0 END AS amount_discrepancy_refund,
        CASE WHEN refund_kind ~* 'refund_discrepancy' THEN COALESCE(refund_tax_amount,0) ELSE 0 END AS tax_amount_discrepancy_refund,
        CASE WHEN refund_kind ~* 'shipping_refund' THEN COALESCE(refund_amount,0) ELSE 0 END AS amount_shipping_refund,
        CASE WHEN refund_kind ~* 'shipping_refund' THEN COALESCE(refund_tax_amount,0) ELSE 0 END AS tax_amount_shipping_refund
        FROM refund_staging LEFT JOIN adjustment_staging USING(refund_id)
    ),

    refund_adjustment_line_refund AS 
    (SELECT 
        order_id,
        refund_id,
        refund_date,
        COALESCE(quantity_refund,0) AS quantity_refund,
        amount_discrepancy_refund,
        tax_amount_discrepancy_refund,
        amount_shipping_refund,
        tax_amount_shipping_refund,
        COALESCE(subtotal_refund,0) AS subtotal_refund,
        COALESCE(total_tax_refund,0) AS total_tax_refund
        FROM refund_adjustment
        LEFT JOIN line_refund USING(refund_id)
        --LEFT JOIN shopify_raw.order ON (order_id = id)
        --WHERE cancelled_at is null
    )

    SELECT 
        order_id, 
        refund_id,
        refund_date,
        quantity_refund,
        shipping_address_country_code,
        SUM(case when order_staging.currency = 'USD' then amount_discrepancy_refund else amount_discrepancy_refund::float/{{ conversion_rate }}::float end) AS amount_discrepancy_refund,
        case when order_staging.currency = 'USD' then tax_amount_discrepancy_refund else tax_amount_discrepancy_refund::float/{{ conversion_rate }}::float end AS tax_amount_discrepancy_refund,
        SUM(case when order_staging.currency = 'USD' then amount_shipping_refund else amount_shipping_refund::float/{{ conversion_rate }}::float end) AS amount_shipping_refund,
        SUM(case when order_staging.currency = 'USD' then tax_amount_shipping_refund else tax_amount_shipping_refund::float/{{ conversion_rate }}::float end) AS tax_amount_shipping_refund,
        case when order_staging.currency = 'USD' then subtotal_refund else subtotal_refund::float/{{ conversion_rate }}::float end AS subtotal_refund,
        case when order_staging.currency = 'USD' then total_tax_refund else total_tax_refund::float/{{ conversion_rate }}::float end AS total_tax_refund
    FROM refund_adjustment_line_refund
    LEFT JOIN order_staging USING(order_id)
    {%- if var('sho_aus_currency') == 'USD' %}
    LEFT JOIN currency ON refund_adjustment_line_refund.refund_date::date = currency.date
    {%- endif %}
    GROUP BY 1,2,3,4,5,7,10,11
