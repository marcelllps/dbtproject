-- Testar dados
    -- SELECT * FROM  {{source('sources', 'order_details')}}
    -- SELECT * FROM  {{source('sources', 'products')}}


/*
Criar Colunas Calculadas:
• total = Unit_price * quantity
• discount = Total – (product.unitprice * 
quantity)
*/

SELECT
    od.order_id,
    od.product_id,
    od.unit_price,
    od.quantity,
    pr.product_name,
    pr.supplier_id,
    pr.category_id,
    od.unit_price * od.quantity AS total,
    total - (pr.unit_price * od.quantity) as discount_calc
FROM {{source('sources', 'order_details')}} AS od
LEFT JOIN {{source('sources', 'products')}} AS pr ON (od.product_id = pr.product_id)
