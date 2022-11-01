/* Criação de uma tabela fato combinando várias outras tabelas, realizando os Joins por etapas usando CTEs */
    -- Lógica: Ir extraindo um Dataframe por vez e ir juntando (Daria para fazer em apenas um SELECT gigante, mas optou=se por CTE em etapas)

-- Unir products, suppliers e categories em um Dataframe (prod)
WITH prod AS
(
    SELECT
        ct.category_name,
        sp.company_name AS supplier,
        pd.product_name,
        pd.unit_price,
        pd.product_id
    FROM {{source('sources', 'products')}} AS pd
    LEFT JOIN {{source('sources', 'suppliers')}} AS sp ON (pd.supplier_id = sp.supplier_id)
    LEFT JOIN {{source('sources', 'categories')}} AS ct ON (pd.category_id = ct.category_id)    
),

-- Unir v_orderdetails (Já processado) com Dataframe prod
orddetai AS
(
    SELECT
        pd.*,
        od.order_id,
        od.quantity,
        od.discount_calc as discount
    FROM {{ref('orderdetails')}} AS od 
        /* Template Jinja para recuperar um script SQL já processado */
    LEFT JOIN prod pd ON (od.product_id = pd.product_id)
),

-- Unir DF "orddetai" com "orders", "employee", "shippers" e "costumers"
ordrs AS
(
    SELECT
        ord.order_date,
        ord.order_id,
        cs.company_name AS customer,
        em.name AS employee,
        em.age,
        em.lengthofservice
    FROM {{source('sources', 'orders')}} AS ord
    LEFT JOIN {{ref('customers')}} AS cs ON (ord.customer_id = cs.customer_id)
    LEFT JOIN {{ref('employees')}} AS em ON (ord.employee_id = em.employee_id)
    LEFT JOIN {{source('sources', 'shippers')}} AS sh ON (ord.ship_via = sh.shipper_id)
),

-- Dataframe final,unindo ordens agregadas com detalhes de ordens
finaljoin as
(
    SELECT
        od.*,
        ord.order_date,
        ord.customer,
        ord.employee,
        ord.age,
        ord.lengthofservice
        FROM orddetai AS od
        inner join ordrs AS ord on (od.order_id = ord.order_id)
)

-- Exibir resultado
SELECT * FROM finaljoin