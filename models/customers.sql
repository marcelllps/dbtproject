-- Script criado para remover duplicatas da tabela "northwind.customers"
    -- Totalmente baseado em CTEs

-- A lógica de duplicação é por "company_name" e "contact_name", pois os dados tem problemas:
    -- um mesmo cliente aparece com mais de um ID diferente, então não adianta fazer distinct na coluna de ID direto

-- 1º) Selecionar todos os registros, ordenar e identificar pela coluna result com IDs iguais para clientes iguais (uniformização)
WITH SELECT_AND_ORDER_ALL AS
(
SELECT 
    *,
-- Window Function: Selecionar o primeiro valor para todas as linhas, tornando assim possível fazer distinct por ID
    -- Todas as linhas onde "company_name" e "contact_name" forem iguais receberão o mesmo ID (o primeiro da lista, como em um PROCV)
first_value(customer_id) over(
                            partition by company_name, contact_name
                            order by  company_name
                            rows between unbounded preceding and unbounded following
                            ) as  result

 -- Template Jinja para dinamizar o código
    -- Buscar no arquivo "source.yml", na seção "sources" a tabela "customers"
FROM {{source('sources', 'customers')}}
                            )

,

-- 2º) Selecionar os novos IDs uniformizados de forma distinta
SELECT_DISTINCT AS
(
    SELECT DISTINCT result FROM SELECT_AND_ORDER_ALL
)

,

-- 3º) Montar resultado final (Apenas IDs distintos) mas selecionando da tabela Source original usando o resultado do distinct como cláusula WHERE
SELECT_FINAL AS
(
    SELECT * FROM {{source('sources', 'customers')}}
    WHERE customer_id in (SELECT result FROM SELECT_DISTINCT)
)

-- 4º) Rodar e visualizar resultado final
SELECT * FROM SELECT_FINAL

-- Antes existiam 94 linhas. Agora apenas 91, pois 3 clientes estavam com o problema de duplicação por IDs diferentes