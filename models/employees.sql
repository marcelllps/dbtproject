-- Testar conexão e dados
-- SELECT * FROM  {{source('sources', 'employees')}}

/* Criar Colunas Calculadas:
• Age (birth_date)
• LengthofService (hire_date)
• Name (last_name + first_name)
*/

WITH calc_employees AS
(
SELECT
    *,
    DATEDIFF(YEAR, birth_date, CURRENT_DATE) AS Age,
    DATEDIFF(YEAR, hire_date, CURRENT_DATE) AS LengthofService,
    first_name || ' ' || last_name AS Name
FROM  {{source('sources', 'employees')}}
)

SELECT * FROM calc_employees