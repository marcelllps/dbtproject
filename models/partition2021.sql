-- Partição dos dados por ano da ordem

SELECT * FROM {{ref('joins')}}
WHERE DATE_PART(YEAR, order_date) = 2021