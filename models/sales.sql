

WITH 

sales AS (SELECT * FROM {{ ref('stg_sales') }} ) 
 ,product AS (SELECT * FROM {{ ref('stg_product') }} )

SELECT
  s.date_date
  ### Key ###
  ,s.orders_id
  ,s.products_id
  ###########
	-- qty --
	,s.qty
  -- turnover --
  ,s.turnover
  -- cost --
  ,p.purchase_price
	,ROUND(s.qty*CAST(p.purchase_price AS FLOAT64),2) AS purchase_cost
	-- margin --
	,
     ROUND( s.turnover - s.qty*CAST(p.purchase_price AS FLOAT64), 2 )
AS product_margin
	,ROUND(s.turnover-s.qty*CAST(p.purchase_price AS FLOAT64),2) AS margin
    ,
   ROUND( SAFE_DIVIDE( (s.turnover - s.qty*CAST(p.purchase_price AS FLOAT64)) , s.turnover ) , 2)
 as product_margin_percent
    

FROM sales s
INNER JOIN product p ON s.products_id = p.products_id