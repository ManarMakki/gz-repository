select 
orders_id,
logCost as log_cost,
COALESCE(shipping_fee, shipping_fee_1) AS shipping_fee,
CAST(ship_cost AS float64) AS ship_cost
from gz_raw_data.raw_gz_ship