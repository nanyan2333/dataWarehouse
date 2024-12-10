SELECT src.local,SUM(src.price) AS total_price FROM sale.data AS src GROUP BY src.local;
