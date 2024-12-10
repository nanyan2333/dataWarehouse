SELECT concat('DROP TABLE IF EXISTS sale.', name)
FROM system.tables
WHERE database = 'sale';
