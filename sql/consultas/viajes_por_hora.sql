SELECT EXTRACT(HOUR FROM tpep_pickup_datetime) AS hour, COUNT(*) AS cantidad
FROM datos_silver 
GROUP BY 1
ORDER BY cantidad DESC;