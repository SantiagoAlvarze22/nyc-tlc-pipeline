SELECT tz.Zone, ROUND(AVG(ds.total_amount),2) as "promedio_ingreso"
FROM datos_silver ds 
LEFT JOIN taxi_zone_lookup tz on tz.locationid = ds.pulocationid
GROUP BY tz.zone
ORDER BY promedio_ingreso DESC

