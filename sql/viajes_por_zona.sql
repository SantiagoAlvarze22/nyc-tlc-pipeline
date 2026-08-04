SELECT tz.Zone, tz.borough, COUNT(*) as "Cantidad"
FROM datos_silver ds 
LEFT JOIN taxi_zone_lookup tz on tz.locationid = ds.pulocationid
GROUP BY tz.zone, tz.borough
ORDER BY Cantidad DESC