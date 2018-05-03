
//Calculates the average price per stable region for Norhteast1 and Northeast2 regions

SELECT MIN(AVERAGE_PRICE) AS MIN_AVERAGE_PRICE, northeast1.INSTANCE_TYPE, northeast1.OS
 FROM
 [Average_price_Output_Region.Average_price_instanceType_apNortheast1] AS northeast1
 LEFT OUTER JOIN EACH
 [Average_price_Output_Region.Average_price_instanceType_apNortheast2] AS northeast2
 ON northeast1.INSTANCE_TYPE = northeast2.INSTANCE_TYPE
 AND northeast1.OS = northeast2.OS
 GROUP BY northeast1.INSTANCE_TYPE, northeast1.OS;