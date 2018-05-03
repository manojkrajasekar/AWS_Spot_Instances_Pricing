//
SELECT MIN(PRICE) AS MIN_PRICE, INSTANCE_TYPE, OS
FROM
 [awsdata.apNortheast1]     
GROUP BY INSTANCE_TYPE, OS;


//
SELECT MIN(PRICE) AS MIN_PRICE, INSTANCE_TYPE, OS
FROM
 [awsdata.apNortheast2]      
GROUP BY INSTANCE_TYPE, OS;


//
SELECT MIN(PRICE) AS MIN_PRICE, INSTANCE_TYPE, OS
FROM
 [awsdata.apSouth1]     
GROUP BY INSTANCE_TYPE, OS;


//
SELECT MIN(PRICE) AS MIN_PRICE, INSTANCE_TYPE, OS
FROM
 [awsdata.apSouthEast1]     
GROUP BY INSTANCE_TYPE, OS;


//
SELECT MIN(PRICE) AS MIN_PRICE, INSTANCE_TYPE, OS
FROM
 [awsdata.apSouthEast2]     
GROUP BY INSTANCE_TYPE, OS;


//
SELECT MIN(PRICE) AS MIN_PRICE, INSTANCE_TYPE, OS
FROM
 [awsdata.saEast1]     
GROUP BY INSTANCE_TYPE, OS;


//
SELECT MIN(PRICE) AS MIN_PRICE, INSTANCE_TYPE, OS
FROM
 [awsdata.caCentral1]          
GROUP BY INSTANCE_TYPE, OS;


//
SELECT MIN(PRICE) AS MIN_PRICE, INSTANCE_TYPE, OS
FROM
 [awsdata.euCentral1]          
GROUP BY INSTANCE_TYPE, OS;


//
SELECT MIN(PRICE) AS MIN_PRICE, INSTANCE_TYPE, OS
FROM
 [awsdata.euWest1]            
GROUP BY INSTANCE_TYPE, OS;



//
SELECT MIN(MIN_PRICE) AS MINIMUM_PRICE, northeast1.INSTANCE_TYPE, northeast1.OS
 FROM
 [Minimum_price_Output_Region.Minimum_Price_Output_apNortheast1]  AS northeast1
 LEFT OUTER JOIN EACH
  [Minimum_price_Output_Region.Minimum_Price_Output_apNortheast2] AS northeast2
    ON northeast1.INSTANCE_TYPE = northeast2.INSTANCE_TYPE
      AND northeast1.OS = northeast2.OS
 GROUP BY northeast1.INSTANCE_TYPE, northeast1.OS;

 
// This query is executed to find the region which has the minimum price of a particular instace type. 
 SELECT AVG(MIN_PRICE) AS MINIMUM_PRICE, northeastone.INSTANCE_TYPE, northeastone.OS
 FROM
 [Minimum_price_Output_Region.Minimum_Price_Output_apNortheast1]  AS northeastone
 LEFT OUTER JOIN EACH
  [Minimum_price_Output_Region.Minimum_Price_Output_apNortheast2] AS northeasttwo
    ON northeastone.INSTANCE_TYPE = northeasttwo.INSTANCE_TYPE
      AND northeastone.OS = northeasttwo.OS
      LEFT OUTER JOIN EACH
        [Minimum_price_Output_Region.Minimum_Price_Output_apSouth1] AS ApSouthone
          ON northeastone.INSTANCE_TYPE = ApSouthone.INSTANCE_TYPE
            AND northeastone.OS = ApSouthone.OS
            LEFT OUTER JOIN EACH
            [Minimum_price_Output_Region.Minimum_Price_Output_apSouthEast1] AS ApSouthEastone
              ON northeastone.INSTANCE_TYPE = ApSouthEastone.INSTANCE_TYPE
                AND northeastone.OS = ApSouthEastone.OS
                LEFT OUTER JOIN EACH
                [Minimum_price_Output_Region.Minimum_Price_Output_apSouthEast2] AS ApSouthEasttwo
                  ON northeastone.INSTANCE_TYPE = ApSouthEasttwo.INSTANCE_TYPE
                  AND northeastone.OS = ApSouthEasttwo.OS
                  LEFT OUTER JOIN EACH
                  [Minimum_price_Output_Region.Minimum_Price_Output_caCentral1] AS caCentralone
                    ON northeastone.INSTANCE_TYPE = caCentralone.INSTANCE_TYPE
                    AND northeastone.OS = caCentralone.OS
                    LEFT OUTER JOIN EACH
                    [Minimum_price_Output_Region.Minimum_Price_Output_eucentral1] AS euCentralone
                      ON northeastone.INSTANCE_TYPE = euCentralone.INSTANCE_TYPE
                      AND northeastone.OS = euCentralone.OS
                      LEFT OUTER JOIN EACH
                      [Minimum_price_Output_Region.Minimum_Price_Output_euWest1] AS euWestone
                        ON northeastone.INSTANCE_TYPE = euCentralone.INSTANCE_TYPE
                        AND northeastone.OS = euWestone.OS
                        LEFT OUTER JOIN EACH
                        [Minimum_price_Output_Region.Minimum_Price_Output_saEast1] AS saEastone
                          ON northeastone.INSTANCE_TYPE = saEastone.INSTANCE_TYPE
                          AND northeastone.OS = saEastone.OS
GROUP BY northeastone.INSTANCE_TYPE, northeastone.OS;





