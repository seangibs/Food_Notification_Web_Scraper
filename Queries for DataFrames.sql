/*********************************
Queries used to populate dataframe
*********************************/

--Main notification DF

SELECT --TOP 1000
    --N.Reference,
    ISNULL(C.Country,LEFT(NotificationFrom,CHARINDEX('(', NotificationFrom)-2)) AS Country
    , CASE WHEN C.Type = 0 THEN C.Country ELSE NULL END AS Origin 
    , CASE WHEN C.Type = 1 THEN C.Country ELSE NULL END AS [Distribution] 
    , LEFT(NotificationFrom,CHARINDEX('(', NotificationFrom)-2) AS Reported_By
    , YEAR(N.NotificationDate) AS [Year]
    , N.NotificationCategory
    , H.Category AS HazardCategory
    , COUNT(DISTINCT N.Reference) AS NumInstances
    , N.ProductCategory 
FROM tbl_RASFF_Notifications N
    LEFT JOIN tbl_RASFF_Notifications_Country C ON C.Reference = N.Reference
    LEFT JOIN tbl_RASFF_Notifications_SubstanceHazard H ON H.Reference = N.Reference
GROUP BY 
    ISNULL(C.Country,LEFT(NotificationFrom,CHARINDEX('(', NotificationFrom)-2))
    , CASE WHEN C.Type = 0 THEN C.Country ELSE NULL END
    , CASE WHEN C.Type = 1 THEN C.Country ELSE NULL END
    , LEFT(N.NotificationFrom,CHARINDEX('(', NotificationFrom)-2)
    , YEAR(N.NotificationDate)
    , N.NotificationCategory
    , H.Category 
    , N.Reference
    , N.ProductCategory 


--Category Drop Down Options for Dashboard

SELECT 'All' AS NotificationCategory, 0 AS Ord
UNION
SELECT DISTINCT NotificationCategory, 1 AS Ord
FROM tbl_RASFF_Notifications
ORDER BY 2,1

--Hazard Drop Down Options

SELECT 'All' AS HazardCategory, 0 AS ORD
UNION
SELECT DISTINCT H.Category AS HazardCategory, 1 AS ORD
FROM tbl_RASFF_Notifications_SubstanceHazard H
ORDER BY 2,1

/************Merged Data************/

--RASFF Notifications & GDP
SELECT G.Country_Name AS Country
    ,G.Value 
    ,CAST(G.[Year] AS INT) AS [Year]
    ,COUNT(DISTINCT N.Reference) AS NumInstances
FROM tbl_RASFF_Notifications N
    INNER JOIN GDP G ON G.Country_Name = LEFT(N.NotificationFrom,CHARINDEX('(', N.NotificationFrom)-2)
        AND YEAR(N.NotificationDate) = G.[Year]
GROUP BY G.Country_Name
    ,G.Value 
    ,G.[Year]


--Smoking & GDP
SELECT T.[Year] 
    ,G.Country_Name AS Country
    ,G.Value 
    ,AVG(T.Data_Value) AS Perc_Smoke
FROM Cleansed_Global_Tobacco_Data T
    INNER JOIN GDP G ON G.Country_Name = T.Country
        AND G.[Year] = T.[Year]
WHERE T.Indicator = 'Percentage of youth who currently smoke cigarettes'
    GROUP BY T.[Year] 
    ,G.Country_Name
    ,G.Value 
        ORDER BY 1,2

--RASFF, Smoking & GDP (Bubble Chart + Multiple Regression)
SELECT G.Country_Name AS Country
    ,G.Value 
    ,COUNT(DISTINCT N.Reference) AS NumInstances
    ,AVG(T.Data_Value) AS Perc_Smoke
FROM tbl_RASFF_Notifications N
    INNER JOIN GDP G ON G.Country_Name = LEFT(N.NotificationFrom,CHARINDEX('(', N.NotificationFrom)-2)
        AND YEAR(N.NotificationDate) = G.[Year]
    INNER JOIN Cleansed_Global_Tobacco_Data T ON G.Country_Name = T.Country
        AND G.[Year] = T.[Year]
GROUP BY G.Country_Name
    ,G.Value 

