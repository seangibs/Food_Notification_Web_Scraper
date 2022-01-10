/****************************
Merge Staging with Interfact
***************************/

--Merge main notification table
MERGE
        tbl_RASFF_Notifications AS T
        USING (
            SELECT DISTINCT
                 N.Reference
                ,N.Subject
                ,CONVERT(DATE, N.NotificationDate, 103) AS NotificationDate
                ,CONVERT(DATE, N.LastUpdate, 103) AS LastUpdate
                ,N.NotificationType
                ,N.NotificationCategory
                ,N.NotificationSource
                ,N.NotificationStatus
                ,N.ActionTaken
                ,N.NotificationFrom
                ,N.DistributionStatus
                ,N.Product
                ,N.ProductCategory
                ,N.RiskDecision
            FROM stg_RASFF_Notifications N) AS S
        ON T.Reference = S.Reference
        WHEN MATCHED THEN
            UPDATE SET
                T.Reference = S.Reference
                ,T.Subject = S.Subject
                ,T.NotificationDate = S.NotificationDate
                ,T.LastUpdate = S.LastUpdate
                ,T.NotificationType = S.NotificationType
                ,T.NotificationCategory = S.NotificationCategory
                ,T.NotificationSource = S.NotificationSource
                ,T.NotificationStatus = S.NotificationStatus
                ,T.ActionTaken = S.ActionTaken
                ,T.NotificationFrom = S.NotificationFrom
                ,T.DistributionStatus = S.DistributionStatus
                ,T.Product = S.Product
                ,T.ProductCategory = S.ProductCategory
                ,T.RiskDecision = S.RiskDecision
        WHEN NOT MATCHED THEN
            INSERT (
                 Reference 
                ,Subject 
                ,NotificationDate 
                ,LastUpdate
                ,NotificationType 
                ,NotificationCategory
                ,NotificationSource 
                ,NotificationStatus 
                ,ActionTaken 
                ,NotificationFrom 
                ,DistributionStatus 
                ,Product
                ,ProductCategory 
                ,RiskDecision)
            VALUES (
                 S.Reference
                ,S.Subject
                ,S.NotificationDate
                ,S.LastUpdate
                ,S.NotificationType
                ,S.NotificationCategory
                ,S.NotificationSource
                ,S.NotificationStatus
                ,S.ActionTaken
                ,S.NotificationFrom
                ,S.DistributionStatus
                ,S.Product
                ,S.ProductCategory
                ,S.RiskDecision);


--Insert into Country
DELETE B
FROM stg_RASFF_Notifications_Country A
    LEFT JOIN tbl_RASFF_Notifications_Country B ON A.Reference = B.Reference
WHERE B.Reference IS NOT NULL
        
INSERT INTO tbl_RASFF_Notifications_Country (
    Id
    ,Reference
    ,Country
    ,Type)
SELECT 
    CHECKSUM(NEWID())
    ,C.Reference
    ,C.Country
    ,C.Type
FROM stg_RASFF_Notifications_Country C
	INNER JOIN tbl_RASFF_Notifications N ON N.Reference = C.Reference

--Insert Hazards
DELETE B
FROM stg_RASFF_Notifications_SubstanceHazard A
    LEFT JOIN tbl_RASFF_Notifications_SubstanceHazard B ON A.Reference = B.Reference
WHERE B.Reference IS NOT NULL
        
INSERT INTO tbl_RASFF_Notifications_SubstanceHazard (
    Id
    ,Reference
    ,SubstanceHazard
    ,Category
    ,AnalyticalResult
    ,Units
    ,SamplingDate)
SELECT 
    CHECKSUM(NEWID())
    ,H.Reference
    ,H.SubstanceHazard
    ,H.Category
    ,H.AnalyticalResult
    ,H.Units
    ,CONVERT(DATE, H.SamplingDate, 105)
FROM stg_RASFF_Notifications_SubstanceHazard H
	INNER JOIN tbl_RASFF_Notifications N ON N.Reference = H.Reference