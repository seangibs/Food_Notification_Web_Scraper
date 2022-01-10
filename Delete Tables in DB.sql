/***************************************
SQL to delete all RASFF tables in the DB
***************************************/
--Delete all tables
BEGIN TRY
        
    BEGIN TRANSACTION
        
        --Delete interface country table is exists
        IF OBJECT_ID(N'tbl_RASFF_Notifications_Country', N'U') IS NOT NULL
            BEGIN
                        
                DROP TABLE [tbl_RASFF_Notifications_Country]
                        
            END
                    
        --Delete interface hazard table is exists
        IF OBJECT_ID(N'tbl_RASFF_Notifications_SubstanceHazard', N'U') IS NOT NULL
            BEGIN
                            
                DROP TABLE [tbl_RASFF_Notifications_SubstanceHazard]
                        
            END
        
        --Delete interface notification table is exists
        IF OBJECT_ID(N'tbl_RASFF_Notifications', N'U') IS NOT NULL
            BEGIN
                        
                DROP TABLE [tbl_RASFF_Notifications]
                        
            END
                    
        
        --Delete staging notification table is exists
        IF OBJECT_ID(N'stg_RASFF_Notifications', N'U') IS NOT NULL
            BEGIN
                        
                DROP TABLE [stg_RASFF_Notifications]
                        
            END
                    
        --Delete staging country table is exists
        IF OBJECT_ID(N'stg_RASFF_Notifications_Country', N'U') IS NOT NULL
            BEGIN
                        
                DROP TABLE [stg_RASFF_Notifications_Country]
                        
            END
                    
        --Delete staging hazard table is exists
        IF OBJECT_ID(N'stg_RASFF_Notifications_SubstanceHazard', N'U') IS NOT NULL
            BEGIN
                            
                DROP TABLE [stg_RASFF_Notifications_SubstanceHazard]
                        
            END
        
    COMMIT TRAN
        
END TRY
        
BEGIN CATCH
        
    IF @@TRANCOUNT > 0
        ROLLBACK TRAN
        
END CATCH