/*==============================================================================
    SCRIPT NAME   : DDL_CCDT_C520_CONFIG_CATALOG.sql
    AUTHOR        : COTIVITI
    CREATED DATE  : 10/28/2025

    MODIFICATION HISTORY
      10/28/2025  COTIVITI  Build 3.00.00.00
                   Initial SQL Server deployment of dbo.CCDT_C520_CONFIG_CATALOG.

    BUILD 3.0.0.0
      X → Major version (interface, behavior, or compatibility changes)
      Y → Minor update (additions/enhancements)
      Z → Patch (bug fixes/maintenance)
      W → Client-specific requirement (custom objects/routines)

    PURPOSE
      Deterministic, idempotent creation of dbo.CCDT_C520_CONFIG_CATALOG —
      catalog of bypass categories (logical groupings of bypass rules). Includes
      constraints, compression, and documentation.

    ASSUMPTIONS
      • Executed in the correct database with DDL permissions.
      • SQL Server 2017 or later (supports compression).
      • Schema dbo exists and is default schema for this deployment.
      • Script may be safely re-run (idempotent).

    OBJECTS CREATED / MODIFIED
      • TABLE   : dbo.CCDT_C520_CONFIG_CATALOG
      • CONSTRAINTS: PK (BYPASS_CATG_ID), DF (CREATE_DTM), CK (BYPASS_TYPE)
      • EXTENDED PROPERTIES for documentation
      • ROW compression applied to all indexes

    PERFORMANCE NOTES
      • Clustered PK on BYPASS_CATG_ID (VARCHAR(10)).
      • Row-level compression enabled.

    SECURITY / PII
      • Contains no PHI/PII.
      • CREATE_DTM stamped via SYSUTCDATETIME(); user IDs set by calling processes.

==============================================================================*/

USE YourDatabaseName;
GO

SET NOCOUNT ON;
SET XACT_ABORT ON;
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

BEGIN TRY
    BEGIN TRAN;


DECLARE @sql NVARCHAR(MAX);
    ------------------------------------------------------------------
    -- Constants (names and IDs) — @c_ variables are not modified
    ------------------------------------------------------------------
    DECLARE
          @c_schema_name           SYSNAME        = N'dbo'
        , @c_table_name            SYSNAME        = N'CCDT_C520_CONFIG_CATALOG'
        , @c_pk_name               SYSNAME        = N'PK_CCDT_C520_CONFIG_CATALOG'
        , @c_df_create_dtm         SYSNAME        = N'DF_C520_CONFIG_CATALOG_CREATE_DTM'
        , @c_ck_bypass_type        SYSNAME        = N'CK_C520_CONFIG_CATALOG_BYPASS_TYPE'
        , @c_ep_ms_description     SYSNAME        = N'MS_Description'
        , @c_table_desc            NVARCHAR(4000) = N'Bypass category catalog: logical groupings of bypass rules for outbound processing.';

    DECLARE
          @v_object_id             INT            = OBJECT_ID(QUOTENAME(@c_schema_name) + N'.' + QUOTENAME(@c_table_name));

    ------------------------------------------------------------------
    -- Create table if not exists (with PK, DF, CK as promised)
    ------------------------------------------------------------------
    IF @v_object_id IS NULL
    BEGIN
        SET @sql = N'
            CREATE TABLE ' + QUOTENAME(@c_schema_name) + N'.' + QUOTENAME(@c_table_name) + N'
            (
                  BYPASS_CATG_ID   VARCHAR(10)                 NOT NULL   -- e.g., C10, C20, C30
                , BYPASS_CATG_DESC VARCHAR(200)                NOT NULL   -- technical bypass description
                , CLIENT_DESC      VARCHAR(200)                NOT NULL   -- client-facing description
                , BYPASS_TYPE      CHAR(1)                     NOT NULL   -- ''R'' = category of rules
                , BYPASS_CODE      NUMERIC(30,0)               NOT NULL   -- e.g., 4, 8, 16, ...
                , CREATE_DTM       DATETIME                    NOT NULL
                    CONSTRAINT ' + QUOTENAME(@c_df_create_dtm) + N' DEFAULT (SYSUTCDATETIME())
                , CREATE_USID      VARCHAR(50)                 NOT NULL
                , LAST_UPD_DTM     DATETIME                        NULL
                , LAST_UPD_USID    VARCHAR(50)                     NULL
                , CONSTRAINT ' + QUOTENAME(@c_pk_name) + N'
                    PRIMARY KEY CLUSTERED (BYPASS_CATG_ID)
                , CONSTRAINT ' + QUOTENAME(@c_ck_bypass_type) + N'
                    CHECK (BYPASS_TYPE IN (''R'',''B''))
            );
        ';
        EXEC sys.sp_executesql @sql;

        SET @v_object_id = OBJECT_ID(QUOTENAME(@c_schema_name) + N'.' + QUOTENAME(@c_table_name));
    END
    ELSE
    BEGIN
        -- Ensure required default on CREATE_DTM
        IF NOT EXISTS (
            SELECT 1
            FROM sys.default_constraints dc
            JOIN sys.columns c
              ON c.object_id = dc.parent_object_id
             AND c.column_id = dc.parent_column_id
            WHERE dc.parent_object_id = @v_object_id
              AND dc.name = @c_df_create_dtm
              AND c.name = N'CREATE_DTM'
        )
        BEGIN
            SET @sql = N'
                ALTER TABLE ' + QUOTENAME(@c_schema_name) + N'.' + QUOTENAME(@c_table_name) + N'
                    ADD CONSTRAINT ' + QUOTENAME(@c_df_create_dtm) + N'
                        DEFAULT (SYSUTCDATETIME()) FOR CREATE_DTM;
            ';
        EXEC sys.sp_executesql @sql;
        END;

        -- Ensure BYPASS_TYPE check constraint
        IF NOT EXISTS (
            SELECT 1
            FROM sys.check_constraints
            WHERE parent_object_id = @v_object_id
              AND name = @c_ck_bypass_type
        )
        BEGIN
            SET @sql = N'
                ALTER TABLE ' + QUOTENAME(@c_schema_name) + N'.' + QUOTENAME(@c_table_name) + N'
                    WITH NOCHECK
                    ADD CONSTRAINT ' + QUOTENAME(@c_ck_bypass_type) + N'
                        CHECK (BYPASS_TYPE IN (''R''));
                ALTER TABLE ' + QUOTENAME(@c_schema_name) + N'.' + QUOTENAME(@c_table_name) + N'
                    CHECK CONSTRAINT ' + QUOTENAME(@c_ck_bypass_type) + N';
            ';
        EXEC sys.sp_executesql @sql;
        END;
    END;

    ------------------------------------------------------------------
    -- Apply row compression to all indexes on the table
    ------------------------------------------------------------------
    SET @sql = N'ALTER INDEX ALL ON ' + QUOTENAME(@c_schema_name) + N'.' + QUOTENAME(@c_table_name) +
         N' REBUILD WITH (DATA_COMPRESSION = ROW);';
        EXEC sys.sp_executesql @sql;

    ------------------------------------------------------------------
    -- Extended properties (add-or-update pattern)
    ------------------------------------------------------------------
    -- Table description
    IF EXISTS (
        SELECT 1
        FROM sys.extended_properties
        WHERE class = 1
          AND name = @c_ep_ms_description
          AND major_id = @v_object_id
          AND minor_id = 0
    )
    BEGIN
        EXEC sys.sp_updateextendedproperty
             @name = @c_ep_ms_description,
             @value = @c_table_desc,
             @level0type = N'SCHEMA', @level0name = @c_schema_name,
             @level1type = N'TABLE',  @level1name = @c_table_name;
    END
    ELSE
    BEGIN
        EXEC sys.sp_addextendedproperty
             @name = @c_ep_ms_description,
             @value = @c_table_desc,
             @level0type = N'SCHEMA', @level0name = @c_schema_name,
             @level1type = N'TABLE',  @level1name = @c_table_name;
    END;

    -- Column: CREATE_DTM description
    DECLARE @c_col_desc_create_dtm NVARCHAR(4000) = N'UTC create timestamp; defaulted via SYSUTCDATETIME().';
    IF EXISTS (
        SELECT 1
        FROM sys.extended_properties
        WHERE class = 1
          AND name = @c_ep_ms_description
          AND major_id = @v_object_id
          AND minor_id = COLUMNPROPERTY(@v_object_id, N'CREATE_DTM', 'ColumnId')
    )
    BEGIN
        EXEC sys.sp_updateextendedproperty
             @name = @c_ep_ms_description,
             @value = @c_col_desc_create_dtm,
             @level0type=N'SCHEMA', @level0name=@c_schema_name,
             @level1type=N'TABLE',  @level1name=@c_table_name,
             @level2type=N'COLUMN', @level2name=N'CREATE_DTM';
    END
    ELSE
    BEGIN
        EXEC sys.sp_addextendedproperty
             @name = @c_ep_ms_description,
             @value = @c_col_desc_create_dtm,
             @level0type=N'SCHEMA', @level0name=@c_schema_name,
             @level1type=N'TABLE',  @level1name=@c_table_name,
             @level2type=N'COLUMN', @level2name=N'CREATE_DTM';
    END;

    -- Column: BYPASS_TYPE description
    DECLARE @c_col_desc_bypass_type NVARCHAR(4000) =
        N'Bypass category type: R = Category of rules.';
    IF EXISTS (
        SELECT 1
        FROM sys.extended_properties
        WHERE class = 1
          AND name = @c_ep_ms_description
          AND major_id = @v_object_id
          AND minor_id = COLUMNPROPERTY(@v_object_id, N'BYPASS_TYPE', 'ColumnId')
    )
    BEGIN
        EXEC sys.sp_updateextendedproperty
             @name = @c_ep_ms_description,
             @value = @c_col_desc_bypass_type,
             @level0type=N'SCHEMA', @level0name=@c_schema_name,
             @level1type=N'TABLE',  @level1name=@c_table_name,
             @level2type=N'COLUMN', @level2name=N'BYPASS_TYPE';
    END
    ELSE
    BEGIN
        EXEC sys.sp_addextendedproperty
             @name = @c_ep_ms_description,
             @value = @c_col_desc_bypass_type,
             @level0type=N'SCHEMA', @level0name=@c_schema_name,
             @level1type=N'TABLE',  @level1name=@c_table_name,
             @level2type=N'COLUMN', @level2name=N'BYPASS_TYPE';
    END;

    COMMIT TRAN;
END TRY
BEGIN CATCH
    IF XACT_STATE() <> 0 ROLLBACK TRAN;

    DECLARE
          @v_err_msg       NVARCHAR(4000) = ERROR_MESSAGE()
        , @v_err_msg_proc  NVARCHAR(128)  = ISNULL(ERROR_PROCEDURE(), N''), @v_err_msg_line  INT            = ERROR_LINE()
        , @v_err_msg_num   INT            = ERROR_NUMBER();

    DECLARE @msg NVARCHAR(2048) = FORMATMESSAGE(N'[CCDT_C520_CONFIG_CATALOG] DDL failed. %s (PROC=%s, LINE=%d, ERR=%d)', @v_err_msg, ISNULL(@v_err_msg_proc, N''), @v_err_msg_proc, @v_err_msg_num);
    THROW 51000, @msg, 1;
END CATCH;
GO

/*------------------------------------------------------------------------------
Operator note:

-- Typical lookup
SELECT BYPASS_CATG_ID, BYPASS_CATG_DESC, CLIENT_DESC, BYPASS_CODE
FROM dbo.CCDT_C520_CONFIG_CATALOG
ORDER BY BYPASS_CATG_ID;

------------------------------------------------------------------------------*/

