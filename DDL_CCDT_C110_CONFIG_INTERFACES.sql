/*==============================================================================
    SCRIPT NAME   : DDL_CCDT_C110_CONFIG_INTERFACES.sql
    AUTHOR        : COTIVITI
    CREATED DATE  : 10/28/2025

    MODIFICATION HISTORY
      10/28/2025  COTIVITI  Build 3.00.00.00
                   Initial SQL Server deployment of dbo.CCDT_C110_CONFIG_INTERFACES.

    BUILD 3.0.0.0
      X → Major version (interface, behavior, or compatibility changes)
      Y → Minor update (additions/enhancements)
      Z → Patch (bug fixes/maintenance)
      W → Client-specific requirement (custom objects/routines)

    PURPOSE
      Deterministic, idempotent creation of dbo.CCDT_C110_CONFIG_INTERFACES —
      configuration of interface IDs/names for PPM, CV, CCV and RA files. Includes
      constraints, a unique index on INTF_NAME, compression, and documentation.

    ASSUMPTIONS
      • Executed in the correct database with DDL permissions.
      • SQL Server 2017 or later (supports compression).
      • Schema dbo exists and is default schema for this deployment.
      • Script may be safely re-run (idempotent).

    OBJECTS CREATED / MODIFIED
      • TABLE   : dbo.CCDT_C110_CONFIG_INTERFACES
      • INDEX   : UQ_C210_CONFIG_INTERFACES_NAME (UNIQUE, INTF_NAME)
      • CONSTRAINTS: PK (INTF_ID), DF (CREATE_DTM), CK (INTF_PRODUCT), CK (INTF_TYPE)
      • EXTENDED PROPERTIES for documentation
      • ROW compression applied to all indexes

    PERFORMANCE NOTES
      • Clustered PK on INTF_ID (INT).
      • Unique index on INTF_NAME for fast lookups and to prevent duplicates.
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
        , @c_table_name            SYSNAME        = N'CCDT_C110_CONFIG_INTERFACES'
        , @c_pk_name               SYSNAME        = N'PK_CCDT_C110_CONFIG_INTERFACES'
        , @c_uq_name               SYSNAME        = N'UQ_C210_CONFIG_INTERFACES_NAME'
        , @c_df_create_dtm         SYSNAME        = N'DF_CCDT_C110_CONFIG_INTERFACES_CREATE_DTM'
        , @c_ck_intf_product       SYSNAME        = N'CK_CCDT_C110_CONFIG_INTERFACES_PRODUCT'
        , @c_ck_intf_type          SYSNAME        = N'CK_CCDT_C110_CONFIG_INTERFACES_TYPE'
        , @c_ep_ms_description     SYSNAME        = N'MS_Description'
        , @c_table_desc            NVARCHAR(4000) = N'Config for interface IDs/names and metadata for PPM, CV, CCV, RA files.';

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
                  INTF_ID              INT             NOT NULL
                , INTF_NAME            VARCHAR(20)     NOT NULL
                , INTF_PRODUCT         VARCHAR(3)      NOT NULL   -- ''PPM'',''CV'',''CCV'',''RA''
                , INTF_TYPE            VARCHAR(6)      NOT NULL   -- ''PROF'',''INST'',''MEM'',''PROV''
                , INTF_DESC            VARCHAR(255)        NULL
                , INTF_FILE_LAYOUT     VARCHAR(255)        NULL
                , INTF_LAST_RUN_DTM    DATETIME            NULL
                , INTF_LAST_SEQ_NO     INT                 NULL    -- Running sequence number incremented for each run
                , INTF_DAILY_SEQ_NO    INT                 NULL    -- Running sequence reset daily
                , CREATE_DTM           DATETIME        NOT NULL
                    CONSTRAINT ' + QUOTENAME(@c_df_create_dtm) + N' DEFAULT (SYSUTCDATETIME())
                , CREATE_USID          VARCHAR(50)     NOT NULL
                , LAST_UPD_DTM         DATETIME        NOT NULL
                , LAST_UPD_USID        VARCHAR(50)     NOT NULL
                , CONSTRAINT ' + QUOTENAME(@c_pk_name) + N'
                    PRIMARY KEY CLUSTERED (INTF_ID)
                , CONSTRAINT ' + QUOTENAME(@c_ck_intf_product) + N'
                    CHECK (INTF_PRODUCT IN (''PPM'',''CV'',''COB'',''CCV'',''CPR'',''RA''))
                , CONSTRAINT ' + QUOTENAME(@c_ck_intf_type) + N'
                    CHECK (INTF_TYPE IN (''GLOBAL'',''PROF'',''INST'',''BOTH'',''MEM'',''PROV''))
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

        -- Ensure check constraints on INTF_PRODUCT and INTF_TYPE
        IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE parent_object_id=@v_object_id AND name=@c_ck_intf_product)
        BEGIN
            SET @sql = N'
                ALTER TABLE ' + QUOTENAME(@c_schema_name) + N'.' + QUOTENAME(@c_table_name) + N'
                    WITH NOCHECK
                    ADD CONSTRAINT ' + QUOTENAME(@c_ck_intf_product) + N'
                        CHECK (INTF_PRODUCT IN (''PPM'',''CV'',''CCV'',''RA''));
                ALTER TABLE ' + QUOTENAME(@c_schema_name) + N'.' + QUOTENAME(@c_table_name) + N'
                    CHECK CONSTRAINT ' + QUOTENAME(@c_ck_intf_product) + N';
            ';
        EXEC sys.sp_executesql @sql;
        END;

        IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE parent_object_id=@v_object_id AND name=@c_ck_intf_type)
        BEGIN
            SET @sql = N'
                ALTER TABLE ' + QUOTENAME(@c_schema_name) + N'.' + QUOTENAME(@c_table_name) + N'
                    WITH NOCHECK
                    ADD CONSTRAINT ' + QUOTENAME(@c_ck_intf_type) + N'
                        CHECK (INTF_TYPE IN (''PROF'',''INST'',''MEM'',''PROV''));
                ALTER TABLE ' + QUOTENAME(@c_schema_name) + N'.' + QUOTENAME(@c_table_name) + N'
                    CHECK CONSTRAINT ' + QUOTENAME(@c_ck_intf_type) + N';
            ';
        EXEC sys.sp_executesql @sql;
        END;
    END;

    ------------------------------------------------------------------
    -- Unique index on INTF_NAME (prevents duplicates, speeds lookups)
    ------------------------------------------------------------------
    IF NOT EXISTS (
        SELECT 1
        FROM sys.indexes
        WHERE object_id = @v_object_id
          AND name = @c_uq_name
    )
    BEGIN
        SET @sql = N'
            CREATE UNIQUE NONCLUSTERED INDEX ' + QUOTENAME(@c_uq_name) + N'
                ON ' + QUOTENAME(@c_schema_name) + N'.' + QUOTENAME(@c_table_name) + N' (INTF_NAME);
        ';
        EXEC sys.sp_executesql @sql;
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

    -- Column descriptions
    DECLARE
          @c_col_desc_product NVARCHAR(4000) = N'Product code: PPM, CV, CCV, RA.'
        , @c_col_desc_type    NVARCHAR(4000) = N'Interface type: PROF, INST, MEM, PROV.'
        , @c_col_desc_cdtm    NVARCHAR(4000) = N'UTC create timestamp; defaulted via SYSUTCDATETIME().';

    -- INTF_PRODUCT
    IF EXISTS (SELECT 1 FROM sys.extended_properties
               WHERE class=1 AND name=@c_ep_ms_description AND major_id=@v_object_id
                 AND minor_id = COLUMNPROPERTY(@v_object_id, N'INTF_PRODUCT', 'ColumnId'))
        EXEC sys.sp_updateextendedproperty
             @name=@c_ep_ms_description,@value=@c_col_desc_product,
             @level0type=N'SCHEMA',@level0name=@c_schema_name,
             @level1type=N'TABLE', @level1name=@c_table_name,
             @level2type=N'COLUMN',@level2name=N'INTF_PRODUCT';
    ELSE
        EXEC sys.sp_addextendedproperty
             @name=@c_ep_ms_description,@value=@c_col_desc_product,
             @level0type=N'SCHEMA',@level0name=@c_schema_name,
             @level1type=N'TABLE', @level1name=@c_table_name,
             @level2type=N'COLUMN',@level2name=N'INTF_PRODUCT';

    -- INTF_TYPE
    IF EXISTS (SELECT 1 FROM sys.extended_properties
               WHERE class=1 AND name=@c_ep_ms_description AND major_id=@v_object_id
                 AND minor_id = COLUMNPROPERTY(@v_object_id, N'INTF_TYPE', 'ColumnId'))
        EXEC sys.sp_updateextendedproperty
             @name=@c_ep_ms_description,@value=@c_col_desc_type,
             @level0type=N'SCHEMA',@level0name=@c_schema_name,
             @level1type=N'TABLE', @level1name=@c_table_name,
             @level2type=N'COLUMN',@level2name=N'INTF_TYPE';
    ELSE
        EXEC sys.sp_addextendedproperty
             @name=@c_ep_ms_description,@value=@c_col_desc_type,
             @level0type=N'SCHEMA',@level0name=@c_schema_name,
             @level1type=N'TABLE', @level1name=@c_table_name,
             @level2type=N'COLUMN',@level2name=N'INTF_TYPE';

    -- CREATE_DTM
    IF EXISTS (SELECT 1 FROM sys.extended_properties
               WHERE class=1 AND name=@c_ep_ms_description AND major_id=@v_object_id
                 AND minor_id = COLUMNPROPERTY(@v_object_id, N'CREATE_DTM', 'ColumnId'))
        EXEC sys.sp_updateextendedproperty
             @name=@c_ep_ms_description,@value=@c_col_desc_cdtm,
             @level0type=N'SCHEMA',@level0name=@c_schema_name,
             @level1type=N'TABLE', @level1name=@c_table_name,
             @level2type=N'COLUMN',@level2name=N'CREATE_DTM';
    ELSE
        EXEC sys.sp_addextendedproperty
             @name=@c_ep_ms_description,@value=@c_col_desc_cdtm,
             @level0type=N'SCHEMA',@level0name=@c_schema_name,
             @level1type=N'TABLE', @level1name=@c_table_name,
             @level2type=N'COLUMN',@level2name=N'CREATE_DTM';

    COMMIT TRAN;
END TRY
BEGIN CATCH
    IF XACT_STATE() <> 0 ROLLBACK;

    DECLARE
          @v_err_msg       NVARCHAR(4000) = ERROR_MESSAGE()
        , @v_err_msg_proc  NVARCHAR(128)  = ISNULL(ERROR_PROCEDURE(), N'')
        , @v_err_msg_line  INT            = ERROR_LINE()
        , @v_err_msg_num   INT            = ERROR_NUMBER();

    DECLARE @v_fmt_msg NVARCHAR(2048) =
        FORMATMESSAGE(N'[CCDT_C110_CONFIG_INTERFACES] failed. %s (PROC=%s, LINE=%d, ERR=%d)',
                      @v_err_msg, @v_err_msg_proc, @v_err_msg_line, @v_err_msg_num);

    THROW 51000, @v_fmt_msg, 1;
END CATCH;
GO

/*------------------------------------------------------------------------------
Operator notes:

-- Example: upsert an interface row
MERGE dbo.CCDT_C110_CONFIG_INTERFACES AS T
USING (VALUES (101, 'PPM_PROF_INTF', 'PPM', 'PROF')) AS S(INTF_ID, INTF_NAME, INTF_PRODUCT, INTF_TYPE)
ON T.INTF_ID = S.INTF_ID
WHEN MATCHED THEN
    UPDATE SET LAST_UPD_DTM = SYSUTCDATETIME(), LAST_UPD_USID = SUSER_SNAME()
WHEN NOT MATCHED THEN
    INSERT (INTF_ID, INTF_NAME, INTF_PRODUCT, INTF_TYPE, CREATE_DTM, CREATE_USID, LAST_UPD_DTM, LAST_UPD_USID)
    VALUES (S.INTF_ID, S.INTF_NAME, S.INTF_PRODUCT, S.INTF_TYPE, SYSUTCDATETIME(), SUSER_SNAME(), SYSUTCDATETIME(), SUSER_SNAME());

------------------------------------------------------------------------------*/






