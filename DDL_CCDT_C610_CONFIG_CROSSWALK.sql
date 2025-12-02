/*==============================================================================
    SCRIPT NAME   : DDL_CCDT_C610_CONFIG_CROSSWALK.sql
    AUTHOR        : COTIVITI
    CREATED DATE  : 10/28/2025

    MODIFICATION HISTORY
      10/28/2025  COTIVITI  Build 3.00.00.00
                   Initial SQL Server deployment of dbo.CCDT_C610_CONFIG_CROSSWALK.

    BUILD 3.0.0.0
      X → Major version (interface, behavior, or compatibility changes)
      Y → Minor update (additions/enhancements)
      Z → Patch (bug fixes/maintenance)
      W → Client-specific requirement (custom objects/routines)

    PURPOSE
      Deterministic, idempotent creation of dbo.CCDT_C610_CONFIG_CROSSWALK —
      a matrix of crosswalk mappings (source→target) by interface and type.
      Includes supporting indexes, compression, and documentation.

    ASSUMPTIONS
      • Executed in the correct database with DDL permissions.
      • SQL Server 2017 or later (supports compression).
      • Schema dbo exists and is default schema for this deployment.
      • Script may be safely re-run (idempotent).

    OBJECTS CREATED / MODIFIED
      • TABLE   : dbo.CCDT_C610_CONFIG_CROSSWALK
      • INDEX   : IX_XWLK_INTF_TYPE        (INTF_ID, XWLK_TYPE)
      • INDEX   : IX_XWLK_TARGETS          (XWLK_TARGET_1, XWLK_TARGET_2)
      • CONSTRAINTS: PK (INTF_ID, XWLK_TYPE, XWLK_SOURCE_1, XWLK_SOURCE_2, XWLK_SOURCE_3, XWLK_EFF_DT),
                     DF (CREATE_DTM)
      • EXTENDED PROPERTIES for documentation
      • ROW compression applied to all indexes

    PERFORMANCE NOTES
      • Clustered composite PK aligned with natural key + effective date.
      • Supporting indexes to accelerate common lookups by interface/type and target values.
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
        , @c_table_name            SYSNAME        = N'CCDT_C610_CONFIG_CROSSWALK'
        , @c_pk_name               SYSNAME        = N'PK_CCDT_C610_CONFIG_CROSSWALK'
        , @c_ix_intf_type          SYSNAME        = N'IX_XWLK_INTF_TYPE'
        , @c_ix_targets            SYSNAME        = N'IX_XWLK_TARGETS'
        , @c_df_create_dtm         SYSNAME        = N'DF_XWLK_CREATE_DTM'
        , @c_ep_ms_description     SYSNAME        = N'MS_Description'
        , @c_table_desc            NVARCHAR(4000) = N'Crosswalk mapping matrix by interface and type (source → target) with effective dating.';

    DECLARE
          @v_object_id             INT            = OBJECT_ID(QUOTENAME(@c_schema_name) + N'.' + QUOTENAME(@c_table_name));

    ------------------------------------------------------------------
    -- Create table if not exists (with PK and default)
    ------------------------------------------------------------------
    IF @v_object_id IS NULL
    BEGIN
        SET @sql = N'
            CREATE TABLE ' + QUOTENAME(@c_schema_name) + N'.' + QUOTENAME(@c_table_name) + N'
            (
                  INTF_ID        INT             NOT NULL
                , XWLK_TYPE      VARCHAR(20)     NOT NULL
                , XWLK_SOURCE_1  VARCHAR(50)     NOT NULL
                , XWLK_SOURCE_2  VARCHAR(50)     NOT NULL
                , XWLK_SOURCE_3  VARCHAR(50)     NOT NULL
                , XWLK_TARGET_1  VARCHAR(50)     NOT NULL
                , XWLK_TARGET_2  VARCHAR(50)         NULL
                , XWLK_TARGET_3  VARCHAR(50)         NULL
                , XWLK_DESC      VARCHAR(255)        NULL
                , XWLK_EFF_DT    DATETIME        NOT NULL
                , XWLK_TERM_DT   DATETIME            NULL
                , CREATE_DTM     DATETIME        NOT NULL
                    CONSTRAINT ' + QUOTENAME(@c_df_create_dtm) + N' DEFAULT (SYSUTCDATETIME())
                , CREATE_USID    VARCHAR(50)     NOT NULL
                , LAST_UPD_DTM   DATETIME        NOT NULL
                , LAST_UPD_USID  VARCHAR(50)     NOT NULL
                , CONSTRAINT ' + QUOTENAME(@c_pk_name) + N'
                    PRIMARY KEY CLUSTERED (INTF_ID, XWLK_TYPE, XWLK_SOURCE_1, XWLK_SOURCE_2, XWLK_SOURCE_3, XWLK_EFF_DT)
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
    END;

    ------------------------------------------------------------------
    -- Indexes (create if missing)
    ------------------------------------------------------------------
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = @v_object_id AND name = @c_ix_intf_type)
    BEGIN
        SET @sql = N'
            CREATE NONCLUSTERED INDEX ' + QUOTENAME(@c_ix_intf_type) + N'
                ON ' + QUOTENAME(@c_schema_name) + N'.' + QUOTENAME(@c_table_name) + N'
                   (INTF_ID, XWLK_TYPE);
        ';
        EXEC sys.sp_executesql @sql;
    END;

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = @v_object_id AND name = @c_ix_targets)
    BEGIN
        SET @sql = N'
            CREATE NONCLUSTERED INDEX ' + QUOTENAME(@c_ix_targets) + N'
                ON ' + QUOTENAME(@c_schema_name) + N'.' + QUOTENAME(@c_table_name) + N'
                   (XWLK_TARGET_1, XWLK_TARGET_2);
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
        FORMATMESSAGE(N'[CCDT_C610_CONFIG_CROSSWALK] failed. %s (PROC=%s, LINE=%d, ERR=%d)',
                      @v_err_msg, @v_err_msg_proc, @v_err_msg_line, @v_err_msg_num);

    THROW 51000, @v_fmt_msg, 1;
END CATCH;
GO

/*------------------------------------------------------------------------------
Operator notes:

-- Typical crosswalk lookup by interface and type (effective-dated)
SELECT TOP (100) *
FROM dbo.CCDT_C610_CONFIG_CROSSWALK
WHERE INTF_ID = @IntfId
  AND XWLK_TYPE = @Type
  AND (@AsOf IS NULL OR (XWLK_EFF_DT <= @AsOf AND (XWLK_TERM_DT IS NULL OR @AsOf < XWLK_TERM_DT)))
ORDER BY XWLK_SOURCE_1, XWLK_SOURCE_2, XWLK_SOURCE_3, XWLK_EFF_DT DESC;

------------------------------------------------------------------------------*/


