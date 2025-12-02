/*==============================================================================
    SCRIPT NAME   : DDL_CCDT_C510_CONFIG_BYPASS.sql
    AUTHOR        : COTIVITI
    CREATED DATE  : 10/28/2025

    MODIFICATION HISTORY
      10/28/2025  COTIVITI  Build 3.00.00.00
                   Initial SQL Server deployment of dbo.CCDT_C510_CONFIG_BYPASS.

    BUILD 3.0.0.0
      X → Major version (interface, behavior, or compatibility changes)
      Y → Minor update (additions/enhancements)
      Z → Patch (bug fixes/maintenance)
      W → Client-specific requirement (custom objects/routines)

    PURPOSE
      Deterministic, idempotent creation of dbo.CCDT_C510_CONFIG_BYPASS —
      configuration of bypass rules used by outbound selection logic
      (daily/history; PROF/INST). Includes supporting indexes, defaults,
      compression, and documentation.

    ASSUMPTIONS
      • Executed in the correct database with DDL permissions.
      • SQL Server 2017 or later (supports compression and filtered indexes).
      • Schema dbo exists and is default schema for this deployment.
      • Script may be safely re-run (idempotent).

    OBJECTS CREATED / MODIFIED
      • TABLE   : dbo.CCDT_C510_CONFIG_BYPASS
      • INDEX   : UQ_INTF_BYPASS_RULE (UNIQUE on INTF_ID, INTF_TYPE, CAT_DESC, SEQ)
      • INDEX   : IX_INTF_BYPASS_INTF_TYPE (INTF_ID, INTF_TYPE)
      • CONSTRAINTS: PK, DF (CREATE_DTM)
      • EXTENDED PROPERTIES for documentation
      • ROW compression applied to all indexes

    PERFORMANCE NOTES
      • Clustered PK on PK_ID (NUMERIC(38,0) IDENTITY).
      • Unique composite index prevents duplicate rules and speeds rule fetches.
      • Secondary index accelerates lookups by interface and type.
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
          @c_schema_name            SYSNAME        = N'dbo'
        , @c_table_name             SYSNAME        = N'CCDT_C510_CONFIG_BYPASS'
        , @c_pk_name                SYSNAME        = N'PK_CCDT_C510_CONFIG_BYPASS'
        , @c_uq_rule_name           SYSNAME        = N'UQ_INTF_BYPASS_RULE'
        , @c_ix_intf_type           SYSNAME        = N'IX_INTF_BYPASS_INTF_TYPE'
        , @c_df_create_dtm          SYSNAME        = N'DF_INTF_CONFIG_BYPASS_CREATE_DTM'
        , @c_ep_ms_description      SYSNAME        = N'MS_Description'
        , @c_table_desc             NVARCHAR(4000) = N'Configuration of bypass rules for outbound file selection (daily/history; PROF/INST).';

    DECLARE
          @v_object_id              INT            = OBJECT_ID(QUOTENAME(@c_schema_name) + N'.' + QUOTENAME(@c_table_name));

    ------------------------------------------------------------------
    -- Create table if not exists (with PK and defaults)
    ------------------------------------------------------------------
    IF @v_object_id IS NULL
    BEGIN
        SET @sql = N'
            CREATE TABLE ' + QUOTENAME(@c_schema_name) + N'.' + QUOTENAME(@c_table_name) + N'
            (
                  PK_ID           NUMERIC(38,0)  IDENTITY(1,1) NOT NULL
                , INTF_ID         INT                           NOT NULL
                , INTF_TYPE       VARCHAR(4)                    NOT NULL
                , CAT_DESC        VARCHAR(50)                   NOT NULL
                , SEQ             INT                           NOT NULL
                , AND_OR          CHAR(1)                       NOT NULL
                , DB_TABLE        VARCHAR(35)                   NOT NULL
                , DB_FIELD        VARCHAR(30)                   NOT NULL
                , COMPARISON      VARCHAR(2)                    NOT NULL
                , ITEM_VALUE1     VARCHAR(50)                   NOT NULL
                , ITEM_VALUE2     VARCHAR(50)                       NULL
                , BYPASS_CODE     INT                           NOT NULL
                , CREATE_DTM      DATETIME                      NOT NULL
                    CONSTRAINT ' + QUOTENAME(@c_df_create_dtm) + N' DEFAULT (SYSUTCDATETIME())
                , CREATE_USID     VARCHAR(50)                   NOT NULL
                , LAST_UPD_DTM    DATETIME                      NOT NULL
                , LAST_UPD_USID   VARCHAR(50)                   NOT NULL
                , CONSTRAINT ' + QUOTENAME(@c_pk_name) + N'
                    PRIMARY KEY CLUSTERED (PK_ID)
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
    -- Indexes
    ------------------------------------------------------------------
    -- Unique rule key: one sequence per (INTF_ID, INTF_TYPE, CAT_DESC)
    IF NOT EXISTS (
        SELECT 1
        FROM sys.indexes
        WHERE object_id = @v_object_id
          AND name = @c_uq_rule_name
    )
    BEGIN
        SET @sql = N'
            CREATE UNIQUE NONCLUSTERED INDEX ' + QUOTENAME(@c_uq_rule_name) + N'
                ON ' + QUOTENAME(@c_schema_name) + N'.' + QUOTENAME(@c_table_name) + N'
                   (INTF_ID, INTF_TYPE, CAT_DESC, SEQ);
        ';
        EXEC sys.sp_executesql @sql;
    END;

    -- Common lookup by interface and type
    IF NOT EXISTS (
        SELECT 1
        FROM sys.indexes
        WHERE object_id = @v_object_id
          AND name = @c_ix_intf_type
    )
    BEGIN
        SET @sql = N'
            CREATE NONCLUSTERED INDEX ' + QUOTENAME(@c_ix_intf_type) + N'
                ON ' + QUOTENAME(@c_schema_name) + N'.' + QUOTENAME(@c_table_name) + N'
                   (INTF_ID, INTF_TYPE);
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
    IF XACT_STATE() <> 0 ROLLBACK TRAN;

    DECLARE
          @v_err_msg       NVARCHAR(4000) = ERROR_MESSAGE()
        , @v_err_msg_proc  NVARCHAR(128)  = ISNULL(ERROR_PROCEDURE(), N''), @v_err_msg_line  INT            = ERROR_LINE()
        , @v_err_msg_num   INT            = ERROR_NUMBER();

    DECLARE @msg NVARCHAR(2048) = FORMATMESSAGE(N'[CCDT_C510_CONFIG_BYPASS] DDL failed. %s (PROC=%s, LINE=%d, ERR=%d)', @v_err_msg, ISNULL(@v_err_msg_proc, N''), @v_err_msg_proc, @v_err_msg_num);
    THROW 51000, @msg, 1;
END CATCH;
GO

/*------------------------------------------------------------------------------
Operator notes:

-- Fetch bypass rules for an interface/type, in execution order
SELECT *
FROM dbo.CCDT_C510_CONFIG_BYPASS
WHERE INTF_ID = @IntfId
  AND INTF_TYPE = @IntfType
ORDER BY CAT_DESC, SEQ;

------------------------------------------------------------------------------*/

