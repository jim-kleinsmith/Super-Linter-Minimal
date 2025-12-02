/*==============================================================================
    SCRIPT NAME   : DDL_CCDT_C410_CONFIG_SELECTION.sql
    AUTHOR        : COTIVITI
    CREATED DATE  : 10/28/2025

    MODIFICATION HISTORY
      10/28/2025  COTIVITI  Build 3.00.00.00
                   Initial SQL Server deployment of dbo.CCDT_C410_CONFIG_SELECTION.

    BUILD 3.0.0.0
      X → Major version (interface, behavior, or compatibility changes)
      Y → Minor update (additions/enhancements)
      Z → Patch (bug fixes/maintenance)
      W → Client-specific requirement (custom objects/routines)

    PURPOSE
      Deterministic, idempotent creation of dbo.CCDT_C410_CONFIG_SELECTION —
      rule-based selection criteria for outbound files (daily/history; PROF/INST).
      Includes supporting indexes, defaults, compression, and documentation.

    ASSUMPTIONS
      • Executed in the correct database with DDL permissions.
      • SQL Server 2017 or later (supports compression and filtered indexes).
      • Schema dbo exists and is default schema for this deployment.
      • Script may be safely re-run (idempotent).

    OBJECTS CREATED / MODIFIED
      • TABLE   : dbo.CCDT_C410_CONFIG_SELECTION
      • INDEX   : UQ_C410_SELECTION_RULE (UNIQUE on INTF_ID, INTF_TYPE, CAT_DESC, SEQ)
      • INDEX   : IX_C410_SELECTION_INTF_TYPE (INTF_ID, INTF_TYPE)
      • CONSTRAINTS: PK, DF (CREATE_DTM, START_DATE, END_DATE)
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
        , @c_table_name             SYSNAME        = N'CCDT_C410_CONFIG_SELECTION'
        , @c_pk_name                SYSNAME        = N'PK_CCDT_C410_CONFIG_SELECTION'
        , @c_uq_rule_name           SYSNAME        = N'UQ_C410_SELECTION_RULE'
        , @c_ix_intf_type           SYSNAME        = N'IX_C410_SELECTION_INTF_TYPE'
        , @c_df_create_dtm          SYSNAME        = N'DF_C410_CONFIG_SELECTION_CREATE_DTM'
        , @c_df_start_date          SYSNAME        = N'DF_C410_CONFIG_SELECTION_START_DATE'
        , @c_df_end_date            SYSNAME        = N'DF_C410_CONFIG_SELECTION_END_DATE'
        , @c_ep_ms_description      SYSNAME        = N'MS_Description'
        , @c_table_desc             NVARCHAR(4000) = N'Configuration of outbound file selection criteria (daily/history; PROF/INST).';

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
                  PK_ID               NUMERIC(38,0)  IDENTITY(1,1) NOT NULL
                , INTF_ID             INT                           NOT NULL
                , INTF_TYPE           VARCHAR(4)                    NOT NULL
                , CAT_DESC            VARCHAR(50)                   NOT NULL
                , SEQ                 INT                           NOT NULL
                , AND_OR              VARCHAR(1)                    NOT NULL
                , DB_TABLE            VARCHAR(35)                   NOT NULL
                , DB_FIELD            VARCHAR(30)                   NOT NULL
                , COMPARISON          VARCHAR(2)                    NOT NULL
                , ITEM_VALUE1         VARCHAR(50)                   NOT NULL
                , ITEM_VALUE2         VARCHAR(50)                       NULL
                , DATE_FILTER_TYPE    VARCHAR(20)                   NOT NULL
                , START_DATE          DATETIME                          NULL
                    CONSTRAINT ' + QUOTENAME(@c_df_start_date) + N' DEFAULT (''1900-01-01T00:00:00'')
                , END_DATE            DATETIME                          NULL
                    CONSTRAINT ' + QUOTENAME(@c_df_end_date) + N' DEFAULT (''9999-12-31T00:00:00'')
                , CREATE_DTM          DATETIME                      NOT NULL
                    CONSTRAINT ' + QUOTENAME(@c_df_create_dtm) + N' DEFAULT (SYSUTCDATETIME())
                , CREATE_USID         VARCHAR(50)                   NOT NULL
                , LAST_UPD_DTM        DATETIME                      NOT NULL
                , LAST_UPD_USID       VARCHAR(50)                   NOT NULL
                , CONSTRAINT ' + QUOTENAME(@c_pk_name) + N'
                    PRIMARY KEY CLUSTERED (PK_ID)
            );
        ';
        EXEC sys.sp_executesql @sql;

        SET @v_object_id = OBJECT_ID(QUOTENAME(@c_schema_name) + N'.' + QUOTENAME(@c_table_name));
    END
    ELSE
    BEGIN
        -- Ensure required defaults on CREATE_DTM, START_DATE, END_DATE
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

        IF NOT EXISTS (
            SELECT 1
            FROM sys.default_constraints dc
            JOIN sys.columns c
              ON c.object_id = dc.parent_object_id
             AND c.column_id = dc.parent_column_id
            WHERE dc.parent_object_id = @v_object_id
              AND dc.name = @c_df_start_date
              AND c.name = N'START_DATE'
        )
        BEGIN
            SET @sql = N'
                ALTER TABLE ' + QUOTENAME(@c_schema_name) + N'.' + QUOTENAME(@c_table_name) + N'
                    ADD CONSTRAINT ' + QUOTENAME(@c_df_start_date) + N'
                        DEFAULT (''1900-01-01T00:00:00'') FOR START_DATE;
            ';
        EXEC sys.sp_executesql @sql;
        END;

        IF NOT EXISTS (
            SELECT 1
            FROM sys.default_constraints dc
            JOIN sys.columns c
              ON c.object_id = dc.parent_object_id
             AND c.column_id = dc.parent_column_id
            WHERE dc.parent_object_id = @v_object_id
              AND dc.name = @c_df_end_date
              AND c.name = N'END_DATE'
        )
        BEGIN
            SET @sql = N'
                ALTER TABLE ' + QUOTENAME(@c_schema_name) + N'.' + QUOTENAME(@c_table_name) + N'
                    ADD CONSTRAINT ' + QUOTENAME(@c_df_end_date) + N'
                        DEFAULT (''9999-12-31T00:00:00'') FOR END_DATE;
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

    -- Column descriptions
    DECLARE
          @c_col_desc_cdtm  NVARCHAR(4000) = N'UTC create timestamp; defaulted via SYSUTCDATETIME().'
        , @c_col_desc_sdt   NVARCHAR(4000) = N'Default start date (1900-01-01) when range is open-ended.'
        , @c_col_desc_edt   NVARCHAR(4000) = N'Default end date (9999-12-31) when range is open-ended.';

    -- CREATE_DTM
    IF EXISTS (
        SELECT 1 FROM sys.extended_properties
        WHERE class=1 AND name=@c_ep_ms_description AND major_id=@v_object_id
          AND minor_id = COLUMNPROPERTY(@v_object_id, N'CREATE_DTM', 'ColumnId')
    )
        EXEC sys.sp_updateextendedproperty
             @name=@c_ep_ms_description, @value=@c_col_desc_cdtm,
             @level0type=N'SCHEMA',@level0name=@c_schema_name,
             @level1type=N'TABLE', @level1name=@c_table_name,
             @level2type=N'COLUMN',@level2name=N'CREATE_DTM';
    ELSE
        EXEC sys.sp_addextendedproperty
             @name=@c_ep_ms_description, @value=@c_col_desc_cdtm,
             @level0type=N'SCHEMA',@level0name=@c_schema_name,
             @level1type=N'TABLE', @level1name=@c_table_name,
             @level2type=N'COLUMN',@level2name=N'CREATE_DTM';

    -- START_DATE
    IF EXISTS (
        SELECT 1 FROM sys.extended_properties
        WHERE class=1 AND name=@c_ep_ms_description AND major_id=@v_object_id
          AND minor_id = COLUMNPROPERTY(@v_object_id, N'START_DATE', 'ColumnId')
    )
        EXEC sys.sp_updateextendedproperty
             @name=@c_ep_ms_description, @value=@c_col_desc_sdt,
             @level0type=N'SCHEMA',@level0name=@c_schema_name,
             @level1type=N'TABLE', @level1name=@c_table_name,
             @level2type=N'COLUMN',@level2name=N'START_DATE';
    ELSE
        EXEC sys.sp_addextendedproperty
             @name=@c_ep_ms_description, @value=@c_col_desc_sdt,
             @level0type=N'SCHEMA',@level0name=@c_schema_name,
             @level1type=N'TABLE', @level1name=@c_table_name,
             @level2type=N'COLUMN',@level2name=N'START_DATE';

    -- END_DATE
    IF EXISTS (
        SELECT 1 FROM sys.extended_properties
        WHERE class=1 AND name=@c_ep_ms_description AND major_id=@v_object_id
          AND minor_id = COLUMNPROPERTY(@v_object_id, N'END_DATE', 'ColumnId')
    )
        EXEC sys.sp_updateextendedproperty
             @name=@c_ep_ms_description, @value=@c_col_desc_edt,
             @level0type=N'SCHEMA',@level0name=@c_schema_name,
             @level1type=N'TABLE', @level1name=@c_table_name,
             @level2type=N'COLUMN',@level2name=N'END_DATE';
    ELSE
        EXEC sys.sp_addextendedproperty
             @name=@c_ep_ms_description, @value=@c_col_desc_edt,
             @level0type=N'SCHEMA',@level0name=@c_schema_name,
             @level1type=N'TABLE', @level1name=@c_table_name,
             @level2type=N'COLUMN',@level2name=N'END_DATE';

    COMMIT TRAN;
END TRY
BEGIN CATCH
    IF XACT_STATE() <> 0 ROLLBACK TRAN;

    DECLARE
          @v_err_msg       NVARCHAR(4000) = ERROR_MESSAGE()
        , @v_err_msg_proc  NVARCHAR(128)  = ISNULL(ERROR_PROCEDURE(), N''), @v_err_msg_line  INT            = ERROR_LINE()
        , @v_err_msg_num   INT            = ERROR_NUMBER();

    DECLARE @msg NVARCHAR(2048) = FORMATMESSAGE(N'[CCDT_C410_CONFIG_SELECTION] DDL failed. %s (PROC=%s, LINE=%d, ERR=%d)', @v_err_msg, ISNULL(@v_err_msg_proc, N''), @v_err_msg_proc, @v_err_msg_num);
    THROW 51000, @msg, 1;
END CATCH;
GO

/*------------------------------------------------------------------------------
Operator notes:

-- Fetch rules for an interface/type, in execution order
SELECT *
FROM dbo.CCDT_C410_CONFIG_SELECTION
WHERE INTF_ID = @IntfId
  AND INTF_TYPE = @IntfType
ORDER BY CAT_DESC, SEQ;

------------------------------------------------------------------------------*/

