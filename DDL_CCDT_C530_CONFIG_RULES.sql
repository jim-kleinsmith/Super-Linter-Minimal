/*==============================================================================
    SCRIPT NAME   : DDL_CCDT_C530_CONFIG_RULES.sql
    AUTHOR        : COTIVITI
    CREATED DATE  : 10/28/2025

    MODIFICATION HISTORY
      10/28/2025  COTIVITI  Build 3.00.00.00
                   Initial SQL Server deployment of dbo.CCDT_C530_CONFIG_RULES.

    BUILD 3.0.0.0
      X → Major version (interface, behavior, or compatibility changes)
      Y → Minor update (additions/enhancements)
      Z → Patch (bug fixes/maintenance)
      W → Client-specific requirement (custom objects/routines)

    PURPOSE
      Deterministic, idempotent creation of dbo.CCDT_C530_CONFIG_RULES —
      joins bypass rules to bypass categories; each BYPASS_RULE_TYPE corresponds
      to specific logic and value triggers. Includes supporting index, compression,
      and documentation.

    ASSUMPTIONS
      • Executed in the correct database with DDL permissions.
      • SQL Server 2017 or later (supports compression).
      • Schema dbo exists and is default schema for this deployment.
      • Script may be safely re-run (idempotent).

    OBJECTS CREATED / MODIFIED
      • TABLE   : dbo.CCDT_C530_CONFIG_RULES
      • INDEX   : IX_C530_RULES_RULETYPE_VALS (BYPASS_RULE_TYPE, BYPASS_VALUE_1, BYPASS_VALUE_2)
      • CONSTRAINTS: PK (BYPASS_RULE_ID, INTF_ID, BYPASS_CATG_ID), DF (CREATE_DTM)
      • EXTENDED PROPERTIES for documentation
      • ROW compression applied to all indexes

    PERFORMANCE NOTES
      • Clustered composite PK on (BYPASS_RULE_ID, INTF_ID, BYPASS_CATG_ID).
      • Supporting index accelerates rule lookups by type/value.
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
        , @c_table_name            SYSNAME        = N'CCDT_C530_CONFIG_RULES'
        , @c_pk_name               SYSNAME        = N'PK_CCDT_C530_CONFIG_RULES'
        , @c_df_create_dtm         SYSNAME        = N'DF_C530_CONFIG_RULES_CREATE_DTM'
        , @c_ix_ruletype_vals      SYSNAME        = N'IX_C530_RULES_RULETYPE_VALS'
        , @c_ep_ms_description     SYSNAME        = N'MS_Description'
        , @c_table_desc            NVARCHAR(4000) = N'Bypass rules mapped to bypass categories; value-driven triggers by rule type.';

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
                  BYPASS_RULE_ID    VARCHAR(20)                 NOT NULL  -- e.g., ''R1''
                , INTF_ID           INT                         NOT NULL  -- e.g., 1, 2, 3
                , BYPASS_CATG_ID    VARCHAR(10)                 NOT NULL  -- links to bypass category/catalog
                , BYPASS_RULE_TYPE  VARCHAR(50)                 NOT NULL  -- e.g., ''IPCD_ID'' | ''CDML_EOB_EXCD'' | ...
                , BYPASS_DB_REF_1   VARCHAR(100)                    NULL  -- e.g., ''CDML.IPCD_ID''
                , BYPASS_VALUE_1    VARCHAR(50)                     NULL
                , BYPASS_DB_REF_2   VARCHAR(100)                    NULL
                , BYPASS_VALUE_2    VARCHAR(50)                     NULL
                , BYPASS_DB_REF_3   VARCHAR(100)                    NULL
                , BYPASS_VALUE_3    VARCHAR(50)                     NULL
                , BYPASS_RULE_DESC  VARCHAR(200)                    NULL
                , BYPASS_EFF_DT     DATETIME                        NULL
                , BYPASS_TERM_DT    DATETIME                        NULL
                , CREATE_DTM        DATETIME                    NOT NULL
                    CONSTRAINT ' + QUOTENAME(@c_df_create_dtm) + N' DEFAULT (SYSUTCDATETIME())
                , CREATE_USID       VARCHAR(50)                 NOT NULL
                , LAST_UPD_DTM      DATETIME                        NULL
                , LAST_UPD_USID     VARCHAR(50)                     NULL
                , CONSTRAINT ' + QUOTENAME(@c_pk_name) + N'
                    PRIMARY KEY CLUSTERED (BYPASS_RULE_ID, INTF_ID, BYPASS_CATG_ID)
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
    IF NOT EXISTS (
        SELECT 1
        FROM sys.indexes
        WHERE object_id = @v_object_id
          AND name = @c_ix_ruletype_vals
    )
    BEGIN
        SET @sql = N'
            CREATE NONCLUSTERED INDEX ' + QUOTENAME(@c_ix_ruletype_vals) + N'
                ON ' + QUOTENAME(@c_schema_name) + N'.' + QUOTENAME(@c_table_name) + N'
                   (BYPASS_RULE_TYPE, BYPASS_VALUE_1, BYPASS_VALUE_2);
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

    -- Column: BYPASS_RULE_TYPE description
    DECLARE @c_col_desc_rule_type NVARCHAR(4000) =
        N'Rule type key driving bypass logic (e.g., IPCD_ID, CDML_EOB_EXCD, CDML_DISALL_EXCD, MCTN_ID).';
    IF EXISTS (
        SELECT 1
        FROM sys.extended_properties
        WHERE class = 1
          AND name = @c_ep_ms_description
          AND major_id = @v_object_id
          AND minor_id = COLUMNPROPERTY(@v_object_id, N'BYPASS_RULE_TYPE', 'ColumnId')
    )
    BEGIN
        EXEC sys.sp_updateextendedproperty
             @name = @c_ep_ms_description,
             @value = @c_col_desc_rule_type,
             @level0type=N'SCHEMA', @level0name=@c_schema_name,
             @level1type=N'TABLE',  @level1name=@c_table_name,
             @level2type=N'COLUMN', @level2name=N'BYPASS_RULE_TYPE';
    END
    ELSE
    BEGIN
        EXEC sys.sp_addextendedproperty
             @name = @c_ep_ms_description,
             @value = @c_col_desc_rule_type,
             @level0type=N'SCHEMA', @level0name=@c_schema_name,
             @level1type=N'TABLE',  @level1name=@c_table_name,
             @level2type=N'COLUMN', @level2name=N'BYPASS_RULE_TYPE';
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
        FORMATMESSAGE(N'[CCDT_C530_CONFIG_RULES] failed. %s (PROC=%s, LINE=%d, ERR=%d)',
                      @v_err_msg, @v_err_msg_proc, @v_err_msg_line, @v_err_msg_num);

    THROW 51000, @v_fmt_msg, 1;
END CATCH;
GO

/*------------------------------------------------------------------------------
Operator notes:

-- Example lookup for rules by type
SELECT *
FROM dbo.CCDT_C530_CONFIG_RULES
WHERE BYPASS_RULE_TYPE = @RuleType
ORDER BY BYPASS_CATG_ID, BYPASS_RULE_ID;

------------------------------------------------------------------------------*/


