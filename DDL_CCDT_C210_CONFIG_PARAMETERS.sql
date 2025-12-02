/*==============================================================================
    SCRIPT NAME   : DDL_CCDT_C210_CONFIG_PARAMETERS.sql
    AUTHOR        : COTIVITI
    CREATED DATE  : 10/28/2025

    PURPOSE
      Idempotent creation/repair of dbo.CCDT_C210_CONFIG_PARAMETERS — parameter
      values per interface ID. Includes PK, default, compression, documentation.

    ASSUMPTIONS
      • Executed in the correct database with DDL permissions.
      • SQL Server 2017+.
==============================================================================*/
GO
USE YourDatabaseName;
GO

SET NOCOUNT ON;
SET XACT_ABORT ON;
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

BEGIN TRY
    BEGIN TRAN;

    DECLARE
          @c_schema_name           SYSNAME        = N'dbo'
        , @c_table_name            SYSNAME        = N'CCDT_C210_CONFIG_PARAMETERS'
        , @c_pk_name               SYSNAME        = N'PK_CCDT_C210_CONFIG_PARAMETERS'
        , @c_df_create_dtm         SYSNAME        = N'DF_CCDT_C210_CONFIG_PARAMETERS_CREATE_DTM'
        , @c_ep_ms_description     SYSNAME        = N'MS_Description'
        , @c_table_desc            NVARCHAR(4000) = N'Parameter values per interface ID.';

    DECLARE
          @v_object_id INT = OBJECT_ID(QUOTENAME(@c_schema_name) + N'.' + QUOTENAME(@c_table_name))
        , @sql         NVARCHAR(MAX);

    /* Create table if not exists */
    IF @v_object_id IS NULL
    BEGIN
        SET @sql = N'CREATE TABLE ' + QUOTENAME(@c_schema_name) + N'.' + QUOTENAME(@c_table_name) + N'(
              INTF_ID        INT             NOT NULL
            , PARM_NAME      NVARCHAR(50)    NOT NULL
            , PARM_TYPE      NVARCHAR(50)    NOT NULL
            , PARM_DESC      NVARCHAR(100)       NULL
            , PARM_VALUE     NVARCHAR(4000)  NOT NULL
            , CREATE_DTM     DATETIME2(3)    NOT NULL
                CONSTRAINT ' + QUOTENAME(@c_df_create_dtm) + N' DEFAULT (SYSUTCDATETIME())
            , CREATE_USID    NVARCHAR(50)    NOT NULL
            , LAST_UPD_DTM   DATETIME2(3)    NOT NULL
            , LAST_UPD_USID  NVARCHAR(50)    NOT NULL
            , CONSTRAINT ' + QUOTENAME(@c_pk_name) + N'
                PRIMARY KEY CLUSTERED (INTF_ID, PARM_NAME)
        );';
        EXEC sys.sp_executesql @sql;

        SET @v_object_id = OBJECT_ID(QUOTENAME(@c_schema_name) + N'.' + QUOTENAME(@c_table_name));
    END
    ELSE
    BEGIN
        /* Ensure default on CREATE_DTM exists */
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
            SET @sql = N'ALTER TABLE ' + QUOTENAME(@c_schema_name) + N'.' + QUOTENAME(@c_table_name) + N'
                        ADD CONSTRAINT ' + QUOTENAME(@c_df_create_dtm) + N'
                        DEFAULT (SYSUTCDATETIME()) FOR CREATE_DTM;';
            EXEC sys.sp_executesql @sql;
        END;
    END;

    /* Apply row compression to all indexes */
    SET @sql = N'ALTER INDEX ALL ON ' + QUOTENAME(@c_schema_name) + N'.' + QUOTENAME(@c_table_name) +
               N' REBUILD WITH (DATA_COMPRESSION = ROW);';
    EXEC sys.sp_executesql @sql;

    /* Extended properties (add-or-update) */
    IF EXISTS (
        SELECT 1 FROM sys.extended_properties
        WHERE class = 1 AND name = @c_ep_ms_description
          AND major_id = @v_object_id AND minor_id = 0
    )
        EXEC sys.sp_updateextendedproperty
             @name=@c_ep_ms_description, @value=@c_table_desc,
             @level0type=N'SCHEMA', @level0name=@c_schema_name,
             @level1type=N'TABLE',  @level1name=@c_table_name;
    ELSE
        EXEC sys.sp_addextendedproperty
             @name=@c_ep_ms_description, @value=@c_table_desc,
             @level0type=N'SCHEMA', @level0name=@c_schema_name,
             @level1type=N'TABLE',  @level1name=@c_table_name;

    /* Column descriptions */
    DECLARE
          @c_col_desc_cdtm  NVARCHAR(4000) = N'UTC create timestamp; defaulted via SYSUTCDATETIME().'
        , @c_col_desc_pname NVARCHAR(4000) = N'Parameter name (unique within an interface).'
        , @c_col_desc_ptype NVARCHAR(4000) = N'Parameter type/category label.'
        , @c_col_desc_pval  NVARCHAR(4000) = N'Parameter value (up to 4000 chars).';

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

    -- PARM_NAME
    IF EXISTS (
        SELECT 1 FROM sys.extended_properties
        WHERE class=1 AND name=@c_ep_ms_description AND major_id=@v_object_id
          AND minor_id = COLUMNPROPERTY(@v_object_id, N'PARM_NAME', 'ColumnId')
    )
        EXEC sys.sp_updateextendedproperty
             @name=@c_ep_ms_description, @value=@c_col_desc_pname,
             @level0type=N'SCHEMA',@level0name=@c_schema_name,
             @level1type=N'TABLE', @level1name=@c_table_name,
             @level2type=N'COLUMN',@level2name=N'PARM_NAME';
    ELSE
        EXEC sys.sp_addextendedproperty
             @name=@c_ep_ms_description, @value=@c_col_desc_pname,
             @level0type=N'SCHEMA',@level0name=@c_schema_name,
             @level1type=N'TABLE', @level1name=@c_table_name,
             @level2type=N'COLUMN',@level2name=N'PARM_NAME';

    -- PARM_TYPE
    IF EXISTS (
        SELECT 1 FROM sys.extended_properties
        WHERE class=1 AND name=@c_ep_ms_description AND major_id=@v_object_id
          AND minor_id = COLUMNPROPERTY(@v_object_id, N'PARM_TYPE', 'ColumnId')
    )
        EXEC sys.sp_updateextendedproperty
             @name=@c_ep_ms_description, @value=@c_col_desc_ptype,
             @level0type=N'SCHEMA',@level0name=@c_schema_name,
             @level1type=N'TABLE', @level1name=@c_table_name,
             @level2type=N'COLUMN',@level2name=N'PARM_TYPE';
    ELSE
        EXEC sys.sp_addextendedproperty
             @name=@c_ep_ms_description, @value=@c_col_desc_ptype,
             @level0type=N'SCHEMA',@level0name=@c_schema_name,
             @level1type=N'TABLE', @level1name=@c_table_name,
             @level2type=N'COLUMN',@level2name=N'PARM_TYPE';

    -- PARM_VALUE
    IF EXISTS (
        SELECT 1 FROM sys.extended_properties
        WHERE class=1 AND name=@c_ep_ms_description AND major_id=@v_object_id
          AND minor_id = COLUMNPROPERTY(@v_object_id, N'PARM_VALUE', 'ColumnId')
    )
        EXEC sys.sp_updateextendedproperty
             @name=@c_ep_ms_description, @value=@c_col_desc_pval,
             @level0type=N'SCHEMA',@level0name=@c_schema_name,
             @level1type=N'TABLE', @level1name=@c_table_name,
             @level2type=N'COLUMN',@level2name=N'PARM_VALUE';
    ELSE
        EXEC sys.sp_addextendedproperty
             @name=@c_ep_ms_description, @value=@c_col_desc_pval,
             @level0type=N'SCHEMA',@level0name=@c_schema_name,
             @level1type=N'TABLE', @level1name=@c_table_name,
             @level2type=N'COLUMN',@level2name=N'PARM_VALUE';

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
        FORMATMESSAGE(N'[CCDT_C210_CONFIG_PARAMETERS] failed. %s (PROC=%s, LINE=%d, ERR=%d)',
                      @v_err_msg, @v_err_msg_proc, @v_err_msg_line, @v_err_msg_num);

    THROW 51000, @v_fmt_msg, 1;
END CATCH;
GO

/*------------------------------------------------------------------------------
Operator notes:

-- Typical lookup for all parameters for a given interface
SELECT  p.PARM_NAME, p.PARM_VALUE
FROM    dbo.CCDT_C210_CONFIG_PARAMETERS AS p
WHERE   p.INTF_ID = @IntfId
ORDER BY p.PARM_NAME;

------------------------------------------------------------------------------*/

