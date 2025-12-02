/*==============================================================================
    SCRIPT NAME   : DDL_CCDT_C310_CONFIG_BACKSWEEP.sql
    AUTHOR        : COTIVITI
    CREATED DATE  : 10/28/2025

    MODIFICATION HISTORY
      10/28/2025  COTIVITI  Build 3.00.00.00
                   Initial SQL Server deployment of dbo.CCDT_C310_CONFIG_BACKSWEEP.

    BUILD 3.0.0.0
      X → Major version (interface, behavior, or compatibility changes)
      Y → Minor update (additions/enhancements)
      Z → Patch (bug fixes/maintenance)
      W → Client-specific requirement (custom objects/routines)

    PURPOSE
      Deterministic, idempotent creation of dbo.CCDT_C310_CONFIG_BACKSWEEP to match
      the DML seed/sync script expectations. Includes PK, unique clustered index on
      (INTF_ID, RUN), data validation, compression, and documentation.

    ASSUMPTIONS
      • Executed in the correct database with DDL permissions.
      • SQL Server 2017+ (SYSUTCDATETIME, compression).
      • Schema dbo exists.
      • Script may be safely re-run (idempotent).

    OBJECTS CREATED / MODIFIED
      • TABLE          : dbo.CCDT_C310_CONFIG_BACKSWEEP
      • CONSTRAINTS    : PK (PK_ID NONCLUSTERED), DF (CREATE_DTM),
                         CK (INTF_TYPE, PROCESS_STATUS, DATE_RANGE)
      • INDEXES        : UNIQUE CLUSTERED on (INTF_ID, RUN)
      • EXTENDED PROPS : table & columns
      • ROW compression applied to all indexes

    PERFORMANCE NOTES
      • Unique **clustered** index on (INTF_ID, RUN) aligns with DML’s business key
        for efficient MERGE-like UPDATE/INSERT.
      • PK_ID retained as surrogate key (NONCLUSTERED).

    SECURITY / PII
      • Contains no PHI/PII.
      • CREATE_DTM defaulted via SYSUTCDATETIME().

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
    -- Constants (names and IDs)
    ------------------------------------------------------------------
    DECLARE
          @c_schema_name              SYSNAME        = N'dbo'
        , @c_table_name               SYSNAME        = N'CCDT_C310_CONFIG_BACKSWEEP'
        , @c_pk_name                  SYSNAME        = N'PK_CCDT_C310_CONFIG_BACKSWEEP'
        , @c_df_create_dtm            SYSNAME        = N'DF_CCDT_C310_CONFIG_BACKSWEEP_CREATE_DTM'
        , @c_ck_intf_type             SYSNAME        = N'CK_CCDT_C310_CONFIG_BACKSWEEP_INTF_TYPE'
        , @c_ck_proc_status           SYSNAME        = N'CK_CCDT_C310_CONFIG_BACKSWEEP_PROCESS_STATUS'
        , @c_ck_date_range            SYSNAME        = N'CK_CCDT_C310_CONFIG_BACKSWEEP_DATE_RANGE'
        , @c_cx_intf_run              SYSNAME        = N'CX_CCDT_C310_CONFIG_BACKSWEEP_INTF_RUN'  -- unique clustered
        , @c_ep_ms_description        SYSNAME        = N'MS_Description'
        , @c_table_desc               NVARCHAR(4000) = N'Backsweep configuration by interface and run; (INTF_ID, RUN) is the natural key used for idempotent sync.'
        ;

    DECLARE
          @v_object_id                INT = OBJECT_ID(QUOTENAME(@c_schema_name) + N'.' + QUOTENAME(@c_table_name));

    ------------------------------------------------------------------
    -- Create table if not exists
    ------------------------------------------------------------------
    IF @v_object_id IS NULL
    BEGIN
        SET @sql = N'
            CREATE TABLE ' + QUOTENAME(@c_schema_name) + N'.' + QUOTENAME(@c_table_name) + N'
            (
                  PK_ID           INT IDENTITY(1,1)    NOT NULL
                , INTF_ID         INT                  NOT NULL   -- 11=PROF, 12=INST
                , INTF_TYPE       VARCHAR(4)           NOT NULL   -- ''PROF'' | ''INST''
                , CREATE_DTM      DATETIME2(3)         NOT NULL
                    CONSTRAINT ' + QUOTENAME(@c_df_create_dtm) + N' DEFAULT (SYSUTCDATETIME())
                , PROCESS_STATUS  VARCHAR(2)           NOT NULL   -- ''01'' Awaiting, ''99'' Complete
                , APPLIED_DTM     DATETIME2(3)             NULL
                , BEGIN_DATE      DATETIME2(3)         NOT NULL
                , END_DATE        DATETIME2(3)         NOT NULL
                , RUN             INT                  NOT NULL   -- file run number; increments by 10
                , FILENAME        VARCHAR(100)         NOT NULL

                , CONSTRAINT ' + QUOTENAME(@c_pk_name) + N'
                    PRIMARY KEY NONCLUSTERED (PK_ID)

                , CONSTRAINT ' + QUOTENAME(@c_ck_intf_type) + N'
                    CHECK (INTF_TYPE IN (''PROF'',''INST''))

                , CONSTRAINT ' + QUOTENAME(@c_ck_proc_status) + N'
                    CHECK (PROCESS_STATUS IN (''01'',''99''))

                , CONSTRAINT ' + QUOTENAME(@c_ck_date_range) + N'
                    CHECK (BEGIN_DATE <= END_DATE)
            );
        ';
        EXEC sys.sp_executesql @sql;

        -- Unique **clustered** index on business key (INTF_ID, RUN)
        SET @sql = N'
            CREATE UNIQUE CLUSTERED INDEX ' + QUOTENAME(@c_cx_intf_run) + N'
            ON ' + QUOTENAME(@c_schema_name) + N'.' + QUOTENAME(@c_table_name) + N' (INTF_ID, RUN)
            WITH (DATA_COMPRESSION = ROW);
        ';
        EXEC sys.sp_executesql @sql;

        SET @v_object_id = OBJECT_ID(QUOTENAME(@c_schema_name) + N'.' + QUOTENAME(@c_table_name));
    END
    ELSE
    BEGIN
        ------------------------------------------------------------------
        -- If table exists, ensure required defaults/constraints/indexes
        ------------------------------------------------------------------

        -- Should return one row named CX_CCDT_C310_CONFIG_BACKSWEEP_INTF_RUN
        IF NOT EXISTS (SELECT i.name, i.type_desc, i.is_unique, i.is_primary_key, i.is_unique_constraint
                       FROM   sys.indexes i
                       WHERE  i.object_id = OBJECT_ID('dbo.CCDT_C310_CONFIG_BACKSWEEP')
                         AND  i.name      = 'CX_CCDT_C310_CONFIG_BACKSWEEP_INTF_RUN'
                      )
        BEGIN
           CREATE UNIQUE CLUSTERED INDEX CX_CCDT_C310_CONFIG_BACKSWEEP_INTF_RUN
                                      ON dbo.CCDT_C310_CONFIG_BACKSWEEP (INTF_ID, RUN)
                                    WITH (DATA_COMPRESSION = ROW);
        END;

        -- CREATE_DTM default
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

        -- CHECK: INTF_TYPE
        IF NOT EXISTS (
            SELECT 1 FROM sys.check_constraints
             WHERE name = @c_ck_intf_type AND parent_object_id = @v_object_id
        )
        BEGIN
            SET @sql = N'
                ALTER TABLE ' + QUOTENAME(@c_schema_name) + N'.' + QUOTENAME(@c_table_name) + N'
                WITH NOCHECK
                ADD CONSTRAINT ' + QUOTENAME(@c_ck_intf_type) + N'
                    CHECK (INTF_TYPE IN (''PROF'',''INST''));
                ALTER TABLE ' + QUOTENAME(@c_schema_name) + N'.' + QUOTENAME(@c_table_name) + N'
                CHECK CONSTRAINT ' + QUOTENAME(@c_ck_intf_type) + N';
            ';
        EXEC sys.sp_executesql @sql;
        END;

        -- CHECK: PROCESS_STATUS
        IF NOT EXISTS (
            SELECT 1 FROM sys.check_constraints
             WHERE name = @c_ck_proc_status AND parent_object_id = @v_object_id
        )
        BEGIN
            SET @sql = N'
                ALTER TABLE ' + QUOTENAME(@c_schema_name) + N'.' + QUOTENAME(@c_table_name) + N'
                WITH NOCHECK
                ADD CONSTRAINT ' + QUOTENAME(@c_ck_proc_status) + N'
                    CHECK (PROCESS_STATUS IN (''01'',''99''));
                ALTER TABLE ' + QUOTENAME(@c_schema_name) + N'.' + QUOTENAME(@c_table_name) + N'
                CHECK CONSTRAINT ' + QUOTENAME(@c_ck_proc_status) + N';
            ';
        EXEC sys.sp_executesql @sql;
        END;

        -- CHECK: DATE_RANGE
        IF NOT EXISTS (
            SELECT 1 FROM sys.check_constraints
             WHERE name = @c_ck_date_range AND parent_object_id = @v_object_id
        )
        BEGIN
            SET @sql = N'
                ALTER TABLE ' + QUOTENAME(@c_schema_name) + N'.' + QUOTENAME(@c_table_name) + N'
                WITH NOCHECK
                ADD CONSTRAINT ' + QUOTENAME(@c_ck_date_range) + N'
                    CHECK (BEGIN_DATE <= END_DATE);
                ALTER TABLE ' + QUOTENAME(@c_schema_name) + N'.' + QUOTENAME(@c_table_name) + N'
                CHECK CONSTRAINT ' + QUOTENAME(@c_ck_date_range) + N';
            ';
        EXEC sys.sp_executesql @sql;
        END;

        -- UNIQUE CLUSTERED index on (INTF_ID, RUN)
        IF NOT EXISTS (
            SELECT 1
            FROM sys.indexes
            WHERE object_id = @v_object_id
              AND name = @c_cx_intf_run
        )
        BEGIN
            -- If a different clustered index exists, keep it NONCLUSTERED and add clustered here.
            DECLARE @clustered_ix SYSNAME =
                (SELECT TOP (1) name FROM sys.indexes WHERE object_id=@v_object_id AND type=1 AND is_hypothetical=0 AND name <> @c_cx_intf_run);
            IF @clustered_ix IS NOT NULL
            BEGIN
                -- Switch existing clustered to nonclustered (requires drop/recreate); skip to avoid disruption.
                -- Instead, create UNIQUE NONCLUSTERED if a clustered already exists.
                SET @sql = N'
                    CREATE UNIQUE NONCLUSTERED INDEX ' + QUOTENAME(@c_cx_intf_run) + N'
                    ON ' + QUOTENAME(@c_schema_name) + N'.' + QUOTENAME(@c_table_name) + N' (INTF_ID, RUN)
                    WITH (DATA_COMPRESSION = ROW);
                ';
        EXEC sys.sp_executesql @sql;
            END
            ELSE
            BEGIN
                SET @sql = N'
                    CREATE UNIQUE CLUSTERED INDEX ' + QUOTENAME(@c_cx_intf_run) + N'
                    ON ' + QUOTENAME(@c_schema_name) + N'.' + QUOTENAME(@c_table_name) + N' (INTF_ID, RUN)
                    WITH (DATA_COMPRESSION = ROW);
                ';
        EXEC sys.sp_executesql @sql;
            END
        END
    END;

    ------------------------------------------------------------------
    -- Apply row compression to all indexes on the table
    ------------------------------------------------------------------
    SET @sql = N'ALTER INDEX ALL ON ' + QUOTENAME(@c_schema_name) + N'.' + QUOTENAME(@c_table_name) +
         N' REBUILD WITH (DATA_COMPRESSION = ROW);';
        EXEC sys.sp_executesql @sql;

    ------------------------------------------------------------------
    -- Extended properties (add-or-update)
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
          @c_col_desc_pkid    NVARCHAR(4000) = N'Surrogate key (IDENTITY). Not used by seed script.'
        , @c_col_desc_intfid  NVARCHAR(4000) = N'Interface ID (11=Professional, 12=Institutional).'
        , @c_col_desc_intftp  NVARCHAR(4000) = N'Interface type: PROF | INST.'
        , @c_col_desc_cdtm    NVARCHAR(4000) = N'UTC create timestamp; defaulted via SYSUTCDATETIME().'
        , @c_col_desc_pstat   NVARCHAR(4000) = N'Process status: 01=Awaiting, 99=Complete.'
        , @c_col_desc_apdtm   NVARCHAR(4000) = N'UTC applied timestamp; nullable.'
        , @c_col_desc_begdt   NVARCHAR(4000) = N'UTC begin date/time (inclusive).'
        , @c_col_desc_enddt   NVARCHAR(4000) = N'UTC end date/time (inclusive).'
        , @c_col_desc_run     NVARCHAR(4000) = N'File run number (increments by 10); part of natural key with INTF_ID.'
        , @c_col_desc_fname   NVARCHAR(4000) = N'Logical filename token for the backsweep window.'
        ;

    DECLARE @cols TABLE (name SYSNAME, descn NVARCHAR(4000));
    INSERT INTO @cols(name, descn) VALUES
        (N'PK_ID'           , @c_col_desc_pkid),
        (N'INTF_ID'         , @c_col_desc_intfid),
        (N'INTF_TYPE'       , @c_col_desc_intftp),
        (N'CREATE_DTM'      , @c_col_desc_cdtm),
        (N'PROCESS_STATUS'  , @c_col_desc_pstat),
        (N'APPLIED_DTM'     , @c_col_desc_apdtm),
        (N'BEGIN_DATE'      , @c_col_desc_begdt),
        (N'END_DATE'        , @c_col_desc_enddt),
        (N'RUN'             , @c_col_desc_run),
        (N'FILENAME'        , @c_col_desc_fname);

    DECLARE @col SYSNAME, @descn NVARCHAR(4000);
    DECLARE cur CURSOR LOCAL FAST_FORWARD FOR SELECT name, descn FROM @cols;
    OPEN cur;
    FETCH NEXT FROM cur INTO @col, @descn;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF EXISTS (
            SELECT 1 FROM sys.extended_properties
            WHERE class = 1 AND name = @c_ep_ms_description
              AND major_id = @v_object_id
              AND minor_id = COLUMNPROPERTY(@v_object_id, @col, 'ColumnId')
        )
            EXEC sys.sp_updateextendedproperty
                 @name=@c_ep_ms_description, @value=@descn,
                 @level0type=N'SCHEMA',@level0name=@c_schema_name,
                 @level1type=N'TABLE', @level1name=@c_table_name,
                 @level2type=N'COLUMN',@level2name=@col;
        ELSE
            EXEC sys.sp_addextendedproperty
                 @name=@c_ep_ms_description, @value=@descn,
                 @level0type=N'SCHEMA',@level0name=@c_schema_name,
                 @level1type=N'TABLE', @level1name=@c_table_name,
                 @level2type=N'COLUMN',@level2name=@col;

        FETCH NEXT FROM cur INTO @col, @descn;
    END
    CLOSE cur; DEALLOCATE cur;

    COMMIT TRAN;
END TRY
BEGIN CATCH
    IF XACT_STATE() <> 0 ROLLBACK TRAN;

    DECLARE
          @v_err_msg       NVARCHAR(4000) = ERROR_MESSAGE()
        , @v_err_msg_proc  NVARCHAR(128)  = ISNULL(ERROR_PROCEDURE(), N''), @v_err_msg_line  INT            = ERROR_LINE()
        , @v_err_msg_num   INT            = ERROR_NUMBER();

    DECLARE @msg NVARCHAR(2048) = FORMATMESSAGE(N'[CCDT_C310_CONFIG_BACKSWEEP] DDL failed. %s (PROC=%s, LINE=%d, ERR=%d)'
                                              , @v_err_msg, ISNULL(@v_err_msg_proc, N''), @v_err_msg_line, @v_err_msg_num);
    THROW 52031, @msg, 1;
END CATCH;
GO

/*------------------------------------------------------------------------------
Operator notes:

-- Natural/business key unique index (clustered if available) is (INTF_ID, RUN).
-- DML seed script updates/inserts by that key; keep this index in place.

-- Quick lookup:
SELECT INTF_ID, INTF_TYPE, PROCESS_STATUS, APPLIED_DTM, BEGIN_DATE, END_DATE, RUN, FILENAME
FROM   dbo.CCDT_C310_CONFIG_BACKSWEEP
ORDER BY INTF_ID, RUN;

------------------------------------------------------------------------------*/

