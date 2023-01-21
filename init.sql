-- 声明分区范围类型
create type public.partition_range as
(
    week        varchar,
    begin_year  varchar,
    begin_month varchar,
    begin_day   varchar,
    begin_at    timestamp with time zone,
    end_at      timestamp with time zone
);

-- 获取分区表唯一索引
create function public.partition_constraint_keys(schema character varying, "table" character varying) returns character varying
    immutable
    language plpgsql
as
$$
DECLARE
    unique_keys character varying;
BEGIN
    WITH t AS (SELECT DISTINCT kcu.column_name
               FROM information_schema.table_constraints AS tc
                        JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name
                        JOIN information_schema.constraint_column_usage AS ccu
                             ON ccu.constraint_name = tc.constraint_name
               WHERE constraint_type = 'UNIQUE'
                 AND tc.table_schema = schema
                 AND tc.table_name = "table")
    SELECT string_agg(concat(t.column_name, '=$1.', t.column_name), ' AND ')
    FROM t
    INTO unique_keys;
    RETURN unique_keys;
END
$$;

-- 创建范围分表表
create function public.range_partition_create(schema character varying, "table" character varying,
                                              suffix character varying,
                                              range_type partition_range) returns character varying
    language plpgsql
as
$$
DECLARE
    part_table_name varchar = format('%s_%s', "table", suffix);
BEGIN
    EXECUTE format('CREATE TABLE IF NOT EXISTS %s PARTITION OF %s FOR VALUES FROM (''%s'') TO (''%s'')',
                   concat_ws('.', schema, part_table_name), concat_ws('.', schema, "table"), range_type.begin_at,
                   range_type.end_at);

    RETURN part_table_name;
END
$$;

-- 判断分区表是否存在
create function public.partition_exists(schema character varying, "table" character varying,
                                        suffix character varying) returns character varying
    immutable
    language plpgsql
as
$$
DECLARE
    part_table_name_without_schema VARCHAR;
BEGIN
    WITH o AS (SELECT inhrelid FROM pg_inherits WHERE inhparent = concat_ws('.', schema, "table")::REGCLASS)
    SELECT relname
    FROM pg_class
    WHERE oid IN (SELECT * FROM o)
      AND relname = concat_ws('_', "table", suffix)
    INTO part_table_name_without_schema;
    RETURN part_table_name_without_schema;
END
$$;

-- 计算天分区范围
create function public.range_partition_day(ts timestamp with time zone) returns SETOF partition_range
    immutable
    strict
    language plpgsql
as
$$
DECLARE
    ts     TIMESTAMPTZ = date_trunc('DAY', ts);
    end_at TIMESTAMPTZ = ts + INTERVAL '1 DAY';
BEGIN
    RETURN QUERY SELECT extract(WEEK FROM ts)::varchar,
                        extract(YEAR FROM ts)::varchar,
                        lpad(extract(MONTH FROM ts)::varchar, 2, '0')::varchar,
                        lpad(extract(DAY FROM ts)::varchar, 2, '0')::varchar,
                        ts,
                        end_at;
END
$$;

-- 计算月分区范围
create function public.range_partition_month(ts timestamp with time zone) returns SETOF partition_range
    immutable
    strict
    language plpgsql
as
$$
DECLARE
    ts     TIMESTAMPTZ = date_trunc('MONTH', ts);
    end_at TIMESTAMPTZ = ts + INTERVAL '1 MONTH';
BEGIN
    RETURN QUERY SELECT extract(WEEK FROM ts)::varchar,
                        extract(YEAR FROM ts)::varchar,
                        lpad(extract(MONTH FROM ts)::varchar, 2, '0')::varchar,
                        lpad(extract(DAY FROM ts)::varchar, 2, '0')::varchar,
                        ts,
                        end_at;
END
$$;

-- 获取范围分区表后缀
create function public.range_partition_suffix(range_type character varying, partition_range partition_range) returns character varying
    language plpgsql
as
$$
DECLARE
    suffix character varying;
BEGIN


    IF range_type = 'day' THEN
        suffix = concat_ws('_', 'day', partition_range.begin_year, partition_range.begin_month,
                           partition_range.begin_day)::CHARACTER VARYING;
    ELSEIF range_type = 'month' THEN
        suffix = concat_ws('_', 'month', partition_range.begin_year, partition_range.begin_month)::CHARACTER VARYING;
    ELSEIF range_type = 'year' THEN
        suffix = concat_ws('_', 'year', partition_range.begin_year)::CHARACTER VARYING;
    ELSEIF range_type = 'week' THEN
        suffix = concat_ws('_', format('w%s', partition_range.week), partition_range.begin_year,
                           partition_range.begin_month, partition_range.begin_day)::CHARACTER VARYING;
    ELSE
        RAISE EXCEPTION 'unsupported range type(only in day, week, month, year)';
    END IF;

    RETURN suffix;
END
$$;

-- 计算周分区范围
create function public.range_partition_week(ts timestamp with time zone) returns SETOF partition_range
    immutable
    strict
    language plpgsql
as
$$
DECLARE
    ts     TIMESTAMPTZ = date_trunc('WEEK', ts);
    end_at TIMESTAMPTZ = ts + INTERVAL '1 WEEK';
BEGIN
    RETURN QUERY SELECT extract(WEEK FROM ts)::varchar,
                        extract(YEAR FROM ts)::varchar,
                        lpad(extract(MONTH FROM ts)::varchar, 2, '0')::varchar,
                        lpad(extract(DAY FROM ts)::varchar, 2, '0')::varchar,
                        ts,
                        end_at;
END
$$;

-- 计算年分区范围
create function public.range_partition_year(ts timestamp with time zone) returns SETOF partition_range
    immutable
    strict
    language plpgsql
as
$$
DECLARE
    ts     TIMESTAMPTZ = date_trunc('YEAR', ts);
    end_at TIMESTAMPTZ = ts + INTERVAL '1 YEAR';
BEGIN
    RETURN QUERY SELECT extract(WEEK FROM ts)::varchar,
                        extract(YEAR FROM ts)::varchar,
                        lpad(extract(MONTH FROM ts)::varchar, 2, '0')::varchar,
                        lpad(extract(DAY FROM ts)::varchar, 2, '0')::varchar,
                        ts,
                        end_at;
END
$$;


-- 声明自动分区表
create procedure public.range_partition_declare(IN schema character varying, IN "table" character varying,
                                                IN range_type character varying DEFAULT 'week'::character varying,
                                                IN range_col character varying DEFAULT 'ts'::character varying)
    language plpgsql
as
$$
DECLARE
    insert_func_name        TEXT             := format('%s.action_%s_range_insert', schema, "table");
    insert_action_statement TEXT;
    range_func_name         TEXT;
    schema_table            CHARACTER VARYING=concat_ws('.', schema, "table");
BEGIN
    IF range_type = 'day' THEN
        range_func_name = 'public.range_partition_day';
    ELSEIF range_type = 'month' THEN
        range_func_name = 'public.range_partition_month';
    ELSEIF range_type = 'year' THEN
        range_func_name = 'public.range_partition_year';
    ELSEIF range_type = 'week' THEN
        range_func_name = 'public.range_partition_week';
    ELSE
        RAISE EXCEPTION 'unsupported range type(only in day, week, month, year)';
    END IF;
    insert_action_statement := E'CREATE OR REPLACE FUNCTION ' || insert_func_name || '(table_row ' || schema_table ||
                               ') ' ||
                               'RETURNS VOID AS ' ||
                               '$body$ ' ||
                               'DECLARE ' ||
                               'schema CHARACTER VARYING = ''' || schema || ''';' ||
                               '"table" CHARACTER VARYING = ''' || "table" || '''; ' ||
                               'range_type CHARACTER VARYING = ''' || range_type || '''; ' ||
                               'pr     public.PARTITION_RANGE = ' || range_func_name || '(table_row.' || range_col ||
                               '); ' ||
                               'suffix CHARACTER VARYING = public.range_partition_suffix(range_type,pr); ' ||
                               'part_table_name CHARACTER VARYING = public.partition_exists(schema,"table", suffix); ' ||
                               'BEGIN ' ||
                               'IF part_table_name ISNULL THEN ' ||
                               'SELECT public.range_partition_create(schema, "table", suffix, pr) ' ||
                               'INTO part_table_name; ' ||
                               'END IF; ' ||
                               'EXECUTE format(''INSERT INTO %s SELECT $1.*'', concat_ws(''.'',schema, part_table_name)) USING table_row; '
                                   'END $body$ LANGUAGE plpgsql';
    EXECUTE insert_action_statement;
    EXECUTE format('DROP RULE IF EXISTS range_insert_action_rule ON %s', schema_table);
    EXECUTE format('CREATE RULE range_insert_action_rule AS ON INSERT TO %s DO INSTEAD SELECT %s(new)', schema_table,
                   insert_func_name);
END ;
$$;

