-- ЗАДАНИЕ 1. Анализ селективности

-- Шаг 1: Обновление статистики
ANALYZE andreev.fact_production;

-- Шаг 2: Запрос к pg_stats
SELECT
    attname AS column_name,
    n_distinct,
    correlation,
    null_frac,
    most_common_vals::text
FROM pg_stats
WHERE tablename = 'fact_production'
  AND schemaname = 'andreev'
ORDER BY attname;

-- ЗАДАНИЕ 2. Коэффициент заполнения — fillfactor

CREATE INDEX idx_prod_date_ff100 ON andreev.fact_production(date_id) WITH (fillfactor = 100);
CREATE INDEX idx_prod_date_ff90  ON andreev.fact_production(date_id) WITH (fillfactor = 90);
CREATE INDEX idx_prod_date_ff70  ON andreev.fact_production(date_id) WITH (fillfactor = 70);
CREATE INDEX idx_prod_date_ff50  ON andreev.fact_production(date_id) WITH (fillfactor = 50);

SELECT
    indexname,
    pg_size_pretty(pg_relation_size(indexname::regclass)) AS index_size,
    pg_relation_size(indexname::regclass) AS size_bytes
FROM pg_indexes
WHERE indexname LIKE 'idx_prod_date_ff%'
ORDER BY size_bytes;

DROP INDEX IF EXISTS idx_prod_date_ff100;
DROP INDEX IF EXISTS idx_prod_date_ff90;
DROP INDEX IF EXISTS idx_prod_date_ff70;
DROP INDEX IF EXISTS idx_prod_date_ff50;

-- ЗАДАНИЕ 3. Управление статистикой

SELECT
    attname,
    attstattarget
FROM pg_attribute
WHERE attrelid = 'andreev.fact_production'::regclass
  AND attnum > 0
  AND NOT attisdropped
ORDER BY attnum;

EXPLAIN ANALYZE
SELECT *
FROM andreev.fact_production
WHERE mine_id = 1
  AND shaft_id = 1
  AND date_id BETWEEN 20240101 AND 20240131;

ALTER TABLE andreev.fact_production ALTER COLUMN mine_id  SET STATISTICS 1000;
ALTER TABLE andreev.fact_production ALTER COLUMN shaft_id SET STATISTICS 1000;
ALTER TABLE andreev.fact_production ALTER COLUMN date_id  SET STATISTICS 1000;
ANALYZE andreev.fact_production;

CREATE STATISTICS andreev.stat_prod_mine_shaft (dependencies, ndistinct)
    ON mine_id, shaft_id FROM andreev.fact_production;

ANALYZE andreev.fact_production;

EXPLAIN ANALYZE
SELECT *
FROM andreev.fact_production
WHERE mine_id = 1
  AND shaft_id = 1
  AND date_id BETWEEN 20240101 AND 20240131;

SELECT
    stxname,
    stxkeys,
    stxkind,
    stxdndistinct,
    stxddependencies
FROM pg_statistic_ext
JOIN pg_statistic_ext_data ON pg_statistic_ext.oid = pg_statistic_ext_data.stxoid
WHERE stxname = 'stat_prod_mine_shaft' AND stxnamespace = 'andreev'::regnamespace;

-- ЗАДАНИЕ 4. Дублирующиеся индексы (10 баллов)

CREATE INDEX idx_prod_equip_date_v1 ON andreev.fact_production(equipment_id, date_id);
CREATE INDEX idx_prod_equip_date_v2 ON andreev.fact_production(equipment_id, date_id);
CREATE INDEX idx_prod_equip_only   ON andreev.fact_production(equipment_id);

SELECT
    a.indexrelid::regclass AS index_1,
    b.indexrelid::regclass AS index_2,
    a.indrelid::regclass   AS table_name,
    pg_size_pretty(pg_relation_size(a.indexrelid)) AS index_size
FROM pg_index a
JOIN pg_index b
    ON a.indrelid    = b.indrelid
   AND a.indexrelid  < b.indexrelid
   AND a.indkey::text = b.indkey::text
WHERE a.indrelid::regclass::text LIKE 'andreev.%';

SELECT
    a.indexrelid::regclass AS shorter_index,
    b.indexrelid::regclass AS longer_index,
    a.indrelid::regclass   AS table_name,
    pg_size_pretty(pg_relation_size(a.indexrelid)) AS shorter_size,
    pg_size_pretty(pg_relation_size(b.indexrelid)) AS longer_size
FROM pg_index a
JOIN pg_index b
    ON a.indrelid    = b.indrelid
   AND a.indexrelid <> b.indexrelid
   AND a.indnkeyatts < b.indnkeyatts
   AND a.indkey::text = (
       SELECT string_agg(x, ' ')
       FROM unnest(string_to_array(b.indkey::text, ' ')) WITH ORDINALITY AS t(x, ord)
       WHERE ord <= a.indnkeyatts
   )
WHERE a.indrelid::regclass::text LIKE 'andreev.%';

SELECT
    pg_size_pretty(SUM(pg_relation_size(b.indexrelid))) AS wasted_space
FROM pg_index a
JOIN pg_index b
    ON a.indrelid    = b.indrelid
   AND a.indexrelid  < b.indexrelid
   AND a.indkey::text = b.indkey::text
WHERE a.indrelid::regclass::text LIKE 'andreev.%';

-- Шаг 5: Очистка
DROP INDEX IF EXISTS idx_prod_equip_date_v1;
DROP INDEX IF EXISTS idx_prod_equip_date_v2;
DROP INDEX IF EXISTS idx_prod_equip_only;

-- ЗАДАНИЕ 5. Мониторинг неиспользуемых индексов


SELECT
    schemaname || '.' || relname AS table_name,
    indexrelname AS index_name,
    idx_scan,
    idx_tup_read,
    pg_size_pretty(pg_relation_size(indexrelid)) AS index_size,
    pg_relation_size(indexrelid) AS size_bytes
FROM pg_stat_user_indexes
WHERE idx_scan = 0
  AND schemaname = 'andreev'
ORDER BY pg_relation_size(indexrelid) DESC;

SELECT
    pg_size_pretty(SUM(pg_relation_size(indexrelid))) AS total_wasted_space,
    COUNT(*) AS unused_index_count
FROM pg_stat_user_indexes
WHERE idx_scan = 0
  AND schemaname = 'andreev';

SELECT
    sui.relname AS table_name,
    sui.indexrelname AS index_name,
    sui.idx_scan,
    pg_size_pretty(pg_relation_size(sui.indexrelid)) AS index_size,
    i.indisunique,
    i.indisprimary
FROM pg_stat_user_indexes sui
JOIN pg_index i ON sui.indexrelid = i.indexrelid
WHERE sui.idx_scan = 0
  AND sui.schemaname = 'andreev'
  AND i.indisunique  = false
  AND i.indisprimary = false
ORDER BY pg_relation_size(sui.indexrelid) DESC;

SELECT stats_reset FROM pg_stat_bgwriter;

-- ЗАДАНИЕ 6. REINDEX и обслуживание

CREATE INDEX idx_prod_bloat_test ON andreev.fact_production(equipment_id, date_id);

SELECT pg_size_pretty(pg_relation_size('idx_prod_bloat_test')) AS initial_size;

UPDATE andreev.fact_production
SET equipment_id = equipment_id
WHERE date_id BETWEEN 20240101 AND 20240115;

UPDATE andreev.fact_production
SET equipment_id = equipment_id
WHERE date_id BETWEEN 20240116 AND 20240131;

SELECT pg_size_pretty(pg_relation_size('idx_prod_bloat_test')) AS bloated_size;

REINDEX INDEX idx_prod_bloat_test;

SELECT pg_size_pretty(pg_relation_size('idx_prod_bloat_test')) AS after_reindex_size;

UPDATE andreev.fact_production
SET equipment_id = equipment_id
WHERE date_id BETWEEN 20240101 AND 20240115;

REINDEX INDEX CONCURRENTLY idx_prod_bloat_test;

DROP INDEX IF EXISTS idx_prod_bloat_test;

-- ЗАДАНИЕ 7. Покрывающий индекс для отчёта

EXPLAIN (ANALYZE, BUFFERS)
SELECT date_id,
       SUM(tons_mined)      AS total_tons,
       SUM(trips_count)     AS total_trips,
       SUM(operating_hours) AS total_hours
FROM andreev.fact_production
WHERE equipment_id = 5
  AND date_id BETWEEN 20240101 AND 20240331
GROUP BY date_id
ORDER BY date_id;

CREATE INDEX idx_prod_equip_date_covering
    ON andreev.fact_production(equipment_id, date_id)
    INCLUDE (tons_mined, trips_count, operating_hours);

VACUUM andreev.fact_production;

EXPLAIN (ANALYZE, BUFFERS)
SELECT date_id,
       SUM(tons_mined)      AS total_tons,
       SUM(trips_count)     AS total_trips,
       SUM(operating_hours) AS total_hours
FROM andreev.fact_production
WHERE equipment_id = 5
  AND date_id BETWEEN 20240101 AND 20240331
GROUP BY date_id
ORDER BY date_id;

DROP INDEX IF EXISTS idx_prod_equip_date_covering;

-- ЗАДАНИЕ 8. Комплексная оптимизация отчёта OEE

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
WITH production_data AS (
    SELECT
        p.equipment_id,
        SUM(p.operating_hours) AS total_operating_hours,
        SUM(p.tons_mined)      AS total_tons
    FROM andreev.fact_production p
    WHERE p.date_id BETWEEN 20240301 AND 20240331
    GROUP BY p.equipment_id
),
downtime_data AS (
    SELECT
        fd.equipment_id,
        SUM(fd.duration_min) / 60.0 AS total_downtime_hours,
        SUM(CASE WHEN fd.is_planned = FALSE THEN fd.duration_min ELSE 0 END) / 60.0 AS unplanned_hours
    FROM andreev.fact_equipment_downtime fd
    WHERE fd.date_id BETWEEN 20240301 AND 20240331
    GROUP BY fd.equipment_id
)
SELECT
    e.equipment_name,
    et.type_name,
    COALESCE(pd.total_operating_hours, 0) AS operating_hours,
    COALESCE(dd.total_downtime_hours, 0)  AS downtime_hours,
    COALESCE(dd.unplanned_hours, 0)       AS unplanned_downtime,
    COALESCE(pd.total_tons, 0)            AS tons_mined,
    CASE
        WHEN COALESCE(pd.total_operating_hours, 0) + COALESCE(dd.total_downtime_hours, 0) > 0
        THEN ROUND(
            COALESCE(pd.total_operating_hours, 0) /
            (COALESCE(pd.total_operating_hours, 0) + COALESCE(dd.total_downtime_hours, 0)) * 100, 1
        )
        ELSE 0
    END AS availability_pct
FROM andreev.dim_equipment e
JOIN andreev.dim_equipment_type et ON et.equipment_type_id = e.equipment_type_id
LEFT JOIN production_data pd ON pd.equipment_id = e.equipment_id
LEFT JOIN downtime_data dd   ON dd.equipment_id  = e.equipment_id
WHERE e.status = 'active'
ORDER BY availability_pct ASC;

CREATE INDEX idx_oee_prod ON andreev.fact_production(date_id)
    INCLUDE (equipment_id, operating_hours, tons_mined);


CREATE INDEX idx_oee_downtime ON andreev.fact_equipment_downtime(date_id)
    INCLUDE (equipment_id, duration_min, is_planned);

CREATE INDEX idx_equip_status ON andreev.dim_equipment(status)
    INCLUDE (equipment_id, equipment_name, equipment_type_id);

VACUUM andreev.fact_production;
VACUUM andreev.fact_equipment_downtime;
VACUUM andreev.dim_equipment;

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
WITH production_data AS (
    SELECT
        p.equipment_id,
        SUM(p.operating_hours) AS total_operating_hours,
        SUM(p.tons_mined)      AS total_tons
    FROM andreev.fact_production p
    WHERE p.date_id BETWEEN 20240301 AND 20240331
    GROUP BY p.equipment_id
),
downtime_data AS (
    SELECT
        fd.equipment_id,
        SUM(fd.duration_min) / 60.0 AS total_downtime_hours,
        SUM(CASE WHEN fd.is_planned = FALSE THEN fd.duration_min ELSE 0 END) / 60.0 AS unplanned_hours
    FROM andreev.fact_equipment_downtime fd
    WHERE fd.date_id BETWEEN 20240301 AND 20240331
    GROUP BY fd.equipment_id
)
SELECT
    e.equipment_name,
    et.type_name,
    COALESCE(pd.total_operating_hours, 0) AS operating_hours,
    COALESCE(dd.total_downtime_hours, 0)  AS downtime_hours,
    COALESCE(dd.unplanned_hours, 0)       AS unplanned_downtime,
    COALESCE(pd.total_tons, 0)            AS tons_mined,
    CASE
        WHEN COALESCE(pd.total_operating_hours, 0) + COALESCE(dd.total_downtime_hours, 0) > 0
        THEN ROUND(
            COALESCE(pd.total_operating_hours, 0) /
            (COALESCE(pd.total_operating_hours, 0) + COALESCE(dd.total_downtime_hours, 0)) * 100, 1
        )
        ELSE 0
    END AS availability_pct
FROM andreev.dim_equipment e
JOIN andreev.dim_equipment_type et ON et.equipment_type_id = e.equipment_type_id
LEFT JOIN production_data pd ON pd.equipment_id = e.equipment_id
LEFT JOIN downtime_data dd   ON dd.equipment_id  = e.equipment_id
WHERE e.status = 'active'
ORDER BY availability_pct ASC;

DROP INDEX IF EXISTS idx_oee_prod;
DROP INDEX IF EXISTS idx_oee_downtime;
DROP INDEX IF EXISTS idx_equip_status;

-- ЗАДАНИЕ 9. Оптимизация пакета запросов

-- ── ДО оптимизации ──────────────────────────────────────────

-- Q1: Добыча за день по конкретной шахте
EXPLAIN (ANALYZE, BUFFERS)
SELECT p.date_id, SUM(p.tons_mined) AS daily_tons
FROM andreev.fact_production p
WHERE p.mine_id = 1
  AND p.date_id BETWEEN 20240301 AND 20240331
GROUP BY p.date_id
ORDER BY p.date_id;

-- Q2: Все простои конкретного оборудования за месяц
EXPLAIN (ANALYZE, BUFFERS)
SELECT fd.date_id, fd.start_time, fd.duration_min, dr.reason_name
FROM andreev.fact_equipment_downtime fd
JOIN andreev.dim_downtime_reason dr ON dr.reason_id = fd.reason_id
WHERE fd.equipment_id = 3
  AND fd.date_id BETWEEN 20240301 AND 20240331
ORDER BY fd.date_id, fd.start_time;

-- Q3: Тревожные показания датчиков за день
EXPLAIN (ANALYZE, BUFFERS)
SELECT t.time_id, s.sensor_code, t.sensor_value
FROM andreev.fact_equipment_telemetry t
JOIN andreev.dim_sensor s ON s.sensor_id = t.sensor_id
WHERE t.date_id = 20240315
  AND t.is_alarm = TRUE
ORDER BY t.time_id;

-- Q4: Среднее качество руды за месяц по шахте
EXPLAIN (ANALYZE, BUFFERS)
SELECT oq.date_id, AVG(oq.fe_content) AS avg_fe, AVG(oq.moisture_pct) AS avg_moisture
FROM andreev.fact_ore_quality oq
WHERE oq.mine_id = 2
  AND oq.date_id BETWEEN 20240301 AND 20240331
GROUP BY oq.date_id
ORDER BY oq.date_id;

-- Q5: Топ-10 самых длительных незапланированных простоев
EXPLAIN (ANALYZE, BUFFERS)
SELECT fd.date_id, e.equipment_name, dr.reason_name, fd.duration_min
FROM andreev.fact_equipment_downtime fd
JOIN andreev.dim_equipment e         ON e.equipment_id  = fd.equipment_id
JOIN andreev.dim_downtime_reason dr  ON dr.reason_id    = fd.reason_id
WHERE fd.is_planned = FALSE
  AND fd.date_id BETWEEN 20240301 AND 20240331
ORDER BY fd.duration_min DESC
LIMIT 10;

-- ── Создание индексов

-- Индекс 1
CREATE INDEX idx_q1_prod_mine_date
    ON andreev.fact_production(mine_id, date_id)
    INCLUDE (tons_mined);

-- Индекс 2
CREATE INDEX idx_q2_downtime_equip
    ON andreev.fact_equipment_downtime(equipment_id, date_id)
    INCLUDE (start_time, duration_min, reason_id);

-- Индекс 3
CREATE INDEX idx_q5_downtime_unplanned
    ON andreev.fact_equipment_downtime(date_id, duration_min DESC)
    WHERE is_planned = FALSE;

-- Индекс 4
CREATE INDEX idx_q3_telemetry_alarm
    ON andreev.fact_equipment_telemetry(date_id, time_id)
    INCLUDE (sensor_id, sensor_value)
    WHERE is_alarm = TRUE;

-- Индекс 5
CREATE INDEX idx_q4_ore_mine_date
    ON andreev.fact_ore_quality(mine_id, date_id)
    INCLUDE (fe_content, moisture_pct);

VACUUM andreev.fact_production;
VACUUM andreev.fact_equipment_downtime;
VACUUM andreev.fact_equipment_telemetry;
VACUUM andreev.fact_ore_quality;


EXPLAIN (ANALYZE, BUFFERS)
SELECT p.date_id, SUM(p.tons_mined) AS daily_tons
FROM andreev.fact_production p
WHERE p.mine_id = 1
  AND p.date_id BETWEEN 20240301 AND 20240331
GROUP BY p.date_id
ORDER BY p.date_id;

EXPLAIN (ANALYZE, BUFFERS)
SELECT fd.date_id, fd.start_time, fd.duration_min, dr.reason_name
FROM andreev.fact_equipment_downtime fd
JOIN andreev.dim_downtime_reason dr ON dr.reason_id = fd.reason_id
WHERE fd.equipment_id = 3
  AND fd.date_id BETWEEN 20240301 AND 20240331
ORDER BY fd.date_id, fd.start_time;

EXPLAIN (ANALYZE, BUFFERS)
SELECT t.time_id, s.sensor_code, t.sensor_value
FROM andreev.fact_equipment_telemetry t
JOIN andreev.dim_sensor s ON s.sensor_id = t.sensor_id
WHERE t.date_id = 20240315
  AND t.is_alarm = TRUE
ORDER BY t.time_id;

EXPLAIN (ANALYZE, BUFFERS)
SELECT oq.date_id, AVG(oq.fe_content) AS avg_fe, AVG(oq.moisture_pct) AS avg_moisture
FROM andreev.fact_ore_quality oq
WHERE oq.mine_id = 2
  AND oq.date_id BETWEEN 20240301 AND 20240331
GROUP BY oq.date_id
ORDER BY oq.date_id;

EXPLAIN (ANALYZE, BUFFERS)
SELECT fd.date_id, e.equipment_name, dr.reason_name, fd.duration_min
FROM andreev.fact_equipment_downtime fd
JOIN andreev.dim_equipment e         ON e.equipment_id = fd.equipment_id
JOIN andreev.dim_downtime_reason dr  ON dr.reason_id   = fd.reason_id
WHERE fd.is_planned = FALSE
  AND fd.date_id BETWEEN 20240301 AND 20240331
ORDER BY fd.duration_min DESC
LIMIT 10;

DROP INDEX IF EXISTS idx_q1_prod_mine_date;
DROP INDEX IF EXISTS idx_q2_downtime_equip;
DROP INDEX IF EXISTS idx_q5_downtime_unplanned;
DROP INDEX IF EXISTS idx_q3_telemetry_alarm;
DROP INDEX IF EXISTS idx_q4_ore_mine_date;

-- ЗАДАНИЕ 10. Стратегический анализ

SELECT
    relname AS table_name,
    pg_size_pretty(pg_relation_size(relid)) AS table_size,
    pg_size_pretty(pg_total_relation_size(relid) - pg_relation_size(relid)) AS current_indexes_size,
    ROUND(
        (pg_total_relation_size(relid) - pg_relation_size(relid))::numeric /
        NULLIF(pg_relation_size(relid), 0) * 100, 1
    ) AS index_to_table_pct
FROM pg_catalog.pg_statio_user_tables
WHERE schemaname = 'andreev'
  AND relname LIKE 'fact_%'
ORDER BY pg_relation_size(relid) DESC;
