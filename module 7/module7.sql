-- Задание 1. Анализ существующих индексов

-- 1. Все индексы по выбранным факт-таблицам --
SELECT
    tablename,
    indexname,
    indexdef
FROM pg_indexes
WHERE tablename IN (
    'fact_production',
    'fact_equipment_telemetry',
    'fact_equipment_downtime',
    'fact_ore_quality'
)
ORDER BY tablename, indexname;

-- 2. Размер и статистика использования индексов fact_production
SELECT
    indexrelname AS index_name,
    pg_size_pretty(pg_relation_size(indexrelid)) AS index_size,
    idx_scan AS times_used,
    idx_tup_read AS tuples_read,
    idx_tup_fetch AS tuples_fetched
FROM pg_stat_user_indexes
WHERE relname = 'fact_production'
ORDER BY pg_relation_size(indexrelid) DESC;

-- 3. Суммарный размер индексов и таблиц
SELECT
    relname AS table_name,
    pg_size_pretty(pg_table_size(relid)) AS table_size,
    pg_size_pretty(pg_indexes_size(relid)) AS indexes_size,
    pg_size_pretty(pg_total_relation_size(relid)) AS total_size,
    ROUND(
        pg_indexes_size(relid)::numeric /
        NULLIF(pg_table_size(relid), 0) * 100,
        1
    ) AS index_pct
FROM pg_stat_user_tables
WHERE relname IN (
    'fact_production',
    'fact_equipment_telemetry',
    'fact_equipment_downtime',
    'fact_ore_quality'
)
ORDER BY pg_total_relation_size(relid) DESC;

-- Задание 2. Анализ плана выполнения --

-- 1. Оценочный план
EXPLAIN
SELECT
    e.equipment_name,
    SUM(p.tons_mined) AS total_tons,
    SUM(p.fuel_consumed_l) AS total_fuel,
    SUM(p.operating_hours) AS total_hours
FROM fact_production p
JOIN dim_equipment e
    ON p.equipment_id = e.equipment_id
WHERE p.date_id BETWEEN 20240301 AND 20240331
GROUP BY e.equipment_name
ORDER BY total_tons DESC;

-- 2. Реальный план
EXPLAIN ANALYZE
SELECT
    e.equipment_name,
    SUM(p.tons_mined) AS total_tons,
    SUM(p.fuel_consumed_l) AS total_fuel,
    SUM(p.operating_hours) AS total_hours
FROM fact_production p
JOIN dim_equipment e
    ON p.equipment_id = e.equipment_id
WHERE p.date_id BETWEEN 20240301 AND 20240331
GROUP BY e.equipment_name
ORDER BY total_tons DESC;

-- 3. План с буферами
EXPLAIN (ANALYZE, BUFFERS)
SELECT
    e.equipment_name,
    SUM(p.tons_mined) AS total_tons,
    SUM(p.fuel_consumed_l) AS total_fuel,
    SUM(p.operating_hours) AS total_hours
FROM fact_production p
JOIN dim_equipment e
    ON p.equipment_id = e.equipment_id
WHERE p.date_id BETWEEN 20240301 AND 20240331
GROUP BY e.equipment_name
ORDER BY total_tons DESC;

-- Задание 3. Оптимизация поиска по расходу топлива --

-- 1. План до индекса
EXPLAIN ANALYZE
SELECT
    p.date_id,
    e.equipment_name,
    o.last_name,
    p.fuel_consumed_l
FROM fact_production p
JOIN dim_equipment e
    ON p.equipment_id = e.equipment_id
JOIN dim_operator o
    ON p.operator_id = o.operator_id
WHERE p.fuel_consumed_l > 80
ORDER BY p.fuel_consumed_l DESC;

-- 2. Избирательность условия
SELECT
    COUNT(*) AS total_rows,
    COUNT(*) FILTER (WHERE fuel_consumed_l > 80) AS matching_rows,
    ROUND(
        COUNT(*) FILTER (WHERE fuel_consumed_l > 80)::numeric /
        COUNT(*) * 100,
        2
    ) AS selectivity_pct
FROM fact_production;

-- 3. Индекс
CREATE INDEX idx_prod_fuel
ON fact_production(fuel_consumed_l);

-- 4. План после индекса 
EXPLAIN ANALYZE
SELECT
    p.date_id,
    e.equipment_name,
    o.last_name,
    p.fuel_consumed_l
FROM fact_production p
JOIN dim_equipment e
    ON p.equipment_id = e.equipment_id
JOIN dim_operator o
    ON p.operator_id = o.operator_id
WHERE p.fuel_consumed_l > 80
ORDER BY p.fuel_consumed_l DESC;

-- Задание 4. Частичный индекс для аварийной телеметрии --

EXPLAIN ANALYZE
SELECT
    t.telemetry_id,
    t.date_id,
    t.equipment_id,
    t.sensor_id,
    t.sensor_value
FROM fact_equipment_telemetry t
WHERE t.date_id = 20240315
  AND t.is_alarm = TRUE;

-- 2. Частичный индекс
CREATE INDEX idx_telemetry_alarm_partial
ON fact_equipment_telemetry(date_id)
WHERE is_alarm = TRUE;

-- 3. Полный индекс для сравнения
CREATE INDEX idx_telemetry_alarm_full
ON fact_equipment_telemetry(date_id, is_alarm);

-- 4. Сравнение размеров
SELECT
    indexrelname,
    pg_size_pretty(pg_relation_size(indexrelid)) AS size
FROM pg_stat_user_indexes
WHERE indexrelname IN ('idx_telemetry_alarm_partial', 'idx_telemetry_alarm_full')
ORDER BY pg_relation_size(indexrelid);

-- 5. План после индексов
EXPLAIN ANALYZE
SELECT
    t.telemetry_id,
    t.date_id,
    t.equipment_id,
    t.sensor_id,
    t.sensor_value
FROM fact_equipment_telemetry t
WHERE t.date_id = 20240315
  AND t.is_alarm = TRUE;

-- Задание 5. Композитный индекс для отчета по добыче --

-- 1. План до индексов
EXPLAIN ANALYZE
SELECT
    date_id,
    tons_mined,
    tons_transported,
    trips_count,
    operating_hours
FROM fact_production
WHERE equipment_id = 5
  AND date_id BETWEEN 20240301 AND 20240331;

-- 2. Индекс (equipment_id, date_id)
CREATE INDEX idx_prod_equip_date
ON fact_production(equipment_id, date_id);

-- 3. Индекс (date_id, equipment_id)
CREATE INDEX idx_prod_date_equip
ON fact_production(date_id, equipment_id);

-- 4. План после создания обоих индексов
EXPLAIN ANALYZE
SELECT
    date_id,
    tons_mined,
    tons_transported,
    trips_count,
    operating_hours
FROM fact_production
WHERE equipment_id = 5
  AND date_id BETWEEN 20240301 AND 20240331;

-- 5. Проверка запроса только по date_id
EXPLAIN ANALYZE
SELECT *
FROM fact_production
WHERE date_id = 20240315;

-- 6. Удаление менее эффективного индекса
DROP INDEX IF EXISTS idx_prod_date_equip;

-- Задание 6. Индекс по выражению для поиска операторов --

-- 1. План до индекса
EXPLAIN ANALYZE
SELECT
    operator_id,
    last_name,
    first_name,
    middle_name,
    position,
    qualification
FROM dim_operator
WHERE LOWER(last_name) = 'петров';

-- 2. Индекс по выражению
CREATE INDEX idx_operator_lower_lastname
ON dim_operator (LOWER(last_name));

-- 3. План после индекса
EXPLAIN ANALYZE
SELECT
    operator_id,
    last_name,
    first_name,
    middle_name,
    position,
    qualification
FROM dim_operator
WHERE LOWER(last_name) = 'петров';

-- 4. Проверка без LOWER
EXPLAIN ANALYZE
SELECT
    operator_id,
    last_name,
    first_name
FROM dim_operator
WHERE last_name = 'Петров';

-- 5. Проверка с UPPER
EXPLAIN ANALYZE
SELECT
    operator_id,
    last_name,
    first_name
FROM dim_operator
WHERE UPPER(last_name) = 'ПЕТРОВ';

-- Задание 7. Покрывающий индекс для дашборда --

-- 1. План до индекса
EXPLAIN ANALYZE
SELECT
    date_id,
    equipment_id,
    tons_mined
FROM fact_production
WHERE date_id = 20240315;

-- 2. Покрывающий индекс
CREATE INDEX idx_prod_date_cover
ON fact_production(date_id)
INCLUDE (equipment_id, tons_mined);

-- 3. VACUUM
VACUUM fact_production;

-- 4. План после индекса
EXPLAIN ANALYZE
SELECT
    date_id,
    equipment_id,
    tons_mined
FROM fact_production
WHERE date_id = 20240315;

-- 5. Добавляем fuel_consumed_l
EXPLAIN ANALYZE
SELECT
    date_id,
    equipment_id,
    tons_mined,
    fuel_consumed_l
FROM fact_production
WHERE date_id = 20240315;

-- 6. Расширенный покрывающий индекс
CREATE INDEX idx_prod_date_cover_ext
ON fact_production(date_id)
INCLUDE (equipment_id, tons_mined, fuel_consumed_l);

VACUUM fact_production;

EXPLAIN ANALYZE
SELECT
    date_id,
    equipment_id,
    tons_mined,
    fuel_consumed_l
FROM fact_production
WHERE date_id = 20240315;

-- Задание 8. BRIN-индекс для телеметрии --

-- 1. Размер существующего B-tree индекса
SELECT
    indexrelname,
    pg_size_pretty(pg_relation_size(indexrelid)) AS size
FROM pg_stat_user_indexes
WHERE indexrelname = 'idx_fact_telemetry_date';

-- 2. BRIN-индекс
CREATE INDEX idx_telemetry_date_brin
ON fact_equipment_telemetry USING brin (date_id)
WITH (pages_per_range = 128);

-- 3. Сравнение размеров
SELECT
    indexrelname,
    pg_size_pretty(pg_relation_size(indexrelid)) AS size
FROM pg_stat_user_indexes
WHERE indexrelname IN ('idx_fact_telemetry_date', 'idx_telemetry_date_brin')
ORDER BY pg_relation_size(indexrelid) DESC;

-- 4. Проверка корреляции
SELECT
    attname,
    correlation
FROM pg_stats
WHERE tablename = 'fact_equipment_telemetry'
  AND attname = 'date_id';

-- 5. Тест B-tree
SET enable_bitmapscan = off;

EXPLAIN (ANALYZE, BUFFERS)
SELECT *
FROM fact_equipment_telemetry
WHERE date_id BETWEEN 20240301 AND 20240331;

RESET enable_bitmapscan;

-- 6. Тест BRIN
SET enable_indexscan = off;

EXPLAIN (ANALYZE, BUFFERS)
SELECT *
FROM fact_equipment_telemetry
WHERE date_id BETWEEN 20240301 AND 20240331;

RESET enable_indexscan;

-- Задание 9. Анализ влияния индексов на INSERT --

-- 1. Текущее количество индексов
SELECT COUNT(*) AS index_count
FROM pg_indexes
WHERE tablename = 'fact_production';

-- 2. INSERT с текущими индексами
EXPLAIN ANALYZE
INSERT INTO fact_production
    (date_id, shift_id, mine_id, shaft_id, equipment_id,
     operator_id, location_id, ore_grade_id,
     tons_mined, tons_transported, trips_count,
     distance_km, fuel_consumed_l, operating_hours)
VALUES
    (20240401, 1, 1, 1, 1, 1, 1, 1,
     120.50, 115.00, 8, 12.5, 45.2, 7.5);

-- 3. Дополнительные индексы
CREATE INDEX idx_test_1 ON fact_production(tons_mined);
CREATE INDEX idx_test_2 ON fact_production(fuel_consumed_l, operating_hours);
CREATE INDEX idx_test_3 ON fact_production(date_id, shift_id, mine_id);

-- 4. Новое количество индексов
SELECT COUNT(*) AS index_count
FROM pg_indexes
WHERE tablename = 'fact_production';

-- 5. INSERT после добавления индексов
EXPLAIN ANALYZE
INSERT INTO fact_production
    (date_id, shift_id, mine_id, shaft_id, equipment_id,
     operator_id, location_id, ore_grade_id,
     tons_mined, tons_transported, trips_count,
     distance_km, fuel_consumed_l, operating_hours)
VALUES
    (20240401, 1, 1, 1, 1, 1, 1, 1,
     130.00, 125.00, 9, 14.0, 50.1, 8.0);

-- Задание 10. Комплексная оптимизация --

-- ---------- Планы ДО ----------

EXPLAIN ANALYZE
SELECT
    m.mine_name,
    SUM(p.tons_mined) AS total_tons,
    SUM(p.operating_hours) AS total_hours
FROM fact_production p
JOIN dim_mine m
    ON p.mine_id = m.mine_id
WHERE p.date_id BETWEEN 20240301 AND 20240331
GROUP BY m.mine_name;

EXPLAIN ANALYZE
SELECT
    g.grade_name,
    AVG(q.fe_content) AS avg_fe,
    AVG(q.sio2_content) AS avg_sio2,
    COUNT(*) AS samples
FROM fact_ore_quality q
JOIN dim_ore_grade g
    ON q.ore_grade_id = g.ore_grade_id
WHERE q.date_id BETWEEN 20240101 AND 20240331
GROUP BY g.grade_name;

EXPLAIN ANALYZE
SELECT
    e.equipment_name,
    SUM(dt.duration_min) AS total_downtime_min,
    COUNT(*) AS incidents
FROM fact_equipment_downtime dt
JOIN dim_equipment e
    ON dt.equipment_id = e.equipment_id
WHERE dt.is_planned = FALSE
  AND dt.date_id BETWEEN 20240301 AND 20240331
GROUP BY e.equipment_name
ORDER BY total_downtime_min DESC
LIMIT 5;

EXPLAIN ANALYZE
SELECT
    t.date_id,
    t.time_id,
    t.sensor_id,
    t.sensor_value,
    t.quality_flag
FROM fact_equipment_telemetry t
WHERE t.equipment_id = 5
  AND t.is_alarm = TRUE
ORDER BY t.date_id DESC, t.time_id DESC
LIMIT 20;

EXPLAIN ANALYZE
SELECT
    p.date_id,
    e.equipment_name,
    p.tons_mined,
    p.trips_count,
    p.operating_hours
FROM fact_production p
JOIN dim_equipment e
    ON p.equipment_id = e.equipment_id
WHERE p.operator_id = 3
  AND p.date_id BETWEEN 20240311 AND 20240317
ORDER BY p.date_id;

-- ---------- Предлагаемые индексы ----------

-- Запрос 1
CREATE INDEX idx_prod_date_mine
ON fact_production(date_id, mine_id);

-- Запрос 2
CREATE INDEX idx_quality_date
ON fact_ore_quality(date_id);

-- Запрос 3
CREATE INDEX idx_downtime_unplanned
ON fact_equipment_downtime(date_id, equipment_id)
WHERE is_planned = FALSE;

-- Запрос 4
CREATE INDEX idx_telemetry_equip_alarm
ON fact_equipment_telemetry(equipment_id, date_id DESC, time_id DESC)
WHERE is_alarm = TRUE;

-- Запрос 5
CREATE INDEX idx_prod_operator_date
ON fact_production(operator_id, date_id);

-- ---------- Планы ПОСЛЕ ----------

EXPLAIN ANALYZE
SELECT
    m.mine_name,
    SUM(p.tons_mined) AS total_tons,
    SUM(p.operating_hours) AS total_hours
FROM fact_production p
JOIN dim_mine m
    ON p.mine_id = m.mine_id
WHERE p.date_id BETWEEN 20240301 AND 20240331
GROUP BY m.mine_name;

EXPLAIN ANALYZE
SELECT
    g.grade_name,
    AVG(q.fe_content) AS avg_fe,
    AVG(q.sio2_content) AS avg_sio2,
    COUNT(*) AS samples
FROM fact_ore_quality q
JOIN dim_ore_grade g
    ON q.ore_grade_id = g.ore_grade_id
WHERE q.date_id BETWEEN 20240101 AND 20240331
GROUP BY g.grade_name;

EXPLAIN ANALYZE
SELECT
    e.equipment_name,
    SUM(dt.duration_min) AS total_downtime_min,
    COUNT(*) AS incidents
FROM fact_equipment_downtime dt
JOIN dim_equipment e
    ON dt.equipment_id = e.equipment_id
WHERE dt.is_planned = FALSE
  AND dt.date_id BETWEEN 20240301 AND 20240331
GROUP BY e.equipment_name
ORDER BY total_downtime_min DESC
LIMIT 5;

EXPLAIN ANALYZE
SELECT
    t.date_id,
    t.time_id,
    t.sensor_id,
    t.sensor_value,
    t.quality_flag
FROM fact_equipment_telemetry t
WHERE t.equipment_id = 5
  AND t.is_alarm = TRUE
ORDER BY t.date_id DESC, t.time_id DESC
LIMIT 20;

EXPLAIN ANALYZE
SELECT
    p.date_id,
    e.equipment_name,
    p.tons_mined,
    p.trips_count,
    p.operating_hours
FROM fact_production p
JOIN dim_equipment e
    ON p.equipment_id = e.equipment_id
WHERE p.operator_id = 3
  AND p.date_id BETWEEN 20240311 AND 20240317
ORDER BY p.date_id;
