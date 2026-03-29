-- Задание 1. Скалярный подзапрос — фильтрация

SELECT
    o.last_name || ' ' || LEFT(o.first_name, 1) || '.' AS operator_name,
    SUM(fp.tons_mined) AS total_mined,
    (
        SELECT AVG(sub.total_tons)
        FROM (
            SELECT SUM(fp2.tons_mined) AS total_tons
            FROM fact_production fp2
            WHERE fp2.date_id BETWEEN 20240301 AND 20240331
            GROUP BY fp2.operator_id
        ) sub
    ) AS avg_production
FROM fact_production fp
JOIN dim_operator o
    ON fp.operator_id = o.operator_id
WHERE fp.date_id BETWEEN 20240301 AND 20240331
GROUP BY o.last_name, o.first_name
HAVING SUM(fp.tons_mined) > (
    SELECT AVG(sub.total_tons)
    FROM (
        SELECT SUM(fp2.tons_mined) AS total_tons
        FROM fact_production fp2
        WHERE fp2.date_id BETWEEN 20240301 AND 20240331
        GROUP BY fp2.operator_id
    ) sub
)
ORDER BY total_mined DESC;

-- Задание 2. Многозначный подзапрос с IN

SELECT
    s.sensor_code,
    st.type_name AS sensor_type,
    e.equipment_name,
    s.status
FROM dim_sensor s
JOIN dim_sensor_type st
    ON s.sensor_type_id = st.sensor_type_id
JOIN dim_equipment e
    ON s.equipment_id = e.equipment_id
WHERE s.equipment_id IN (
    SELECT DISTINCT fp.equipment_id
    FROM fact_production fp
    WHERE fp.date_id BETWEEN 20240101 AND 20240331
)
ORDER BY e.equipment_name, s.sensor_code;

-- Задание 3. NOT IN и NOT EXISTS

-- Вариант 1: NOT IN
SELECT
    e.equipment_name,
    et.type_name,
    m.mine_name,
    e.status
FROM dim_equipment e
JOIN dim_equipment_type et
    ON e.equipment_type_id = et.equipment_type_id
JOIN dim_mine m
    ON e.mine_id = m.mine_id
WHERE e.equipment_id NOT IN (
    SELECT fp.equipment_id
    FROM fact_production fp
    WHERE fp.equipment_id IS NOT NULL
)
ORDER BY e.equipment_name;

-- Вариант 2: NOT EXISTS
SELECT
    e.equipment_name,
    et.type_name,
    m.mine_name,
    e.status
FROM dim_equipment e
JOIN dim_equipment_type et
    ON e.equipment_type_id = et.equipment_type_id
JOIN dim_mine m
    ON e.mine_id = m.mine_id
WHERE NOT EXISTS (
    SELECT 1
    FROM fact_production fp
    WHERE fp.equipment_id = e.equipment_id
)
ORDER BY e.equipment_name;

-- Задание 4. Коррелированный подзапрос

SELECT
    m.mine_name,
    d.full_date,
    e.equipment_name,
    fp.tons_mined,
    (
        SELECT AVG(fp2.tons_mined)
        FROM fact_production fp2
        WHERE fp2.mine_id = fp.mine_id
          AND fp2.date_id BETWEEN 20240101 AND 20240331
    ) AS mine_avg,
    fp.tons_mined - (
        SELECT AVG(fp2.tons_mined)
        FROM fact_production fp2
        WHERE fp2.mine_id = fp.mine_id
          AND fp2.date_id BETWEEN 20240101 AND 20240331
    ) AS deviation
FROM fact_production fp
JOIN dim_mine m
    ON fp.mine_id = m.mine_id
JOIN dim_date d
    ON fp.date_id = d.date_id
JOIN dim_equipment e
    ON fp.equipment_id = e.equipment_id
WHERE fp.date_id BETWEEN 20240101 AND 20240331
  AND fp.tons_mined < (
      SELECT AVG(fp2.tons_mined)
      FROM fact_production fp2
      WHERE fp2.mine_id = fp.mine_id
        AND fp2.date_id BETWEEN 20240101 AND 20240331
  )
ORDER BY deviation ASC
LIMIT 20;

-- Задание 5. EXISTS — оборудование с тревожными показаниями

SELECT
    e.equipment_name,
    et.type_name,
    m.mine_name,
    (
        SELECT COUNT(*)
        FROM fact_equipment_telemetry ft
        WHERE ft.equipment_id = e.equipment_id
          AND ft.is_alarm = TRUE
          AND ft.date_id BETWEEN 20240301 AND 20240331
    ) AS alarm_count
FROM dim_equipment e
JOIN dim_equipment_type et
    ON e.equipment_type_id = et.equipment_type_id
JOIN dim_mine m
    ON e.mine_id = m.mine_id
WHERE EXISTS (
    SELECT 1
    FROM fact_equipment_telemetry ft
    WHERE ft.equipment_id = e.equipment_id
      AND ft.is_alarm = TRUE
      AND ft.date_id BETWEEN 20240301 AND 20240331
)
ORDER BY alarm_count DESC, e.equipment_name;

-- Задание 6. NOT EXISTS — поиск пробелов в данных

SELECT
    d.full_date,
    d.day_of_week_name,
    d.is_weekend
FROM dim_date d
WHERE d.date_id BETWEEN 20240301 AND 20240331
  AND NOT EXISTS (
      SELECT 1
      FROM fact_production fp
      WHERE fp.date_id = d.date_id
        AND fp.equipment_id = 5
  )
ORDER BY d.full_date;

-- Задание 7. Подзапрос с ANY / ALL

-- Вариант 1: > ALL
SELECT
    e.equipment_name,
    et.type_name,
    d.full_date,
    fp.shift_id,
    fp.tons_mined
FROM fact_production fp
JOIN dim_equipment e
    ON fp.equipment_id = e.equipment_id
JOIN dim_equipment_type et
    ON e.equipment_type_id = et.equipment_type_id
JOIN dim_date d
    ON fp.date_id = d.date_id
WHERE fp.tons_mined > ALL (
    SELECT fp2.tons_mined
    FROM fact_production fp2
    JOIN dim_equipment e2
        ON fp2.equipment_id = e2.equipment_id
    JOIN dim_equipment_type et2
        ON e2.equipment_type_id = et2.equipment_type_id
    WHERE et2.type_code = 'TRUCK'
)
ORDER BY fp.tons_mined DESC;

-- Вариант 2: через MAX
SELECT
    e.equipment_name,
    et.type_name,
    d.full_date,
    fp.shift_id,
    fp.tons_mined
FROM fact_production fp
JOIN dim_equipment e
    ON fp.equipment_id = e.equipment_id
JOIN dim_equipment_type et
    ON e.equipment_type_id = et.equipment_type_id
JOIN dim_date d
    ON fp.date_id = d.date_id
WHERE fp.tons_mined > (
    SELECT MAX(fp2.tons_mined)
    FROM fact_production fp2
    JOIN dim_equipment e2
        ON fp2.equipment_id = e2.equipment_id
    JOIN dim_equipment_type et2
        ON e2.equipment_type_id = et2.equipment_type_id
    WHERE et2.type_code = 'TRUCK'
)
ORDER BY fp.tons_mined DESC;

-- Вариант 3: > ANY
SELECT
    e.equipment_name,
    et.type_name,
    d.full_date,
    fp.shift_id,
    fp.tons_mined
FROM fact_production fp
JOIN dim_equipment e
    ON fp.equipment_id = e.equipment_id
JOIN dim_equipment_type et
    ON e.equipment_type_id = et.equipment_type_id
JOIN dim_date d
    ON fp.date_id = d.date_id
WHERE fp.tons_mined > ANY (
    SELECT fp2.tons_mined
    FROM fact_production fp2
    JOIN dim_equipment e2
        ON fp2.equipment_id = e2.equipment_id
    JOIN dim_equipment_type et2
        ON e2.equipment_type_id = et2.equipment_type_id
    WHERE et2.type_code = 'TRUCK'
)
ORDER BY fp.tons_mined DESC;

-- Задание 8. Последняя запись о добыче

SELECT
    e.equipment_name,
    et.type_name,
    d.full_date AS last_work_date,
    fp.tons_mined,
    o.last_name || ' ' || LEFT(o.first_name, 1) || '.' AS operator_name
FROM fact_production fp
JOIN dim_equipment e
    ON fp.equipment_id = e.equipment_id
JOIN dim_equipment_type et
    ON e.equipment_type_id = et.equipment_type_id
JOIN dim_date d
    ON fp.date_id = d.date_id
JOIN dim_operator o
    ON fp.operator_id = o.operator_id
WHERE fp.date_id = (
    SELECT MAX(fp2.date_id)
    FROM fact_production fp2
    WHERE fp2.equipment_id = fp.equipment_id
)
ORDER BY d.full_date ASC, e.equipment_name;

-- Задание 9. Комплексный запрос с вложенными подзапросами

SELECT
    m.mine_name,
    COUNT(DISTINCT e.equipment_id) AS top_equipment_count,
    ROUND(AVG(fd.duration_min), 2) AS avg_downtime_min,
    ROUND(SUM(fd.duration_min) / 60.0, 2) AS total_downtime_hours
FROM fact_equipment_downtime fd
JOIN dim_equipment e
    ON fd.equipment_id = e.equipment_id
JOIN dim_mine m
    ON e.mine_id = m.mine_id
WHERE fd.is_planned = FALSE
  AND fd.equipment_id IN (
      SELECT fp.equipment_id
      FROM fact_production fp
      WHERE fp.date_id BETWEEN 20240101 AND 20240331
      GROUP BY fp.equipment_id
      HAVING SUM(fp.tons_mined) > (
          SELECT AVG(sub.total_tons)
          FROM (
              SELECT SUM(fp2.tons_mined) AS total_tons
              FROM fact_production fp2
              WHERE fp2.date_id BETWEEN 20240101 AND 20240331
              GROUP BY fp2.equipment_id
          ) sub
      )
  )
GROUP BY m.mine_name
ORDER BY total_downtime_hours DESC;

-- Задание 10. OEE по оборудованию

SELECT
    e.equipment_name,
    et.type_name,

    ROUND(
        100.0 *
        (
            SELECT COALESCE(SUM(fp.operating_hours), 0)
            FROM fact_production fp
            WHERE fp.equipment_id = e.equipment_id
              AND fp.date_id BETWEEN 20240101 AND 20240331
        )
        /
        NULLIF(
            (
                SELECT COALESCE(SUM(fp.operating_hours), 0)
                FROM fact_production fp
                WHERE fp.equipment_id = e.equipment_id
                  AND fp.date_id BETWEEN 20240101 AND 20240331
            )
            +
            (
                SELECT COALESCE(SUM(fd.duration_min) / 60.0, 0)
                FROM fact_equipment_downtime fd
                WHERE fd.equipment_id = e.equipment_id
                  AND fd.date_id BETWEEN 20240101 AND 20240331
            ),
            0
        ),
        2
    ) AS availability_pct,

    ROUND(
        100.0 *
        (
            SELECT COALESCE(SUM(fp.tons_mined), 0)
            FROM fact_production fp
            WHERE fp.equipment_id = e.equipment_id
              AND fp.date_id BETWEEN 20240101 AND 20240331
        )
        /
        NULLIF(
            (
                SELECT COALESCE(SUM(fp.operating_hours), 0)
                FROM fact_production fp
                WHERE fp.equipment_id = e.equipment_id
                  AND fp.date_id BETWEEN 20240101 AND 20240331
            ) * et.max_payload_tons,
            0
        ),
        2
    ) AS performance_pct,

    ROUND(
        100.0 *
        (
            SELECT COUNT(*)
            FROM fact_ore_quality foq
            WHERE foq.equipment_id = e.equipment_id
              AND foq.date_id BETWEEN 20240101 AND 20240331
              AND foq.fe_content >= 55
        )
        /
        NULLIF(
            (
                SELECT COUNT(*)
                FROM fact_ore_quality foq
                WHERE foq.equipment_id = e.equipment_id
                  AND foq.date_id BETWEEN 20240101 AND 20240331
            ),
            0
        ),
        2
    ) AS quality_pct,

    ROUND(
        (
            (
                SELECT COALESCE(SUM(fp.operating_hours), 0)
                FROM fact_production fp
                WHERE fp.equipment_id = e.equipment_id
                  AND fp.date_id BETWEEN 20240101 AND 20240331
            )
            /
            NULLIF(
                (
                    SELECT COALESCE(SUM(fp.operating_hours), 0)
                    FROM fact_production fp
                    WHERE fp.equipment_id = e.equipment_id
                      AND fp.date_id BETWEEN 20240101 AND 20240331
                )
                +
                (
                    SELECT COALESCE(SUM(fd.duration_min) / 60.0, 0)
                    FROM fact_equipment_downtime fd
                    WHERE fd.equipment_id = e.equipment_id
                      AND fd.date_id BETWEEN 20240101 AND 20240331
                ),
                0
            )
        )
        *
        (
            (
                SELECT COALESCE(SUM(fp.tons_mined), 0)
                FROM fact_production fp
                WHERE fp.equipment_id = e.equipment_id
                  AND fp.date_id BETWEEN 20240101 AND 20240331
            )
            /
            NULLIF(
                (
                    SELECT COALESCE(SUM(fp.operating_hours), 0)
                    FROM fact_production fp
                    WHERE fp.equipment_id = e.equipment_id
                      AND fp.date_id BETWEEN 20240101 AND 20240331
                ) * et.max_payload_tons,
                0
            )
        )
        *
        (
            (
                SELECT COUNT(*)
                FROM fact_ore_quality foq
                WHERE foq.equipment_id = e.equipment_id
                  AND foq.date_id BETWEEN 20240101 AND 20240331
                  AND foq.fe_content >= 55
            )::NUMERIC
            /
            NULLIF(
                (
                    SELECT COUNT(*)
                    FROM fact_ore_quality foq
                    WHERE foq.equipment_id = e.equipment_id
                      AND foq.date_id BETWEEN 20240101 AND 20240331
                ),
                0
            )
        ) * 100,
        2
    ) AS oee_pct

FROM dim_equipment e
JOIN dim_equipment_type et
    ON e.equipment_type_id = et.equipment_type_id
WHERE e.status = 'active'
ORDER BY oee_pct DESC NULLS LAST;