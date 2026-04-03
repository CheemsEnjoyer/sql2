-- ЗАДАНИЕ 1. ROLLUP — сменный рапорт с подитогами

SELECT
    CASE
        WHEN GROUPING(m.mine_name) = 1 THEN '== ИТОГО =='
        ELSE m.mine_name
    END AS mine_name,

    CASE
        WHEN GROUPING(m.mine_name) = 1 THEN '—'
        WHEN GROUPING(s.shift_name) = 1 THEN 'Итого по шахте'
        ELSE s.shift_name
    END AS shift_name,

    SUM(fp.tons_mined)                AS total_tons,
    COUNT(DISTINCT fp.equipment_id)   AS equipment_count

FROM andreev.fact_production fp
JOIN andreev.dim_mine  m ON m.mine_id  = fp.mine_id
JOIN andreev.dim_shift s ON s.shift_id = fp.shift_id
JOIN andreev.dim_date  d ON d.date_id  = fp.date_id

WHERE d.date_id = 20240115

GROUP BY ROLLUP(m.mine_name, s.shift_name)

ORDER BY
    GROUPING(m.mine_name),
    m.mine_name,
    GROUPING(s.shift_name),
    s.shift_name;

-- ЗАДАНИЕ 2. CUBE — матрица «шахта x тип оборудования»

SELECT
    CASE
        WHEN GROUPING(m.mine_name) = 1 THEN 'ВСЕ ШАХТЫ'
        ELSE m.mine_name
    END AS mine_name,

    CASE
        WHEN GROUPING(et.type_name) = 1 THEN 'ВСЕ ТИПЫ'
        ELSE et.type_name
    END AS type_name,

    SUM(fp.tons_mined)                                          AS total_tons,
    ROUND(
        SUM(fp.tons_mined)::numeric /
        NULLIF(COUNT(DISTINCT fp.equipment_id), 0), 2
    )                                                           AS avg_tons_per_equipment,

    GROUPING(m.mine_name) * 2 + GROUPING(et.type_name)         AS grouping_level

FROM andreev.fact_production fp
JOIN andreev.dim_mine           m  ON m.mine_id           = fp.mine_id
JOIN andreev.dim_equipment      e  ON e.equipment_id      = fp.equipment_id
JOIN andreev.dim_equipment_type et ON et.equipment_type_id = e.equipment_type_id
JOIN andreev.dim_date           d  ON d.date_id           = fp.date_id

WHERE d.year = 2024
  AND d.quarter = 1

GROUP BY CUBE(m.mine_name, et.type_name)

ORDER BY
    grouping_level,
    m.mine_name NULLS LAST,
    et.type_name NULLS LAST;

-- ЗАДАНИЕ 3. GROUPING SETS — сводка KPI по нескольким срезам

SELECT
    CASE
        WHEN GROUPING(m.mine_name)  = 0 THEN 'Шахта'
        WHEN GROUPING(s.shift_name) = 0 THEN 'Смена'
        WHEN GROUPING(et.type_name) = 0 THEN 'Тип оборудования'
        ELSE 'ИТОГО'
    END AS dimension,

    COALESCE(m.mine_name, s.shift_name, et.type_name, 'Все') AS dimension_value,

    SUM(fp.tons_mined)                                             AS total_tons,
    SUM(fp.trips_count)                                            AS total_trips,
    ROUND(
        SUM(fp.tons_mined)::numeric /
        NULLIF(SUM(fp.trips_count), 0), 2
    )                                                              AS avg_tons_per_trip

FROM andreev.fact_production fp
JOIN andreev.dim_mine           m  ON m.mine_id           = fp.mine_id
JOIN andreev.dim_shift          s  ON s.shift_id          = fp.shift_id
JOIN andreev.dim_equipment      e  ON e.equipment_id      = fp.equipment_id
JOIN andreev.dim_equipment_type et ON et.equipment_type_id = e.equipment_type_id
JOIN andreev.dim_date           d  ON d.date_id           = fp.date_id

WHERE d.year = 2024
  AND d.month = 1

GROUP BY GROUPING SETS (
    (m.mine_name),
    (s.shift_name),
    (et.type_name),
    ()
)

ORDER BY
    dimension,
    dimension_value;

-- ЗАДАНИЕ 4. Условная агрегация — PIVOT

SELECT
    COALESCE(m.mine_name, '== ИТОГО ==') AS mine_name,

    ROUND(AVG(CASE WHEN d.month = 1 THEN oq.fe_content END)::numeric, 2) AS "Янв",
    ROUND(AVG(CASE WHEN d.month = 2 THEN oq.fe_content END)::numeric, 2) AS "Фев",
    ROUND(AVG(CASE WHEN d.month = 3 THEN oq.fe_content END)::numeric, 2) AS "Мар",
    ROUND(AVG(CASE WHEN d.month = 4 THEN oq.fe_content END)::numeric, 2) AS "Апр",
    ROUND(AVG(CASE WHEN d.month = 5 THEN oq.fe_content END)::numeric, 2) AS "Май",
    ROUND(AVG(CASE WHEN d.month = 6 THEN oq.fe_content END)::numeric, 2) AS "Июн",

    ROUND(AVG(oq.fe_content)::numeric, 2)                                 AS "Среднее за период"

FROM andreev.fact_ore_quality oq
JOIN andreev.dim_mine m ON m.mine_id = oq.mine_id
JOIN andreev.dim_date d ON d.date_id = oq.date_id

WHERE d.year = 2024
  AND d.month BETWEEN 1 AND 6

GROUP BY GROUPING SETS (
    (m.mine_name),
    ()
)

ORDER BY
    GROUPING(m.mine_name),
    m.mine_name;

-- ЗАДАНИЕ 5. crosstab — динамический разворот (20 баллов)
SELECT
    dr.reason_name,
    ROUND(SUM(fd.duration_min) / 60.0, 1) AS total_hours
FROM andreev.dim_downtime_reason dr
JOIN andreev.fact_equipment_downtime fd ON dr.reason_id = fd.reason_id
WHERE fd.date_id BETWEEN 20240101 AND 20240331
GROUP BY dr.reason_name
ORDER BY SUM(fd.duration_min) DESC
LIMIT 5;


SELECT *
FROM crosstab(
    $$
    SELECT
        e.equipment_name,
        dr.reason_name,
        ROUND(SUM(fd.duration_min) / 60.0, 1)
    FROM andreev.fact_equipment_downtime fd
    JOIN andreev.dim_equipment      e  ON e.equipment_id  = fd.equipment_id
    JOIN andreev.dim_downtime_reason dr ON dr.reason_id   = fd.reason_id
    WHERE fd.date_id BETWEEN 20240101 AND 20240331
    GROUP BY e.equipment_name, dr.reason_name
    ORDER BY e.equipment_name, dr.reason_name
    $$,
    $$
    SELECT reason_name
    FROM andreev.dim_downtime_reason dr
    JOIN andreev.fact_equipment_downtime fd ON dr.reason_id = fd.reason_id
    WHERE fd.date_id BETWEEN 20240101 AND 20240331
    GROUP BY dr.reason_name
    ORDER BY SUM(fd.duration_min) DESC
    LIMIT 5
    $$
) AS ct (
    equipment_name  TEXT,
    "Причина 1"     NUMERIC,
    "Причина 2"     NUMERIC,
    "Причина 3"     NUMERIC,
    "Причина 4"     NUMERIC,
    "Причина 5"     NUMERIC
);


-- ЗАДАНИЕ 6. Комплексный отчёт — ROLLUP + PIVOT + тренд
SELECT
    COALESCE(m.mine_name, '== ИТОГО ==') AS mine,
    'Добыча (тонн)'                       AS metric,

    ROUND(SUM(CASE WHEN d.month = 1 THEN fp.tons_mined END)::numeric, 1) AS jan,
    ROUND(SUM(CASE WHEN d.month = 2 THEN fp.tons_mined END)::numeric, 1) AS feb,
    ROUND(SUM(CASE WHEN d.month = 3 THEN fp.tons_mined END)::numeric, 1) AS mar,
    ROUND(SUM(fp.tons_mined)::numeric, 1)                                 AS q1_total,

    ROUND(
        (SUM(CASE WHEN d.month = 2 THEN fp.tons_mined END) -
         SUM(CASE WHEN d.month = 1 THEN fp.tons_mined END))::numeric * 100 /
        NULLIF(SUM(CASE WHEN d.month = 1 THEN fp.tons_mined END), 0), 1
    ) AS feb_vs_jan_pct,

    ROUND(
        (SUM(CASE WHEN d.month = 3 THEN fp.tons_mined END) -
         SUM(CASE WHEN d.month = 2 THEN fp.tons_mined END))::numeric * 100 /
        NULLIF(SUM(CASE WHEN d.month = 2 THEN fp.tons_mined END), 0), 1
    ) AS mar_vs_feb_pct,

    CASE
        WHEN ABS(
            (SUM(CASE WHEN d.month = 3 THEN fp.tons_mined END) -
             SUM(CASE WHEN d.month = 2 THEN fp.tons_mined END))::numeric * 100 /
            NULLIF(SUM(CASE WHEN d.month = 2 THEN fp.tons_mined END), 0)
        ) < 5 THEN 'стабильно'
        WHEN SUM(CASE WHEN d.month = 3 THEN fp.tons_mined END) >
             SUM(CASE WHEN d.month = 2 THEN fp.tons_mined END) THEN 'рост'
        ELSE 'снижение'
    END AS trend,

    GROUPING(m.mine_name) AS is_total

FROM andreev.fact_production fp
JOIN andreev.dim_mine m ON m.mine_id = fp.mine_id
JOIN andreev.dim_date d ON d.date_id = fp.date_id

WHERE d.year = 2024 AND d.quarter = 1

GROUP BY ROLLUP(m.mine_name)

UNION ALL

SELECT
    COALESCE(m.mine_name, '== ИТОГО ==') AS mine,
    'Простои (часы)'                       AS metric,

    ROUND(SUM(CASE WHEN d.month = 1 THEN fd.duration_min END)::numeric / 60, 1) AS jan,
    ROUND(SUM(CASE WHEN d.month = 2 THEN fd.duration_min END)::numeric / 60, 1) AS feb,
    ROUND(SUM(CASE WHEN d.month = 3 THEN fd.duration_min END)::numeric / 60, 1) AS mar,
    ROUND(SUM(fd.duration_min)::numeric / 60, 1)                                  AS q1_total,

    ROUND(
        (SUM(CASE WHEN d.month = 2 THEN fd.duration_min END) -
         SUM(CASE WHEN d.month = 1 THEN fd.duration_min END))::numeric * 100 /
        NULLIF(SUM(CASE WHEN d.month = 1 THEN fd.duration_min END), 0), 1
    ) AS feb_vs_jan_pct,

    ROUND(
        (SUM(CASE WHEN d.month = 3 THEN fd.duration_min END) -
         SUM(CASE WHEN d.month = 2 THEN fd.duration_min END))::numeric * 100 /
        NULLIF(SUM(CASE WHEN d.month = 2 THEN fd.duration_min END), 0), 1
    ) AS mar_vs_feb_pct,

    CASE
        WHEN ABS(
            (SUM(CASE WHEN d.month = 3 THEN fd.duration_min END) -
             SUM(CASE WHEN d.month = 2 THEN fd.duration_min END))::numeric * 100 /
            NULLIF(SUM(CASE WHEN d.month = 2 THEN fd.duration_min END), 0)
        ) < 5 THEN 'стабильно'
        WHEN SUM(CASE WHEN d.month = 3 THEN fd.duration_min END) >
             SUM(CASE WHEN d.month = 2 THEN fd.duration_min END) THEN 'рост'
        ELSE 'снижение'
    END AS trend,

    GROUPING(m.mine_name) AS is_total

FROM andreev.fact_equipment_downtime fd
JOIN andreev.dim_equipment e ON e.equipment_id = fd.equipment_id
JOIN andreev.dim_mine      m ON m.mine_id      = e.mine_id
JOIN andreev.dim_date      d ON d.date_id      = fd.date_id

WHERE d.year = 2024 AND d.quarter = 1

GROUP BY ROLLUP(m.mine_name)

ORDER BY
    metric,
    is_total,
    mine;