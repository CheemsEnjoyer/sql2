-- Задание 1. Анализ длины строковых полей --
SELECT
    equipment_name,
    LENGTH(COALESCE(equipment_name, '')) AS name_len,
    LENGTH(COALESCE(inventory_number, '')) AS inv_len,
    LENGTH(COALESCE(model, '')) AS model_len,
    LENGTH(COALESCE(manufacturer, '')) AS manuf_len,

    LENGTH(COALESCE(equipment_name, '')) +
    LENGTH(COALESCE(inventory_number, '')) +
    LENGTH(COALESCE(model, '')) +
    LENGTH(COALESCE(manufacturer, '')) AS total_text_length

FROM dim_equipment
ORDER BY total_text_length DESC;

-- Задание 2. Разбор инвентарного номера --
SELECT
    inventory_number,
    prefix,
    type_code,
    serial_no,
    CASE type_code
        WHEN 'LHD' THEN 'Погрузочно-доставочная машина'
        WHEN 'TRUCK' THEN 'Шахтный самосвал'
        WHEN 'CART' THEN 'Вагонетка'
        WHEN 'SKIP' THEN 'Скиповой подъёмник'
        ELSE 'Неизвестный тип'
    END AS type_description
FROM (
    SELECT
        inventory_number,
        SPLIT_PART(inventory_number, '-', 1) AS prefix,
        SPLIT_PART(inventory_number, '-', 2) AS type_code,
        SPLIT_PART(inventory_number, '-', 3)::INT AS serial_no
    FROM dim_equipment
) t
ORDER BY type_code, serial_no;

-- Задание 3. Формирование краткого имени оператора --
SELECT
    last_name,
    first_name,
    middle_name,
    position,

    last_name || ' ' ||
    LEFT(first_name, 1) || '.' ||
    COALESCE(LEFT(middle_name, 1) || '.', '') AS short_name_1,

    LEFT(first_name, 1) || '.' ||
    COALESCE(LEFT(middle_name, 1) || '.', '') || ' ' ||
    last_name AS short_name_2,

    UPPER(last_name) AS last_name_upper,

    LOWER(position) AS position_lower

FROM dim_operator
ORDER BY last_name;

-- 4 Задание 4. Поиск оборудования по шаблону --.
-- Название оборудования содержит «ПДМ»
SELECT
    equipment_name,
    inventory_number,
    manufacturer,
    model
FROM dim_equipment
WHERE equipment_name LIKE '%ПДМ%'
ORDER BY equipment_name;

-- Производитель начинается на S/S
SELECT
    equipment_name,
    inventory_number,
    manufacturer,
    model
FROM dim_equipment
WHERE manufacturer ILIKE 's%'
ORDER BY manufacturer, equipment_name;

-- Название шахты содержит кавычки
SELECT
    mine_name,
    mine_code,
    region,
    city
FROM dim_mine
WHERE POSITION('"' IN mine_name) > 0
ORDER BY mine_name;

-- Серийная часть инвентарного номера от 001 до 010
SELECT
    equipment_name,
    inventory_number
FROM dim_equipment
WHERE inventory_number ~ '^INV-(LHD|TRUCK|CART|SKIP)-(00[1-9]|010)$'
ORDER BY inventory_number;

-- Задание 5. Список оборудования по шахтам (STRING_AGG) --
SELECT
    m.mine_name,
    COUNT(e.equipment_id) AS equipment_count,
    
    STRING_AGG(
        e.equipment_name,
        ', ' ORDER BY e.equipment_name
    ) AS equipment_list,
    
    STRING_AGG(
        DISTINCT e.manufacturer,
        ', ' ORDER BY e.manufacturer
    ) AS manufacturers_list

FROM dim_mine m
LEFT JOIN dim_equipment e
    ON m.mine_id = e.mine_id

GROUP BY m.mine_name
ORDER BY m.mine_name;

-- Задание 6. Возраст оборудования
SELECT
    equipment_name,
    commissioning_date,

    AGE(CURRENT_DATE, commissioning_date) AS age_full,

    EXTRACT(YEAR FROM AGE(CURRENT_DATE, commissioning_date))::INT AS years,


    (CURRENT_DATE - commissioning_date) AS days,

    CASE
        WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, commissioning_date)) < 2 THEN 'Новое'
        WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, commissioning_date)) BETWEEN 2 AND 4 THEN 'Рабочее'
        ELSE 'Требует внимания'
    END AS category

FROM dim_equipment
ORDER BY years DESC, days DESC;

-- Задание 7. Формирование дат для отчетов --
SELECT
    equipment_name,
    commissioning_date,

    -- Русский формат
    TO_CHAR(commissioning_date, 'DD.MM.YYYY') AS date_ru,

    -- Полный формат
    TO_CHAR(commissioning_date, 'DD TMMonth YYYY "г."') AS date_full,

    -- ISO формат
    TO_CHAR(commissioning_date, 'YYYY-MM-DD') AS date_iso,

    -- Год-квартал
    TO_CHAR(commissioning_date, 'YYYY-"Q"Q') AS year_quarter,

    -- День недели
    TO_CHAR(commissioning_date, 'TMDay') AS day_name,

    -- Год-месяц
    TO_CHAR(commissioning_date, 'YYYY-MM') AS year_month

FROM dim_equipment
ORDER BY commissioning_date;

-- Задание 8. Анализ простоев по дням недели и часам --
-- Извлечение дня недели, часа и минуты
SELECT
    downtime_id,
    start_time,
    EXTRACT(DOW FROM start_time) AS day_of_week_num,
    EXTRACT(HOUR FROM start_time) AS hour_num,
    EXTRACT(MINUTE FROM start_time) AS minute_num
FROM fact_equipment_downtime
ORDER BY start_time;

-- Анализ по дням недели
SELECT
    CASE EXTRACT(DOW FROM start_time)
        WHEN 0 THEN 'Воскресенье'
        WHEN 1 THEN 'Понедельник'
        WHEN 2 THEN 'Вторник'
        WHEN 3 THEN 'Среда'
        WHEN 4 THEN 'Четверг'
        WHEN 5 THEN 'Пятница'
        WHEN 6 THEN 'Суббота'
    END AS day_name,
    COUNT(*) AS downtime_count,
    ROUND(AVG(duration_min), 1) AS avg_duration_min
FROM fact_equipment_downtime
GROUP BY EXTRACT(DOW FROM start_time)
ORDER BY EXTRACT(DOW FROM start_time);

-- Группировка по часам через DATE_TRUNC
SELECT
    DATE_TRUNC('hour', start_time) AS hour_start,
    COUNT(*) AS downtime_count
FROM fact_equipment_downtime
GROUP BY DATE_TRUNC('hour', start_time)
ORDER BY hour_start;

-- Количество простоев по часу суток
SELECT
    EXTRACT(HOUR FROM start_time) AS hour_num,
    COUNT(*) AS downtime_count
FROM fact_equipment_downtime
GROUP BY EXTRACT(HOUR FROM start_time)
ORDER BY hour_num;

-- Пиковый час
SELECT
    EXTRACT(HOUR FROM start_time) AS peak_hour,
    COUNT(*) AS downtime_count
FROM fact_equipment_downtime
GROUP BY EXTRACT(HOUR FROM start_time)
ORDER BY downtime_count DESC, peak_hour
LIMIT 1;

-- Задание 9. Расчет графика калибровки датчиков --
SELECT
    s.sensor_id,
    e.equipment_name,
    st.type_name AS sensor_type,
    s.calibration_date,
    CURRENT_DATE - s.calibration_date AS days_since_calibration,
    s.calibration_date + INTERVAL '180 days' AS next_calibration_date,
    CASE
        WHEN AGE(CURRENT_DATE, s.calibration_date) > INTERVAL '180 days' THEN 'Просрочена'
        WHEN AGE(CURRENT_DATE, s.calibration_date) BETWEEN INTERVAL '150 days' AND INTERVAL '180 days' THEN 'Скоро'
        ELSE 'В норме'
    END AS calibration_status
FROM dim_sensor s
JOIN dim_equipment e
    ON s.equipment_id = e.equipment_id
JOIN dim_sensor_type st
    ON s.sensor_type_id = st.sensor_type_id
ORDER BY
    CASE
        WHEN AGE(CURRENT_DATE, s.calibration_date) > INTERVAL '180 days' THEN 1
        WHEN AGE(CURRENT_DATE, s.calibration_date) BETWEEN INTERVAL '150 days' AND INTERVAL '180 days' THEN 2
        ELSE 3
    END,
    s.calibration_date;

-- Задание 10.Комплексный отчёт: карточка оборудования --
SELECT
    CONCAT(
        '[',
        CASE
            WHEN et.type_name = 'Погрузочно-доставочная машина' THEN 'ПДМ'
            WHEN et.type_name = 'Шахтный самосвал' THEN 'САМОСВАЛ'
            WHEN et.type_name = 'Вагонетка' THEN 'ВАГОНЕТКА'
            WHEN et.type_name = 'Скиповой подъёмник' THEN 'СКИП'
            ELSE UPPER(et.type_name)
        END,
        '] ',
        e.equipment_name,
        ' (',
        e.manufacturer, ' ', e.model,
        ') | Шахта: ',
        REPLACE(REPLACE(m.mine_name, 'Шахта "', ''), '"', ''),
        ' | Введён: ',
        TO_CHAR(e.commissioning_date, 'DD.MM.YYYY'),
        ' | Возраст: ',
        EXTRACT(YEAR FROM AGE(CURRENT_DATE, e.commissioning_date))::INT,
        ' лет',
        ' | Статус: ',
        CASE e.status
            WHEN 'active' THEN 'АКТИВЕН'
            WHEN 'maintenance' THEN 'НА ТО'
            WHEN 'decommissioned' THEN 'СПИСАН'
            ELSE UPPER(e.status)
        END,
        ' | Видеорег.: ',
        CASE
            WHEN e.has_video_recorder THEN 'ДА'
            ELSE 'НЕТ'
        END,
        ' | Навигация: ',
        CASE
            WHEN e.has_navigation THEN 'ДА'
            ELSE 'НЕТ'
        END
    ) AS equipment_card
FROM dim_equipment e
JOIN dim_equipment_type et
    ON e.equipment_type_id = et.equipment_type_id
JOIN dim_mine m
    ON e.mine_id = m.mine_id
ORDER BY e.equipment_name;
