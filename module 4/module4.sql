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


-- 4 Задание 4. Поиск оборудования по шаблону --.
-- 1. Название оборудования содержит «ПДМ»
SELECT
    equipment_name,
    inventory_number,
    manufacturer,
    model
FROM dim_equipment
WHERE equipment_name LIKE '%ПДМ%'
ORDER BY equipment_name;

-- 2. Производитель начинается на S/S...
SELECT
    equipment_name,
    inventory_number,
    manufacturer,
    model
FROM dim_equipment
WHERE manufacturer ILIKE 's%'
ORDER BY manufacturer, equipment_name;

-- 3. Название шахты содержит кавычки
SELECT
    mine_name,
    mine_code,
    region,
    city
FROM dim_mine
WHERE POSITION('"' IN mine_name) > 0
ORDER BY mine_name;

-- 4. Серийная часть инвентарного номера от 001 до 010
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