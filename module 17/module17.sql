-- Задание 1. Безопасное деление

CREATE OR REPLACE FUNCTION safe_production_rate(p_tons NUMERIC, p_hours NUMERIC)
RETURNS NUMERIC LANGUAGE plpgsql AS $$
BEGIN
    IF p_tons IS NULL OR p_hours IS NULL THEN
        RETURN NULL;
    END IF;

    BEGIN
        RETURN p_tons / p_hours;
    EXCEPTION
        WHEN division_by_zero THEN
            RAISE WARNING 'safe_production_rate: деление на ноль (tons=%, hours=%)', p_tons, p_hours;
            RETURN 0;
    END;
END;
$$;


-- Задание 2. Валидация данных телеметрии

CREATE OR REPLACE FUNCTION validate_sensor_reading(p_sensor_type VARCHAR, p_value NUMERIC)
RETURNS VARCHAR LANGUAGE plpgsql AS $$
DECLARE
    v_min NUMERIC;
    v_max NUMERIC;
BEGIN
    CASE p_sensor_type
        WHEN 'Температура' THEN v_min := -40;  v_max := 200;
        WHEN 'Давление'    THEN v_min := 0;    v_max := 500;
        WHEN 'Вибрация'    THEN v_min := 0;    v_max := 100;
        WHEN 'Скорость'    THEN v_min := 0;    v_max := 50;
        ELSE
            RAISE EXCEPTION 'Неизвестный тип датчика: %', p_sensor_type
                USING ERRCODE = 'S0001';
    END CASE;

    IF p_value < v_min OR p_value > v_max THEN
        RAISE EXCEPTION 'Значение % вне допустимого диапазона для типа "%"', p_value, p_sensor_type
            USING ERRCODE = 'S0002',
                  HINT    = 'Допустимый диапазон: ' || v_min || ' .. ' || v_max;
    END IF;

    RETURN 'OK';
END;
$$;


-- Задание 3. Обработка ошибок при вставке

DO $$
DECLARE
    v_inserted INT := 0;
    v_errors   INT := 0;

    -- Тестовые записи: (equipment_id, date_id, shift_id, downtime_minutes, reason)
    type_rec RECORD;
BEGIN
    -- Используем массив строк для имитации пакета
    FOR type_rec IN
        SELECT *
        FROM (VALUES
            (1,  20250115, 1, 60,   'Плановое ТО',         FALSE, FALSE, FALSE),
            (2,  20250115, 1, 120,  'Отказ двигателя',     FALSE, FALSE, FALSE),
            (3,  20250115, 2, 45,   'Замена фильтра',      FALSE, FALSE, FALSE),
            -- FK violation: несуществующий equipment_id
            (999999, 20250115, 1, 30, 'Тест FK',           TRUE,  FALSE, FALSE),
            (4,  20250115, 1, 90,   'Гидравлика',          FALSE, FALSE, FALSE),
            -- NOT NULL violation: NULL в downtime_minutes
            (5,  20250115, 1, NULL, 'Тест NULL',           FALSE, TRUE,  FALSE),
            (6,  20250115, 2, 15,   'Настройка',           FALSE, FALSE, FALSE),
            (7,  20250115, 1, 200,  'Капитальный ремонт',  FALSE, FALSE, FALSE),
            (8,  20250115, 2, 30,   'Смазка',              FALSE, FALSE, FALSE),
            (9,  20250115, 1, 75,   'Электрика',           FALSE, FALSE, FALSE)
        ) AS t(equipment_id, date_id, shift_id, downtime_minutes, reason,
               is_bad_fk, is_null_field, is_dup_pk)
    LOOP
        BEGIN
            INSERT INTO fact_equipment_downtime (equipment_id, date_id, shift_id, downtime_minutes, reason)
            VALUES (type_rec.equipment_id, type_rec.date_id, type_rec.shift_id,
                    type_rec.downtime_minutes, type_rec.reason);

            v_inserted := v_inserted + 1;

        EXCEPTION
            WHEN foreign_key_violation THEN
                v_errors := v_errors + 1;
                PERFORM log_error('ERROR', 'batch_downtime_insert', SQLSTATE, SQLERRM,
                    NULL, NULL, NULL,
                    jsonb_build_object('equipment_id', type_rec.equipment_id));
                RAISE WARNING 'Запись equipment_id=% — ошибка FK: %', type_rec.equipment_id, SQLERRM;

            WHEN not_null_violation THEN
                v_errors := v_errors + 1;
                PERFORM log_error('ERROR', 'batch_downtime_insert', SQLSTATE, SQLERRM,
                    NULL, NULL, NULL,
                    jsonb_build_object('equipment_id', type_rec.equipment_id));
                RAISE WARNING 'Запись equipment_id=% — NOT NULL нарушение: %', type_rec.equipment_id, SQLERRM;

            WHEN unique_violation THEN
                v_errors := v_errors + 1;
                PERFORM log_error('ERROR', 'batch_downtime_insert', SQLSTATE, SQLERRM,
                    NULL, NULL, NULL,
                    jsonb_build_object('equipment_id', type_rec.equipment_id));
                RAISE WARNING 'Запись equipment_id=% — дубль PK: %', type_rec.equipment_id, SQLERRM;

            WHEN OTHERS THEN
                v_errors := v_errors + 1;
                PERFORM log_error('ERROR', 'batch_downtime_insert', SQLSTATE, SQLERRM,
                    NULL, NULL, NULL,
                    jsonb_build_object('equipment_id', type_rec.equipment_id));
                RAISE WARNING 'Запись equipment_id=% — неизвестная ошибка: %', type_rec.equipment_id, SQLERRM;
        END;
    END LOOP;

    RAISE NOTICE '--- Итог вставки ---';
    RAISE NOTICE 'Успешно вставлено: %', v_inserted;
    RAISE NOTICE 'Ошибок: %',           v_errors;
END $$;


-- Задание 4. GET STACKED DIAGNOSTICS — детальный отчёт

CREATE OR REPLACE FUNCTION test_error_diagnostics(p_error_type INT)
RETURNS TABLE (field_name VARCHAR, field_value TEXT)
LANGUAGE plpgsql AS $$
DECLARE
    v_message    TEXT;
    v_detail     TEXT;
    v_hint       TEXT;
    v_context    TEXT;
    v_sqlstate   TEXT;
    v_constraint TEXT;
    v_datatype   TEXT;
    v_table      TEXT;
    v_column     TEXT;
    v_schema     TEXT;
    v_x          INT;
BEGIN
    BEGIN
        CASE p_error_type
            WHEN 1 THEN
                v_x := 1 / 0;

            WHEN 2 THEN
                INSERT INTO dim_mine (mine_id, mine_name)
                SELECT mine_id, mine_name FROM dim_mine LIMIT 1;

            WHEN 3 THEN
                INSERT INTO fact_production (equipment_id, date_id, shift_id, mine_id, tons_mined)
                VALUES (999999999, 20250115, 1, 1, 100);

            WHEN 4 THEN
                v_x := 'не число'::INT;

            WHEN 5 THEN
                RAISE EXCEPTION 'Пользовательская ошибка модуля 17'
                    USING ERRCODE = 'P0001',
                          DETAIL  = 'Детали пользовательской ошибки',
                          HINT    = 'Проверьте входные параметры';

            ELSE
                RAISE EXCEPTION 'Неизвестный тип ошибки: %', p_error_type;
        END CASE;

    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS
            v_message    = MESSAGE_TEXT,
            v_detail     = PG_EXCEPTION_DETAIL,
            v_hint       = PG_EXCEPTION_HINT,
            v_context    = PG_EXCEPTION_CONTEXT,
            v_sqlstate   = RETURNED_SQLSTATE,
            v_constraint = CONSTRAINT_NAME,
            v_datatype   = PG_DATATYPE_NAME,
            v_table      = TABLE_NAME,
            v_column     = COLUMN_NAME,
            v_schema     = SCHEMA_NAME;

        field_name := 'RETURNED_SQLSTATE';   field_value := v_sqlstate;   RETURN NEXT;
        field_name := 'MESSAGE_TEXT';         field_value := v_message;    RETURN NEXT;
        field_name := 'PG_EXCEPTION_DETAIL';  field_value := v_detail;     RETURN NEXT;
        field_name := 'PG_EXCEPTION_HINT';    field_value := v_hint;       RETURN NEXT;
        field_name := 'PG_EXCEPTION_CONTEXT'; field_value := v_context;    RETURN NEXT;
        field_name := 'CONSTRAINT_NAME';      field_value := v_constraint; RETURN NEXT;
        field_name := 'PG_DATATYPE_NAME';     field_value := v_datatype;   RETURN NEXT;
        field_name := 'TABLE_NAME';           field_value := v_table;      RETURN NEXT;
        field_name := 'COLUMN_NAME';          field_value := v_column;     RETURN NEXT;
        field_name := 'SCHEMA_NAME';          field_value := v_schema;     RETURN NEXT;
    END;
END;
$$;


-- Задание 5. Безопасный импорт с логированием

CREATE TABLE IF NOT EXISTS staging_lab_results (
    row_id       SERIAL,
    mine_name    TEXT,
    sample_date  TEXT,
    fe_content   TEXT,
    moisture     TEXT,
    status       VARCHAR(20) DEFAULT 'NEW',
    error_msg    TEXT
);

-- Тестовые данные
INSERT INTO staging_lab_results (mine_name, sample_date, fe_content, moisture)
VALUES
    ('Шахта №1',        '2025-01-15', '62.5',  '8.2'),   -- корректная
    ('Шахта №2',        '2025-01-15', '58.0',  '9.1'),   -- корректная
    ('Шахта №3',        '2025-01-16', '71.3',  '7.5'),   -- корректная
    ('Несуществующая',  '2025-01-15', '60.0',  '8.0'),   -- нет шахты
    ('Шахта №1',        '32-01-2025', '55.0',  '10.0'),  -- некорректная дата
    ('Шахта №2',        '2025-01-17', 'N/A',   '8.5'),   -- Fe не число
    ('Шахта №3',        '2025-01-17', '150',   '9.0'),   -- Fe > 100
    ('Шахта №1',        '2025-01-18', '64.2',  '7.8'),   -- корректная
    ('Шахта №2',        '2025-01-18', '59.7',  '8.3'),   -- корректная
    ('Шахта №3',        '2025-01-19', '67.1',  '6.9');   -- корректная

CREATE OR REPLACE FUNCTION process_lab_import()
RETURNS TABLE (total INT, valid INT, errors INT)
LANGUAGE plpgsql AS $$
DECLARE
    rec          RECORD;
    v_total      INT := 0;
    v_valid      INT := 0;
    v_errors     INT := 0;
    v_mine_id    INT;
    v_date       DATE;
    v_fe         NUMERIC;
    v_moisture   NUMERIC;
    v_err_msg    TEXT;
BEGIN
    FOR rec IN
        SELECT * FROM staging_lab_results WHERE status = 'NEW' ORDER BY row_id
    LOOP
        v_total   := v_total + 1;
        v_err_msg := NULL;

        BEGIN
            -- Проверка шахты
            SELECT mine_id INTO v_mine_id
            FROM dim_mine
            WHERE mine_name = rec.mine_name;

            IF NOT FOUND THEN
                RAISE EXCEPTION 'Шахта "%" не найдена', rec.mine_name
                    USING ERRCODE = 'P0002';
            END IF;

            -- Преобразование даты
            BEGIN
                v_date := rec.sample_date::DATE;
            EXCEPTION WHEN OTHERS THEN
                RAISE EXCEPTION 'Некорректная дата: "%"', rec.sample_date
                    USING ERRCODE = 'P0003';
            END;

            -- Преобразование Fe
            BEGIN
                v_fe := rec.fe_content::NUMERIC;
            EXCEPTION WHEN OTHERS THEN
                RAISE EXCEPTION 'Некорректное значение Fe: "%"', rec.fe_content
                    USING ERRCODE = 'P0004';
            END;

            -- Диапазон Fe
            IF v_fe < 0 OR v_fe > 100 THEN
                RAISE EXCEPTION 'Fe = % вне диапазона 0-100%%', v_fe
                    USING ERRCODE = 'P0005';
            END IF;

            -- Преобразование влажности
            BEGIN
                v_moisture := rec.moisture::NUMERIC;
            EXCEPTION WHEN OTHERS THEN
                RAISE EXCEPTION 'Некорректное значение влажности: "%"', rec.moisture
                    USING ERRCODE = 'P0006';
            END;

            -- Успех
            UPDATE staging_lab_results
            SET status = 'VALID'
            WHERE row_id = rec.row_id;

            v_valid := v_valid + 1;

        EXCEPTION WHEN OTHERS THEN
            v_err_msg := SQLERRM;
            v_errors  := v_errors + 1;

            UPDATE staging_lab_results
            SET status    = 'ERROR',
                error_msg = v_err_msg
            WHERE row_id = rec.row_id;

            PERFORM log_error(
                'ERROR', 'process_lab_import',
                SQLSTATE, SQLERRM,
                NULL, NULL, NULL,
                jsonb_build_object('row_id', rec.row_id, 'mine_name', rec.mine_name)
            );
        END;
    END LOOP;

    total  := v_total;
    valid  := v_valid;
    errors := v_errors;
    RETURN NEXT;
END;
$$;


-- Задание 6. Комплексная функция с иерархией обработки ошибок

CREATE TABLE IF NOT EXISTS daily_kpi (
    kpi_id         SERIAL PRIMARY KEY,
    mine_id        INT,
    date_id        INT,
    tons_mined     NUMERIC,
    oee_percent    NUMERIC,
    downtime_hours NUMERIC,
    quality_score  NUMERIC,
    status         VARCHAR(20),
    error_detail   TEXT,
    calculated_at  TIMESTAMP DEFAULT NOW(),
    UNIQUE (mine_id, date_id)
);

CREATE OR REPLACE FUNCTION recalculate_daily_kpi(p_date_id INT)
RETURNS TABLE (mines_processed INT, mines_ok INT, mines_error INT)
LANGUAGE plpgsql AS $$
DECLARE
    rec              RECORD;
    v_processed      INT := 0;
    v_ok             INT := 0;
    v_errors         INT := 0;
    v_tons           NUMERIC;
    v_oee            NUMERIC;
    v_downtime_hours NUMERIC;
    v_quality        NUMERIC;
    v_planned_hours  NUMERIC := 24;
    v_err_msg        TEXT;
    v_message        TEXT;
    v_context        TEXT;
    v_sqlstate       TEXT;
BEGIN
    FOR rec IN SELECT mine_id, mine_name FROM dim_mine LOOP
        v_processed := v_processed + 1;

        BEGIN
            -- Общая добыча
            SELECT COALESCE(SUM(fp.tons_mined), 0)
            INTO v_tons
            FROM fact_production fp
            WHERE fp.date_id = p_date_id
              AND fp.mine_id = rec.mine_id;

            -- OEE
            SELECT CASE WHEN v_planned_hours > 0
                        THEN ROUND(COALESCE(SUM(fp.operating_hours), 0) / v_planned_hours * 100, 1)
                        ELSE 0 END
            INTO v_oee
            FROM fact_production fp
            WHERE fp.date_id = p_date_id
              AND fp.mine_id = rec.mine_id;

            -- Часы простоев
            SELECT COALESCE(SUM(fed.downtime_minutes) / 60.0, 0)
            INTO v_downtime_hours
            FROM fact_equipment_downtime fed
            JOIN fact_production fp
                ON fed.equipment_id = fp.equipment_id
               AND fed.date_id      = fp.date_id
            WHERE fed.date_id = p_date_id
              AND fp.mine_id  = rec.mine_id;

            -- Среднее качество Fe
            SELECT ROUND(COALESCE(AVG(foq.fe_content), 0), 2)
            INTO v_quality
            FROM fact_ore_quality foq
            WHERE foq.date_id = p_date_id
              AND foq.mine_id = rec.mine_id;

            -- UPSERT
            INSERT INTO daily_kpi
                (mine_id, date_id, tons_mined, oee_percent, downtime_hours, quality_score, status, error_detail, calculated_at)
            VALUES
                (rec.mine_id, p_date_id, v_tons, v_oee, v_downtime_hours, v_quality, 'OK', NULL, NOW())
            ON CONFLICT (mine_id, date_id) DO UPDATE SET
                tons_mined     = EXCLUDED.tons_mined,
                oee_percent    = EXCLUDED.oee_percent,
                downtime_hours = EXCLUDED.downtime_hours,
                quality_score  = EXCLUDED.quality_score,
                status         = 'OK',
                error_detail   = NULL,
                calculated_at  = NOW();

            v_ok := v_ok + 1;

        EXCEPTION WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS
                v_message  = MESSAGE_TEXT,
                v_context  = PG_EXCEPTION_CONTEXT,
                v_sqlstate = RETURNED_SQLSTATE;

            v_err_msg := v_message;
            v_errors  := v_errors + 1;

            INSERT INTO daily_kpi
                (mine_id, date_id, tons_mined, oee_percent, downtime_hours, quality_score, status, error_detail, calculated_at)
            VALUES
                (rec.mine_id, p_date_id, NULL, NULL, NULL, NULL, 'ERROR', v_err_msg, NOW())
            ON CONFLICT (mine_id, date_id) DO UPDATE SET
                status       = 'ERROR',
                error_detail = EXCLUDED.error_detail,
                calculated_at = NOW();

            PERFORM log_error(
                'ERROR', 'recalculate_daily_kpi',
                v_sqlstate, v_message,
                NULL, NULL, v_context,
                jsonb_build_object('mine_id', rec.mine_id, 'date_id', p_date_id)
            );

            RAISE WARNING 'Ошибка для шахты % (id=%): %', rec.mine_name, rec.mine_id, v_err_msg;
        END;
    END LOOP;

    mines_processed := v_processed;
    mines_ok        := v_ok;
    mines_error     := v_errors;
    RETURN NEXT;

EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_message  = MESSAGE_TEXT,
        v_context  = PG_EXCEPTION_CONTEXT,
        v_sqlstate = RETURNED_SQLSTATE;

    PERFORM log_error(
        'CRITICAL', 'recalculate_daily_kpi',
        v_sqlstate, v_message,
        NULL, NULL, v_context,
        jsonb_build_object('date_id', p_date_id)
    );

    RAISE EXCEPTION 'Критическая ошибка в recalculate_daily_kpi: %', v_message;
END;
$$;