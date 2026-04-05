-- Задание 1. BEGIN / COMMIT / ROLLBACK

-- Смена 1 — фиксируем
BEGIN;

INSERT INTO fact_production (date_id, shift_id, mine_id, equipment_id, tons_mined, operating_hours)
VALUES
    (20250310, 1, 1, 1, 120.5, 7.5),
    (20250310, 1, 1, 2, 98.0,  6.0),
    (20250310, 1, 1, 3, 145.3, 8.0),
    (20250310, 1, 1, 4, 87.2,  5.5),
    (20250310, 1, 1, 5, 110.0, 7.0);

SELECT * FROM fact_production WHERE date_id = 20250310 AND shift_id = 1;

COMMIT;

SELECT * FROM fact_production WHERE date_id = 20250310 AND shift_id = 1;


-- Смена 2 — откатываем
BEGIN;

INSERT INTO fact_production (date_id, shift_id, mine_id, equipment_id, tons_mined, operating_hours)
VALUES
    (20250310, 2, 1, 1, 95.0,  6.5),
    (20250310, 2, 1, 2, 130.0, 8.0),
    (20250310, 2, 1, 3, 78.5,  5.0),
    (20250310, 2, 1, 4, 102.0, 7.0),
    (20250310, 2, 1, 5, 88.0,  6.0);

SELECT * FROM fact_production WHERE date_id = 20250310 AND shift_id = 2;

ROLLBACK;

SELECT * FROM fact_production WHERE date_id = 20250310 AND shift_id = 2;

-- Задание 2. SAVEPOINT — частичная загрузка

BEGIN;

-- Вставка добычи
INSERT INTO fact_production (date_id, shift_id, mine_id, equipment_id, tons_mined, operating_hours)
VALUES (20250311, 1, 1, 1, 100.0, 8.0);

SAVEPOINT sp_after_production;

-- Вставка качества руды
INSERT INTO fact_ore_quality (date_id, mine_id, shift_id, fe_content, moisture)
VALUES (20250311, 1, 1, 63.5, 8.1);

SAVEPOINT sp_after_quality;

-- Попытка вставить телеметрию с несуществующим sensor_id (намеренная ошибка)
INSERT INTO fact_equipment_telemetry (date_id, sensor_id, value, recorded_at)
VALUES (20250311, 999999999, 42.0, NOW());

-- Откат только к точке после quality
ROLLBACK TO sp_after_quality;

-- Данные production и quality сохранены, телеметрия откатилась
COMMIT;

-- Проверка
SELECT * FROM fact_production   WHERE date_id = 20250311 AND shift_id = 1;
SELECT * FROM fact_ore_quality  WHERE date_id = 20250311;
SELECT * FROM fact_equipment_telemetry WHERE date_id = 20250311;

-- Задание 3. ACID на практике

CREATE TABLE IF NOT EXISTS equipment_balance (
    equipment_id INT PRIMARY KEY,
    balance_tons NUMERIC DEFAULT 0,
    CHECK (balance_tons >= 0)
);

INSERT INTO equipment_balance VALUES (1, 1000), (2, 500)
ON CONFLICT (equipment_id) DO UPDATE SET balance_tons = EXCLUDED.balance_tons;

-- Перевод 200 тонн: оборудование 1 → оборудование 2
BEGIN;

UPDATE equipment_balance SET balance_tons = balance_tons - 200 WHERE equipment_id = 1;
UPDATE equipment_balance SET balance_tons = balance_tons + 200 WHERE equipment_id = 2;

COMMIT;

-- Проверка балансов: должно быть 800 и 700
SELECT * FROM equipment_balance;

-- Попытка перевести 1500 тонн с оборудования 2 (нарушение CHECK)
BEGIN;

UPDATE equipment_balance SET balance_tons = balance_tons - 1500 WHERE equipment_id = 2;
UPDATE equipment_balance SET balance_tons = balance_tons + 1500 WHERE equipment_id = 1;

COMMIT;

SELECT * FROM equipment_balance;

-- Задание 5. Обработка конфликтов блокировок

CREATE OR REPLACE FUNCTION safe_update_production(
    p_production_id INT,
    p_new_tons      NUMERIC,
    p_timeout_ms    INT DEFAULT 5000
)
RETURNS VARCHAR LANGUAGE plpgsql AS $$
DECLARE
    v_rec RECORD;
BEGIN
    -- Установка таймаута ожидания блокировки
    EXECUTE format('SET LOCAL lock_timeout = %L', p_timeout_ms || 'ms');

    BEGIN
        -- Попытка захватить блокировку строки
        SELECT * INTO v_rec
        FROM fact_production
        WHERE production_id = p_production_id
        FOR UPDATE;

        IF NOT FOUND THEN
            RETURN 'ЗАПИСЬ НЕ НАЙДЕНА';
        END IF;

        UPDATE fact_production
        SET tons_mined = p_new_tons
        WHERE production_id = p_production_id;

        RETURN 'OK';

    EXCEPTION
        WHEN lock_not_available THEN
            RETURN 'ЗАБЛОКИРОВАНО: попробуйте позже';
        WHEN deadlock_detected THEN
            RETURN 'DEADLOCK: повторите операцию';
    END;
END;
$$;

-- Задание 6. Предотвращение Deadlock

CREATE TABLE IF NOT EXISTS mine_daily_stats (
    mine_id    INT,
    date_id    INT,
    total_tons NUMERIC DEFAULT 0,
    status     VARCHAR(20) DEFAULT 'pending',
    PRIMARY KEY (mine_id, date_id)
);

INSERT INTO mine_daily_stats (mine_id, date_id)
VALUES (1, 20250301), (2, 20250301)
ON CONFLICT DO NOTHING;

-- Исправление: блокируем строки в отсортированном порядке mine_id
CREATE OR REPLACE FUNCTION update_mine_stats(
    p_mine_ids INT[],
    p_date_id  INT,
    p_tons     NUMERIC[]
)
RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE
    v_sorted_ids INT[];
    i            INT;
    v_mine_id    INT;
BEGIN
    -- Сортируем mine_ids по возрастанию — ключ от deadlock
    SELECT ARRAY_AGG(x ORDER BY x)
    INTO v_sorted_ids
    FROM UNNEST(p_mine_ids) x;

    -- Блокируем строки в строго отсортированном порядке
    FOR i IN 1..array_length(v_sorted_ids, 1) LOOP
        PERFORM *
        FROM mine_daily_stats
        WHERE mine_id = v_sorted_ids[i] AND date_id = p_date_id
        FOR UPDATE;
    END LOOP;

    -- Обновляем данные
    FOR i IN 1..array_length(p_mine_ids, 1) LOOP
        UPDATE mine_daily_stats
        SET total_tons = p_tons[i],
            status     = 'done'
        WHERE mine_id = p_mine_ids[i] AND date_id = p_date_id;
    END LOOP;
END;
$$;

-- Задание 7. Advisory Lock — защита ETL

CREATE TABLE IF NOT EXISTS report_daily_production (
    report_date    DATE PRIMARY KEY,
    total_tons     NUMERIC,
    total_shifts   INT,
    avg_fe         NUMERIC,
    created_at     TIMESTAMP DEFAULT NOW(),
    updated_at     TIMESTAMP DEFAULT NOW()
);

CREATE OR REPLACE FUNCTION etl_daily_report(p_date_id INT)
RETURNS VARCHAR LANGUAGE plpgsql AS $$
DECLARE
    v_lock_key   BIGINT := p_date_id;
    v_locked     BOOLEAN;
    v_report_date DATE;
    v_exists     BOOLEAN;
    v_tons       NUMERIC;
    v_shifts     INT;
    v_avg_fe     NUMERIC;
BEGIN
    -- Попытка захватить advisory lock
    v_locked := pg_try_advisory_lock(v_lock_key);

    IF NOT v_locked THEN
        RETURN 'ETL уже запущен';
    END IF;

    BEGIN
        v_report_date := TO_DATE(p_date_id::TEXT, 'YYYYMMDD');

        -- Проверка: уже обработана?
        SELECT EXISTS (
            SELECT 1 FROM report_daily_production WHERE report_date = v_report_date
        ) INTO v_exists;

        IF v_exists THEN
            PERFORM pg_advisory_unlock(v_lock_key);
            RETURN 'Дата уже обработана: ' || v_report_date;
        END IF;

        -- Агрегация данных
        SELECT
            COALESCE(SUM(fp.tons_mined), 0),
            COUNT(DISTINCT fp.shift_id)
        INTO v_tons, v_shifts
        FROM fact_production fp
        WHERE fp.date_id = p_date_id;

        SELECT COALESCE(ROUND(AVG(foq.fe_content), 2), 0)
        INTO v_avg_fe
        FROM fact_ore_quality foq
        WHERE foq.date_id = p_date_id;

        -- Вставка в отчёт
        INSERT INTO report_daily_production (report_date, total_tons, total_shifts, avg_fe, updated_at)
        VALUES (v_report_date, v_tons, v_shifts, v_avg_fe, NOW())
        ON CONFLICT (report_date) DO UPDATE SET
            total_tons   = EXCLUDED.total_tons,
            total_shifts = EXCLUDED.total_shifts,
            avg_fe       = EXCLUDED.avg_fe,
            updated_at   = NOW();

        -- Освобождаем блокировку
        PERFORM pg_advisory_unlock(v_lock_key);

        RETURN 'OK: загружено ' || v_tons || ' т за ' || v_report_date;

    EXCEPTION WHEN OTHERS THEN
        PERFORM pg_advisory_unlock(v_lock_key);
        RAISE;
    END;
END;
$$;

-- Задание 8. MVCC — наблюдение

CREATE TABLE IF NOT EXISTS test_mvcc (
    id   INT PRIMARY KEY,
    data VARCHAR(50)
);

INSERT INTO test_mvcc VALUES (1, 'версия 1')
ON CONFLICT (id) DO UPDATE SET data = 'версия 1';

-- Начальное состояние
SELECT ctid, xmin, xmax, * FROM test_mvcc;

-- Обновление 1
UPDATE test_mvcc SET data = 'версия 2' WHERE id = 1;
SELECT ctid, xmin, xmax, * FROM test_mvcc;

-- Обновление 2
UPDATE test_mvcc SET data = 'версия 3' WHERE id = 1;
SELECT ctid, xmin, xmax, * FROM test_mvcc;

-- Обновление 3
UPDATE test_mvcc SET data = 'версия 4' WHERE id = 1;
SELECT ctid, xmin, xmax, * FROM test_mvcc;

-- VACUUM убирает мёртвые версии
VACUUM test_mvcc;
SELECT ctid, xmin, xmax, * FROM test_mvcc;

-- Задание 9. Процедура с управлением транзакциями

CREATE OR REPLACE PROCEDURE load_monthly_production(p_year INT, p_month INT)
LANGUAGE plpgsql AS $$
DECLARE
    v_date         DATE;
    v_date_id      INT;
    v_days         INT;
    v_processed    INT := 0;
    v_errors       INT := 0;
    v_rows         INT := 0;
    v_rows_day     INT;
    v_tons         NUMERIC;
    v_shifts       INT;
    v_avg_fe       NUMERIC;
    v_message      TEXT;
BEGIN
    v_days := EXTRACT(DAY FROM (DATE_TRUNC('month', make_date(p_year, p_month, 1))
                                + INTERVAL '1 month - 1 day'))::INT;

    FOR d IN 1..v_days LOOP
        v_date    := make_date(p_year, p_month, d);
        v_date_id := TO_CHAR(v_date, 'YYYYMMDD')::INT;

        BEGIN
            SELECT COALESCE(SUM(tons_mined), 0), COUNT(DISTINCT shift_id)
            INTO v_tons, v_shifts
            FROM fact_production
            WHERE date_id = v_date_id;

            SELECT COALESCE(ROUND(AVG(fe_content), 2), 0)
            INTO v_avg_fe
            FROM fact_ore_quality
            WHERE date_id = v_date_id;

            INSERT INTO report_daily_production (report_date, total_tons, total_shifts, avg_fe, updated_at)
            VALUES (v_date, v_tons, v_shifts, v_avg_fe, NOW())
            ON CONFLICT (report_date) DO UPDATE SET
                total_tons   = EXCLUDED.total_tons,
                total_shifts = EXCLUDED.total_shifts,
                avg_fe       = EXCLUDED.avg_fe,
                updated_at   = NOW();

            GET DIAGNOSTICS v_rows_day = ROW_COUNT;
            v_rows     := v_rows + v_rows_day;
            v_processed := v_processed + 1;

            -- Фиксируем каждый день отдельно
            COMMIT;

        EXCEPTION WHEN OTHERS THEN
            v_message := SQLERRM;
            v_errors  := v_errors + 1;

            RAISE NOTICE 'Ошибка для даты %: %', v_date, v_message;

            -- Откатываем только текущий день
            ROLLBACK;
        END;
    END LOOP;

    RAISE NOTICE '=== Итог load_monthly_production(%/%) ===', p_month, p_year;
    RAISE NOTICE 'Дней обработано: %', v_processed;
    RAISE NOTICE 'Ошибок: %',          v_errors;
    RAISE NOTICE 'Строк вставлено/обновлено: %', v_rows;
END;
$$;

-- Задание 10. Параллельная обработка смен (оптимистичная блокировка)

CREATE TABLE IF NOT EXISTS shift_summary (
    date_id     INT,
    shift_id    INT,
    mine_id     INT,
    total_tons  NUMERIC,
    total_trips INT,
    oee_percent NUMERIC,
    updated_by  VARCHAR(50),
    updated_at  TIMESTAMP DEFAULT NOW(),
    version     INT DEFAULT 1,
    PRIMARY KEY (date_id, shift_id, mine_id)
);

-- Оптимистичная блокировка через version
CREATE OR REPLACE FUNCTION update_shift_summary(
    p_date_id    INT,
    p_shift_id   INT,
    p_mine_id    INT,
    p_total_tons NUMERIC,
    p_version    INT
)
RETURNS VARCHAR LANGUAGE plpgsql AS $$
DECLARE
    v_updated INT;
BEGIN
    UPDATE shift_summary
    SET
        total_tons = p_total_tons,
        updated_by = CURRENT_USER,
        updated_at = NOW(),
        version    = version + 1
    WHERE date_id  = p_date_id
      AND shift_id = p_shift_id
      AND mine_id  = p_mine_id
      AND version  = p_version;

    GET DIAGNOSTICS v_updated = ROW_COUNT;

    IF v_updated = 0 THEN
        -- Либо записи нет, либо version не совпал
        IF EXISTS (
            SELECT 1 FROM shift_summary
            WHERE date_id  = p_date_id
              AND shift_id = p_shift_id
              AND mine_id  = p_mine_id
        ) THEN
            RETURN 'Данные были изменены другим пользователем';
        ELSE
            RETURN 'ЗАПИСЬ НЕ НАЙДЕНА';
        END IF;
    END IF;

    RETURN 'OK';
END;
$$;

-- Пересчёт всех смен для даты с advisory lock
CREATE OR REPLACE FUNCTION refresh_shift_summary(p_date_id INT)
RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE
    v_locked BOOLEAN;
BEGIN
    -- Транзакционный advisory lock (освобождается при COMMIT/ROLLBACK)
    v_locked := pg_try_advisory_xact_lock(p_date_id::BIGINT);

    IF NOT v_locked THEN
        RAISE EXCEPTION 'refresh_shift_summary уже выполняется для date_id=%', p_date_id;
    END IF;

    -- UPSERT агрегированных данных по каждой комбинации смена+шахта
    INSERT INTO shift_summary (date_id, shift_id, mine_id, total_tons, total_trips, oee_percent, updated_by, updated_at, version)
    SELECT
        fp.date_id,
        fp.shift_id,
        fp.mine_id,
        COALESCE(SUM(fp.tons_mined), 0)                                        AS total_tons,
        COUNT(fp.production_id)                                                 AS total_trips,
        CASE WHEN COUNT(fp.equipment_id) > 0
             THEN ROUND(COALESCE(SUM(fp.operating_hours), 0)
                  / (COUNT(DISTINCT fp.equipment_id) * 8.0) * 100, 1)
             ELSE 0 END                                                         AS oee_percent,
        CURRENT_USER,
        NOW(),
        1
    FROM fact_production fp
    WHERE fp.date_id = p_date_id
    GROUP BY fp.date_id, fp.shift_id, fp.mine_id
    ON CONFLICT (date_id, shift_id, mine_id) DO UPDATE SET
        total_tons  = EXCLUDED.total_tons,
        total_trips = EXCLUDED.total_trips,
        oee_percent = EXCLUDED.oee_percent,
        updated_by  = EXCLUDED.updated_by,
        updated_at  = EXCLUDED.updated_at,
        version     = shift_summary.version + 1;
END;
$$;