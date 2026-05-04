-- Тест 1: Первоначальная загрузка, создаем строки в dim_exoplanets.
select count(*) as dim_row_count
from dbo.dim_exoplanets;

-- Тест 2: Повторный запуск, проверяем не должно увеличиться количество строк.
select count(*) as dim_row_count_after_second_run
from dbo.dim_exoplanets;

-- Тест 3: Проверка на отсутствие дубликатов pl_name/disc_pubdate для каждого source_run_id.
select source_run_id, pl_name, disc_pubdate, count(*) as cnt
from dbo.stg_exoplanets_raw
group by source_run_id, pl_name, disc_pubdate
having count(*) > 1;

-- Тест 4: Проверка уникальности названий планет в core-слое.
select pl_name, count(*) as cnt
from dbo.dim_exoplanets
group by pl_name
having count(*) > 1;

-- Тест 5: Стаутс ETL-процесса и динамика watermark.
select TOP 20
    id,
    entity_name,
    status,
    rows_loaded,
    watermark_before,
    watermark_after,
    start_time,
    end_time,
    error_message
from dbo.etl_log
order by id desc;
