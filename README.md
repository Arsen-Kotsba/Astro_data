# ETL-процесс "Анализ открытия новых экзопланет"

В рамках проекта инкрементально загружаются официальные данные из архива NASA в MS SQL Server с использованием оркестратора Airflow.

## Структура
- `scripts/etl.py` - ETL логика и логирование.
- `scripts/sql/init_exoplanet_schema.sql` - schema for staging/core/log/view.
- `dags/exoplanet_incremental_etl.py` - Airflow DAG.
- `docs/exoplanet_source_contract.md` - описание источника данных.
- `tests/validation_checklist.sql` - SQL-проверки для подтверждения после загрузки.

## Установка
1. Загрузка необходимых пакетов:
   - `pip install -r requirements.txt`
2. Исполненяемый файл для создания схемы `market_data` в БД:
   - `scripts/sql/init_exoplanet_schema.sql`
3. При необходимости настроить переменные среды подключения:
   - `MSSQL_SERVER`, `MSSQL_PORT`, `MSSQL_DATABASE`, `MSSQL_USERNAME`, `MSSQL_PASSWORD`, `MSSQL_DRIVER`

## Ручной запуск
- `python scripts/etl.py`

## Airflow DAG
- DAG ID: `exoplanet_incremental_etl`
- Schedule: ежедневно в 03:00
- Таски:
  - `extract_to_stage`
  - `data_quality_checks`
  - `finalize_log`
