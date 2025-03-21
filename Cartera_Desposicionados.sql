-- ==========================================================================================
-- SCRIPT: Carga histórica e incremental de cartera GMM enriquecida (routers + servicios)
-- OBJETIVO: Cargar históricos desde 2025-01-01 y permitir luego actualización diaria por mes
-- ==========================================================================================

-- ======================================
-- DECLARACIÓN DE PARÁMETROS DE FECHA
-- ======================================
DECLARE VAR_PERIOD_START DATE DEFAULT '2024-09-01';        -- Inicio histórico
DECLARE VAR_PERIOD_END   DATE DEFAULT CURRENT_DATE();           -- Hasta hoy
DECLARE VAR_PERIOD_ACT   DATE DEFAULT DATE_TRUNC(CURRENT_DATE(), MONTH);  -- mes en curso

-- ======================================
-- 1. CARGA DE ROUTERS ACTIVOS DESDE 2025-01-01
-- ======================================
CREATE OR REPLACE TABLE `mo-migrations-reporting.STB_CPE_DESPO.gmm_router_ftth` AS
SELECT DISTINCT
  DATE_TRUNC(data_day_dt, MONTH)                         AS MES,
  brand_ds                                               AS BRAND_DS,
  customer_id                                            AS CUSTOMER_ID,
  identity_ds                                            AS IDENTITY_DS,
  router_model_id                                        AS ROUTER_MODEL_ID,
  router_model_alias                                     AS ROUTER_MODEL_ALIAS,
  router_model_wifi_type                                 AS ROUTER_MODEL_WIFI_TYPE,
  provcode_id                                            AS PROVCODE_ID,
  workorder_id                                           AS WORKORDER_ID,
  remote_id                                              AS REMOTE_ID,
  active_router_lg                                       AS ACTIVE_ROUTER_LG
FROM `mm-datamart-kd.QOS.rep_det_router_ftth`
WHERE data_day_dt >= VAR_PERIOD_START
  AND active_router_lg = TRUE;

-- ======================================
-- 2. CARGA DE DESCO ACTIVOS DESDE 2025-01-01 
-- Esta tabla no la estamos usando en el proceso. Se deja ya que estamos en fase de analisis.
-- Este bloque de query se inserta en un LEFT JOIN en el paso n5
-- ======================================
CREATE OR REPLACE TABLE `mo-migrations-reporting.STB_CPE_DESPO.gmm_deco_model` AS
SELECT DISTINCT
  customer_id,
  brand_ds,
  device_data.modelName AS DECO_MODEL
FROM `mm-datamart-kd.QOS.rep_cx_tv_gmm`
WHERE data_day_dt BETWEEN VAR_PERIOD_START AND VAR_PERIOD_END
  AND customer_id IS NOT NULL
  AND device_data.modelName IS NOT NULL;


-- ======================================
-- 3. KPI SERVICES CRUZADO CON ROUTERS ACTIVOS (desde 2025-01-01)
-- ======================================
CREATE OR REPLACE TABLE `mo-migrations-reporting.STB_CPE_DESPO.gmm_router_ftth_serv` AS
SELECT DISTINCT
  A.brand_ds                                            AS BRAND_DS,
  A.customer_id                                         AS CUSTOMER_ID,
  A.phone_nm                                            AS PHONE_NM,
  A.service_id                                          AS SERVICE_ID_SEMANTIC,
  A.account_id                                          AS ACCOUNT_ID_SEMANTIC,
  A.fix_access.workorderid                              AS WORKORDERID,
  A.fix_access.provisioning_code                        AS PROVISIONING_CODE,
  A.fix_access.remote_id                                AS REMOTE_ID,
  A.fix_access.territoryowner                           AS TERRITORYOWNER,
  A.fix_access.gescal17                                 AS GESCAL17,
  A.fix_access.gescal37                                 AS GESCAL37,
  A.fix_access.footprint_owner                          AS FOOTPRINT_OWNER,
  A.fix_access.footprint_rank                           AS FOOTPRINT_RANK,
  B.router_model_id                                     AS ROUTER_MODEL_ID,
  B.router_model_alias                                  AS ROUTER_MODEL_ALIAS,
  B.router_model_wifi_type                              AS ROUTER_MODEL_WIFI_TYPE,
  B.provcode_id                                         AS PROVCODE_ID_ROUTER,
  B.workorder_id                                        AS WORKORDER_ID_ROUTER,
  B.remote_id                                           AS REMOTE_ID_ROUTER
FROM `mm-datamart-kd.SERVICES.semantic_kpi_services` A
LEFT JOIN `mo-migrations-reporting.STB_CPE_DESPO.gmm_router_ftth` B
  ON UPPER(A.fix_access.provisioning_code) = UPPER(B.provcode_id)
WHERE A.service_type = 'FIX'
  AND A.kpi = 'eop'
  AND A.period >= VAR_PERIOD_START;

-- ======================================
-- 4. CREACIÓN DE LA TABLA HISTÓRICA SI NO EXISTE 
-- ======================================

CREATE TABLE IF NOT EXISTS `mo-migrations-reporting.STB_CPE_DESPO.gmm_router_ftth_historical_serv` AS
SELECT DISTINCT
  A.*,
  DATE_TRUNC(PARSE_DATE('%Y%m%d', CAST(A.fecha_datos AS STRING)), MONTH)   AS MES,
  CAST(NULL AS INT64)                                                      AS SERVICE_ID_SEMANTIC,
  CAST(NULL AS INT64)                                                      AS ACCOUNT_ID_SEMANTIC,
  CAST(NULL AS STRING)                                                     AS WORKORDER_ID,
  CAST(NULL AS STRING)                                                     AS PROVISIONING_CODE,
  CAST(NULL AS STRING)                                                     AS REMOTE_ID,
  CAST(NULL AS STRING)                                                     AS TERRITORYOWNER,
  CAST(NULL AS STRING)                                                     AS GESCAL37,  
  CAST(NULL AS STRING)                                                     AS FOOTPRINT_OWNER,
  CAST(NULL AS STRING)                                                     AS FOOTPRINT_RANK,
  CAST(NULL AS STRING)                                                     AS ROUTER_MODEL_ID,
  CAST(NULL AS STRING)                                                     AS ROUTER_MODEL_ALIAS,
  CAST(NULL AS STRING)                                                     AS ROUTER_MODEL_WIFI_TYPE,
  CAST(NULL AS STRING)                                                     AS PROVCODE_ID_ROUTER,
  CAST(NULL AS STRING)                                                     AS REMOTE_ID_ROUTER,
  CAST(NULL AS STRING)                                                     AS DECO_MODEL,
  CONCAT(A.BRAND_DS, A.CUSTOMER_ID, A.SERVICE_ID, A.PHONE_NM)              AS PK,
  CAST(NULL AS STRING)                                                     AS DEVICE_TYPE,
  CAST(NULL AS STRING)                                                     AS DEVICE_TYPE2
FROM `mm-corporate-reporting.FINANCE_BI.CUSTOMER_BASE_*` A
WHERE FALSE -- Indicamos Where FALSE como "seguro" de que no se va a insertar ningún registro en la creación de la tabla.
;

-- ======================================
-- BLOQUE OPCIONAL: BORRADO DE UN PERIODO CONCRETO (SÓLO SI SE NECESITA REEMPLAZAR)
-- ======================================
-- IMPORTANTE: Este bloque está comentado para evitar borrados accidentales
-- USAR SOLO SI PARA REEMPLAZAR COMPLETAMENTE LA CARGA DE UN MES ESPECÍFICO

-- DELETE FROM `mo-migrations-reporting.STB_CPE_DESPO.gmm_router_ftth_historical_serv`
-- WHERE MES BETWEEN DATE '2025-01-01' AND CURRENT_DATE()
-- ;  --Ajustar la fecha del MES a borrar

-- EN CASO DE INSERCIÓN DE NUEVO CAMPO DEBEREMOS REALIZAR UN DROP TABLE
-- DROP TABLE IF EXISTS `mo-migrations-reporting.STB_CPE_DESPO.gmm_router_ftth_historical_serv`;


-- ======================================
-- 5. CARGA DEL PERIODO ACTUAL PARA MERGE EN HISTÓRICO (GMM ENRIQUECIDO CON DECO_MODEL)
-- ======================================
CREATE OR REPLACE TABLE `mo-migrations-reporting.STB_CPE_DESPO.gmm_router_ftth_plus_serv` AS
SELECT DISTINCT
  A.*,
  DATE_TRUNC(PARSE_DATE('%Y%m%d', CAST(A.fecha_datos AS STRING)), MONTH)   AS MES,
  CAST(B.service_id_semantic            AS INT64)   AS SERVICE_ID_SEMANTIC,
  CAST(B.account_id_semantic            AS INT64)   AS ACCOUNT_ID_SEMANTIC,
  CAST(B.workorderid                    AS STRING)  AS WORKORDER_ID,
  CAST(B.provisioning_code              AS STRING)  AS PROVISIONING_CODE,
  CAST(B.remote_id                      AS STRING)  AS REMOTE_ID,
  CAST(B.territoryowner                 AS STRING)  AS TERRITORYOWNER,
  CAST(B.gescal37                       AS STRING)  AS GESCAL37,
  CAST(B.footprint_owner                AS STRING)  AS FOOTPRINT_OWNER,
  CAST(B.footprint_rank                 AS STRING)  AS FOOTPRINT_RANK,
  CAST(B.router_model_id                AS STRING)  AS ROUTER_MODEL_ID,
  CAST(B.router_model_alias             AS STRING)  AS ROUTER_MODEL_ALIAS,
  CAST(B.router_model_wifi_type         AS STRING)  AS ROUTER_MODEL_WIFI_TYPE,
  CAST(B.provcode_id_router             AS STRING)  AS PROVCODE_ID_ROUTER,
  CAST(B.remote_id_router               AS STRING)  AS REMOTE_ID_ROUTER,
  CAST(C.DECO_MODEL                     AS STRING)  AS DECO_MODEL,
  CONCAT(A.brand_ds, A.customer_id, A.service_id, A.phone_nm)              AS PK,
  CASE WHEN B.ROUTER_MODEL_ID IS NOT NULL THEN 'CPE' ELSE NULL END         AS DEVICE_TYPE,
  CASE WHEN C.DECO_MODEL IS NOT NULL THEN 'STB' ELSE NULL END              AS DEVICE_TYPE2
FROM `mm-corporate-reporting.FINANCE_BI.CUSTOMER_BASE_*` A
LEFT JOIN `mo-migrations-reporting.STB_CPE_DESPO.gmm_router_ftth_serv` B
  ON A.customer_id = B.customer_id
  AND A.phone_nm   = B.phone_nm
  AND A.brand_ds   = B.brand_ds
LEFT JOIN (
    SELECT DISTINCT customer_id, brand_ds, device_data.modelName AS DECO_MODEL
    FROM `mm-datamart-kd.QOS.rep_cx_tv_gmm`
    WHERE data_day_dt BETWEEN VAR_PERIOD_START AND VAR_PERIOD_END
      AND customer_id IS NOT NULL
      AND device_data.modelName IS NOT NULL
) C
  ON A.customer_id = C.customer_id
  AND A.brand_ds = C.brand_ds
WHERE PARSE_DATE('%Y%m%d', CAST(A.fecha_datos AS STRING)) BETWEEN VAR_PERIOD_START AND VAR_PERIOD_END
  AND A.daily = TRUE
  AND A.billing_type = 'POSTPAID'
  AND A.service_type = 'BROADBAND'
  AND A.brand_ds IN ('YOIGO', 'MASMOVIL', 'GUUK', 'PEPEPEHONE', 'OPERADORES', 'LLAMAYA')
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY CONCAT(A.brand_ds, A.customer_id, A.service_id, A.phone_nm),
               DATE_TRUNC(PARSE_DATE('%Y%m%d', CAST(A.fecha_datos AS STRING)), MONTH)
  ORDER BY A.fecha_datos DESC
) = 1;


-- ======================================
-- 6. MERGE EN LA TABLA HISTÓRICA
-- ======================================
MERGE INTO `mo-migrations-reporting.STB_CPE_DESPO.gmm_router_ftth_historical_serv` T
USING (
        SELECT * FROM `mo-migrations-reporting.STB_CPE_DESPO.gmm_router_ftth_plus_serv` 
       -- WHERE MES BETWEEN VAR_PERIOD_START AND VAR_PERIOD_END -- Se usa para la carga inicial de datos
        WHERE MES = VAR_PERIOD_ACT --Se usa de manera permanente una vez se han cargaron los datos inciales. Solo actualiza mes en curso.
      ) S
ON T.PK = S.PK AND T.MES = S.MES
WHEN MATCHED THEN
  UPDATE SET
    T.SERVICE_ID_SEMANTIC      = S.SERVICE_ID_SEMANTIC,
    T.ACCOUNT_ID_SEMANTIC      = S.ACCOUNT_ID_SEMANTIC,
    T.WORKORDER_ID             = S.WORKORDER_ID,
    T.PROVISIONING_CODE        = S.PROVISIONING_CODE,
    T.REMOTE_ID                = S.REMOTE_ID,
    T.TERRITORYOWNER           = S.TERRITORYOWNER,
    T.GESCAL37                 = S.GESCAL37,
    T.FOOTPRINT_OWNER          = S.FOOTPRINT_OWNER,
    T.FOOTPRINT_RANK           = S.FOOTPRINT_RANK,
    T.ROUTER_MODEL_ID          = S.ROUTER_MODEL_ID,
    T.ROUTER_MODEL_ALIAS       = S.ROUTER_MODEL_ALIAS,
    T.ROUTER_MODEL_WIFI_TYPE   = S.ROUTER_MODEL_WIFI_TYPE,
    T.PROVCODE_ID_ROUTER       = S.PROVCODE_ID_ROUTER,
    T.REMOTE_ID_ROUTER         = S.REMOTE_ID_ROUTER,
    T.DECO_MODEL               = S.DECO_MODEL,
    T.DEVICE_TYPE              = S.DEVICE_TYPE,
    T.DEVICE_TYPE2             = S.DEVICE_TYPE2
WHEN NOT MATCHED THEN
  INSERT ROW;


-- ======================================
-- 7. LOG DE CONTROL: CUÁNTOS REGISTROS NUEVOS SE PROCESARON EN EL PERIODO
-- ======================================
-- SELECT
--   COUNT(*) AS REGISTROS_INSERTADOS,
--   VAR_PERIOD_START AS PERIODO_INICIO,
--   VAR_PERIOD_END   AS PERIODO_FIN
-- FROM `mo-migrations-reporting.STB_CPE_DESPO.temp_router_ftth_plus_serv_gmm_periodo`
-- ;




-- ==========================================================================================
-- SCRIPT COMPLETO: Carga histórica e incremental de cartera KRTV enriquecida (routers + servicios)
-- OBJETIVO: Cargar históricos desde 2025-01-01 y permitir luego actualización diaria por mes
-- ==========================================================================================

-- ======================================
-- 1. CARGA DE ROUTERS ACTIVOS DESDE 2025-01-01
-- ======================================
CREATE OR REPLACE TABLE `mo-migrations-reporting.STB_CPE_DESPO.krtv_router_ftth` AS
SELECT DISTINCT
  DATE_TRUNC(data_day_dt, MONTH)                         AS MES,
  brand_ds                                               AS BRAND_DS,
  customer_id                                            AS CUSTOMER_ID,
  service_id                                             AS SERVICE_ID,
  IUA                                                    AS IUA,
  conexion_data.model                                    AS ROUTER_MODEL_ID,
  provcode_id                                            AS PROVCODE_ID,
  remoteId                                               AS REMOTE_ID
FROM `mm-datamart-kd.QOS.rep_det_router_ftth_krt`
WHERE data_day_dt >= VAR_PERIOD_START
;

-- ======================================
-- 2. CARGA DE DESCO ACTIVOS DESDE 2025-01-01 
-- Esta tabla no se usa directamente en el proceso, el LEFT JOIN está en el paso 5
-- ======================================
CREATE OR REPLACE TABLE `mo-migrations-reporting.STB_CPE_DESPO.krtv_deco_model` AS
SELECT DISTINCT
  customer_id,
  brand_ds,
  deco_model AS DECO_MODEL
FROM `mm-datamart-kd.QOS.rep_cx_tv_krt`
WHERE data_day_dt BETWEEN VAR_PERIOD_START AND VAR_PERIOD_END
  AND customer_id IS NOT NULL
  AND deco_model IS NOT NULL
;

-- ======================================
-- 3. KPI SERVICES CRUZADO CON ROUTERS ACTIVOS KRTV
-- ======================================
CREATE OR REPLACE TABLE `mo-migrations-reporting.STB_CPE_DESPO.krtv_router_ftth_serv` AS
SELECT DISTINCT
  A.brand_ds                                            AS BRAND_DS,
  A.customer_id                                         AS CUSTOMER_ID,
  A.phone_nm                                            AS PHONE_NM,
  A.service_id                                          AS SERVICE_ID_SEMANTIC,
  A.account_id                                          AS ACCOUNT_ID_SEMANTIC,
  A.fix_access.workorderid                              AS WORKORDERID,
  A.fix_access.provisioning_code                        AS PROVISIONING_CODE,
  A.fix_access.remote_id                                AS REMOTE_ID,
  A.fix_access.territoryowner                           AS TERRITORYOWNER,
  A.fix_access.gescal17                                 AS GESCAL17,
  A.fix_access.gescal37                                 AS GESCAL37,
  A.fix_access.footprint_owner                          AS FOOTPRINT_OWNER,
  A.fix_access.footprint_rank                           AS FOOTPRINT_RANK,
  B.router_model_id                                     AS ROUTER_MODEL_ID,
  B.provcode_id                                         AS PROVCODE_ID_ROUTER,
  B.remote_id                                           AS REMOTE_ID_ROUTER
FROM `mm-datamart-kd.SERVICES.semantic_kpi_services` A
LEFT JOIN `mo-migrations-reporting.STB_CPE_DESPO.krtv_router_ftth` B
  ON UPPER(A.fix_access.provisioning_code) = UPPER(B.provcode_id)
     AND UPPER(A.fix_access.iua) = UPPER(B.IUA)
WHERE A.service_type = 'FIX'
  AND A.kpi = 'eop'
  AND A.period >= VAR_PERIOD_START
  AND A.brand_ds IN ('VIRGIN','R','EUSKALTEL','RACC','TELECABLE')
;

-- ======================================
-- 4. CREACIÓN DE LA TABLA HISTÓRICA SI NO EXISTE
-- ======================================
CREATE TABLE IF NOT EXISTS `mo-migrations-reporting.STB_CPE_DESPO.krtv_router_ftth_historical_serv` AS
SELECT DISTINCT
  A.*,
  DATE_TRUNC(PARSE_DATE('%Y%m%d', CAST(A.fecha_datos AS STRING)), MONTH)  AS MES,
  CAST(NULL AS INT64)                                                     AS SERVICE_ID_SEMANTIC,
  CAST(NULL AS INT64)                                                     AS ACCOUNT_ID_SEMANTIC,
  CAST(NULL AS STRING)                                                    AS WORKORDER_ID,
  CAST(NULL AS STRING)                                                    AS PROVISIONING_CODE,
  CAST(NULL AS STRING)                                                    AS REMOTE_ID,
  CAST(NULL AS STRING)                                                    AS TERRITORYOWNER,
  CAST(NULL AS STRING)                                                    AS GESCAL37,
  CAST(NULL AS STRING)                                                    AS FOOTPRINT_OWNER,
  CAST(NULL AS STRING)                                                    AS FOOTPRINT_RANK,
  CAST(NULL AS STRING)                                                    AS ROUTER_MODEL_ID,
  CAST(NULL AS STRING)                                                    AS ROUTER_MODEL_ALIAS,
  CAST(NULL AS STRING)                                                    AS ROUTER_MODEL_WIFI_TYPE,
  CAST(NULL AS STRING)                                                    AS PROVCODE_ID_ROUTER,
  CAST(NULL AS STRING)                                                    AS REMOTE_ID_ROUTER,
  CAST(NULL AS STRING)                                                    AS DECO_MODEL,
  CONCAT(A.BRAND_DS, A.CUSTOMER_ID, A.SERVICE_ID, A.PHONE_NM)             AS PK,
  CAST(NULL AS STRING)                                                    AS DEVICE_TYPE,
  CAST(NULL AS STRING)                                                    AS DEVICE_TYPE2
FROM `mm-corporate-reporting.FINANCE_BI.CUSTOMER_BASE_*` A
WHERE FALSE;


-- ======================================
-- BLOQUE OPCIONAL: BORRADO DE UN PERIODO CONCRETO (SÓLO SI SE NECESITA REEMPLAZAR)
-- ======================================
-- IMPORTANTE: Este bloque está comentado para evitar borrados accidentales
-- USAR SOLO SI PARA REEMPLAZAR COMPLETAMENTE LA CARGA DE UN MES ESPECÍFICO

-- DELETE FROM `mo-migrations-reporting.STB_CPE_DESPO.krtv_router_ftth_historical_serv`
-- WHERE MES BETWEEN DATE '2025-01-01' AND DATE '2025-03-01'
-- ;  --Ajustar la fecha del MES a borrar

-- EN CASO DE INSERCIÓN DE NUEVO CAMPO DEBEREMOS REALIZAR UN DROP TABLE
-- DROP TABLE IF EXISTS `mo-migrations-reporting.STB_CPE_DESPO.krtv_router_ftth_historical_serv`;


-- ======================================
-- 5. CARGA DEL PERIODO ACTUAL PARA MERGE EN HISTÓRICO (KRTV ENRIQUECIDO CON DECO_MODEL)
-- ======================================
CREATE OR REPLACE TABLE `mo-migrations-reporting.STB_CPE_DESPO.krtv_router_ftth_plus_serv` AS
SELECT DISTINCT
  A.*,
  DATE_TRUNC(PARSE_DATE('%Y%m%d', CAST(A.fecha_datos AS STRING)), MONTH)   AS MES,
  CAST(B.service_id_semantic            AS INT64)                          AS SERVICE_ID_SEMANTIC,
  CAST(B.account_id_semantic            AS INT64)                          AS ACCOUNT_ID_SEMANTIC,
  CAST(B.workorderid                    AS STRING)                         AS WORKORDER_ID,
  CAST(B.provisioning_code              AS STRING)                         AS PROVISIONING_CODE,
  CAST(B.remote_id                      AS STRING)                         AS REMOTE_ID,
  CAST(B.territoryowner                 AS STRING)                         AS TERRITORYOWNER,
  CAST(B.gescal37                       AS STRING)                         AS GESCAL37,
  CAST(B.footprint_owner                AS STRING)                         AS FOOTPRINT_OWNER,
  CAST(B.footprint_rank                 AS STRING)                         AS FOOTPRINT_RANK,
  CAST(B.router_model_id                AS STRING)                         AS ROUTER_MODEL_ID,
  CAST(NULL AS STRING)                                                     AS ROUTER_MODEL_ALIAS,
  CAST(NULL AS STRING)                                                     AS ROUTER_MODEL_WIFI_TYPE,
  CAST(B.provcode_id_router             AS STRING)                         AS PROVCODE_ID_ROUTER,
  CAST(B.remote_id_router               AS STRING)                         AS REMOTE_ID_ROUTER,
  CAST(C.DECO_MODEL                     AS STRING)                         AS DECO_MODEL,
  CONCAT(A.BRAND_DS, A.CUSTOMER_ID, A.SERVICE_ID, A.PHONE_NM)              AS PK,
  CASE WHEN B.ROUTER_MODEL_ID IS NOT NULL THEN 'CPE' ELSE NULL END         AS DEVICE_TYPE,
  CASE WHEN C.DECO_MODEL IS NOT NULL THEN 'STB' ELSE NULL END              AS DEVICE_TYPE2
FROM `mm-corporate-reporting.FINANCE_BI.CUSTOMER_BASE_*` A
LEFT JOIN `mo-migrations-reporting.STB_CPE_DESPO.krtv_router_ftth_serv` B
  ON A.customer_id = B.customer_id
  AND A.phone_nm   = B.phone_nm
  AND A.brand_ds   = B.brand_ds
  AND A.service_id = B.service_id_semantic
LEFT JOIN (
    SELECT DISTINCT customer_id, brand_ds, deco_model AS DECO_MODEL
    FROM `mm-datamart-kd.QOS.rep_cx_tv_krt`
    WHERE data_day_dt BETWEEN VAR_PERIOD_START AND VAR_PERIOD_END
      AND customer_id IS NOT NULL
      AND deco_model IS NOT NULL
) C
  ON A.customer_id = C.customer_id
  AND A.brand_ds = C.brand_ds
WHERE PARSE_DATE('%Y%m%d', CAST(A.fecha_datos AS STRING)) BETWEEN VAR_PERIOD_START AND VAR_PERIOD_END
  AND A.daily = TRUE
  AND A.billing_type = 'POSTPAID'
  AND A.service_type = 'BROADBAND'
  AND A.brand_ds IN ('VIRGIN','R','EUSKALTEL','RACC','TELECABLE')
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY CONCAT(A.brand_ds, A.customer_id, A.service_id, A.phone_nm),
               DATE_TRUNC(PARSE_DATE('%Y%m%d', CAST(A.fecha_datos AS STRING)), MONTH)
  ORDER BY A.fecha_datos DESC
) = 1;


-- ======================================
-- 6. MERGE EN LA TABLA HISTÓRICA
-- ======================================
MERGE INTO `mo-migrations-reporting.STB_CPE_DESPO.krtv_router_ftth_historical_serv` T
USING (
        SELECT * FROM `mo-migrations-reporting.STB_CPE_DESPO.krtv_router_ftth_plus_serv` 
        --WHERE MES BETWEEN VAR_PERIOD_START AND VAR_PERIOD_END -- Se usa para la carga inicial de datos
        WHERE MES = VAR_PERIOD_ACT -- Se usa de manera permanente una vez se han cargaron los datos inciales. Solo actualiza mes en curso.
      ) S
ON T.PK = S.PK AND T.MES = S.MES
WHEN MATCHED THEN
  UPDATE SET
    T.SERVICE_ID_SEMANTIC      = S.SERVICE_ID_SEMANTIC,
    T.ACCOUNT_ID_SEMANTIC      = S.ACCOUNT_ID_SEMANTIC,
    T.WORKORDER_ID             = S.WORKORDER_ID,
    T.PROVISIONING_CODE        = S.PROVISIONING_CODE,
    T.REMOTE_ID                = S.REMOTE_ID,
    T.TERRITORYOWNER           = S.TERRITORYOWNER,
    T.GESCAL37                 = S.GESCAL37,
    T.FOOTPRINT_OWNER          = S.FOOTPRINT_OWNER,
    T.FOOTPRINT_RANK           = S.FOOTPRINT_RANK,
    T.ROUTER_MODEL_ID          = S.ROUTER_MODEL_ID,
    T.ROUTER_MODEL_ALIAS       = S.ROUTER_MODEL_ALIAS,
    T.ROUTER_MODEL_WIFI_TYPE   = S.ROUTER_MODEL_WIFI_TYPE,
    T.PROVCODE_ID_ROUTER       = S.PROVCODE_ID_ROUTER,
    T.REMOTE_ID_ROUTER         = S.REMOTE_ID_ROUTER,
    T.DECO_MODEL               = S.DECO_MODEL,
    T.DEVICE_TYPE              = S.DEVICE_TYPE,
    T.DEVICE_TYPE2             = S.DEVICE_TYPE2
WHEN NOT MATCHED THEN
  INSERT ROW;

-- ======================================
-- 6. LOG DE CONTROL: CUÁNTOS REGISTROS NUEVOS SE PROCESARON EN EL PERIODO
-- ======================================
-- SELECT
--   COUNT(*) AS REGISTROS_INSERTADOS,
--   VAR_PERIOD_START AS PERIODO_INICIO,
--   VAR_PERIOD_END   AS PERIODO_FIN
-- FROM `mo-migrations-reporting.STB_CPE_DESPO.temp_router_ftth_plus_serv_krtv_periodo`
-- ;



-- ======================================
-- 7. TABLA FINAL UNIFICADA
-- ======================================

CREATE OR REPLACE TABLE `mo-migrations-reporting.STB_CPE_DESPO.gmmkrtv_router_ftth_historical_serv` AS
SELECT * FROM `mo-migrations-reporting.STB_CPE_DESPO.gmm_router_ftth_historical_serv`
UNION ALL
SELECT * FROM `mo-migrations-reporting.STB_CPE_DESPO.krtv_router_ftth_historical_serv`
;


/*
¿Cuál es la diferencia entre MERGE y DELETE + INSERT INTO?

-- ==========================================================================================
-- DIFERENCIA ENTRE MERGE vs DELETE + INSERT INTO
-- ==========================================================================================

-- | Característica                        | MERGE                                       | DELETE + INSERT INTO                      |
-- |--------------------------------------|---------------------------------------------|-------------------------------------------|
-- | Actualiza solo lo necesario?          | Sí, solo cambia lo nuevo o actualizado      | Elimina todo y vuelve a insertar todo     |
-- | Conserva registros no afectados?     | Sí, si no hay match, no los toca            | Todo se borra, aunque no haya cambios     |
-- | Más eficiente en tablas grandes?     | Evita reescribir datos sin cambios          | Reescribe completamente toda la tabla     |
-- | Evita duplicados?                    | Usa PK o condiciones para evitarlo          | Riesgo si no hay limpieza previa          |
-- | Recomendado para histórico?           | Ideal para cargas incrementales             | Menos control sobre versiones previas     |
-- | Mayor control sobre los cambios?      | Puedes hacer UPDATE, INSERT y DELETE        | Menos flexible, todo se sustituye         |

-- RECOMENDACIÓN:
-- Usar MERGE cuando se quiera mantener histórico incremental y actualizar solo lo necesario.
-- Usa DELETE + INSERT solo si necesitas reemplazar completamente un periodo específico.
-- ==========================================================================================
*/
