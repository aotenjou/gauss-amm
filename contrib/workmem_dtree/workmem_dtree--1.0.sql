/* contrib/workmem_dtree/workmem_dtree--1.0.sql */

\echo Use "CREATE EXTENSION workmem_dtree" to load this file. \quit

CREATE TYPE gs_workmem_dtree_detail AS (
    raw_cache_kb float8,
    raw_onepass_kb float8,
    raw_multipass_kb float8,
    calibrated_cache_kb float8,
    calibrated_onepass_kb float8,
    calibrated_multipass_kb float8,
    model_version int8,
    leaf_id int8,
    calibration_version int8,
    calibration_scale float8
);

CREATE FUNCTION gs_workmem_dtree_predict(float8[])
RETURNS float8[]
AS 'MODULE_PATHNAME', 'gs_workmem_dtree_predict'
LANGUAGE C STRICT IMMUTABLE NOT FENCED;

CREATE FUNCTION gs_workmem_dtree_predict_detail(float8[])
RETURNS gs_workmem_dtree_detail
AS 'MODULE_PATHNAME', 'gs_workmem_dtree_predict_detail'
LANGUAGE C STRICT STABLE NOT FENCED;

COMMENT ON FUNCTION gs_workmem_dtree_predict(float8[]) IS
'Predict [cache_mb, one_pass_mb, multi_pass_mb] from the 19-feature work_mem distillation ABI.';

COMMENT ON FUNCTION gs_workmem_dtree_predict_detail(float8[]) IS
'Predict raw and calibrated work_mem bounds plus calibration metadata from the 19-feature work_mem distillation ABI.';
