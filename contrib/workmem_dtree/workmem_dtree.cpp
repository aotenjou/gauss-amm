/*
 * contrib/workmem_dtree/workmem_dtree.cpp
 *
 * SQL-callable wrapper for the generated work_mem decision-tree model.
 */
#include "postgres.h"

#include "catalog/pg_type.h"
#include "funcapi.h"
#include "fmgr.h"
#include "storage/gs_amm.h"
#include "utils/array.h"

#include "workmem_model.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(gs_workmem_dtree_predict);
PG_FUNCTION_INFO_V1(gs_workmem_dtree_predict_detail);
extern "C" Datum gs_workmem_dtree_predict(PG_FUNCTION_ARGS);
extern "C" Datum gs_workmem_dtree_predict_detail(PG_FUNCTION_ARGS);

static void
extract_float8_features(ArrayType *feature_array, double features[MEMTUNE_WORKMEM_FEATURE_COUNT])
{
    Datum *datums = NULL;
    bool *nulls = NULL;
    int count = 0;
    int index;

    if (ARR_NDIM(feature_array) != 1 || ARR_ELEMTYPE(feature_array) != FLOAT8OID) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("expected one-dimensional float8[] with %d elements", MEMTUNE_WORKMEM_FEATURE_COUNT)));
    }

    deconstruct_array(feature_array, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd', &datums, &nulls, &count);
    if (count != MEMTUNE_WORKMEM_FEATURE_COUNT) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("expected %d work_mem decision-tree features, got %d",
                    MEMTUNE_WORKMEM_FEATURE_COUNT, count)));
    }

    for (index = 0; index < count; ++index) {
        if (nulls[index]) {
            ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                    errmsg("work_mem decision-tree feature %d is null", index)));
        }
        features[index] = DatumGetFloat8(datums[index]);
    }
}

static void
detail_from_prediction(
    const MemTuneWorkMemPrediction *prediction,
    GsAmmDtreeDetail *detail)
{
    int index;
    double raw_bounds_mb[GS_AMM_DTREE_BOUND_COUNT] = {
        prediction->bounds.cache_mb,
        prediction->bounds.one_pass_mb,
        prediction->bounds.multi_pass_mb
    };

    gs_amm_dtree_detail(raw_bounds_mb, prediction->model_version, prediction->leaf_id, detail);
    for (index = 0; index < GS_AMM_DTREE_BOUND_COUNT; ++index) {
        detail->raw_bounds_kb[index] = raw_bounds_mb[index] * 1024.0;
    }
}

static HeapTuple
form_detail_tuple(FunctionCallInfo fcinfo, const GsAmmDtreeDetail *detail)
{
    TupleDesc tupdesc;
    Datum values[10];
    bool nulls[10] = {false};
    HeapTuple tuple;

    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("detail function requires composite return type")));
    tupdesc = BlessTupleDesc(tupdesc);

    values[0] = Float8GetDatum(detail->raw_bounds_kb[0]);
    values[1] = Float8GetDatum(detail->raw_bounds_kb[1]);
    values[2] = Float8GetDatum(detail->raw_bounds_kb[2]);
    values[3] = Float8GetDatum(detail->calibrated_bounds_kb[0]);
    values[4] = Float8GetDatum(detail->calibrated_bounds_kb[1]);
    values[5] = Float8GetDatum(detail->calibrated_bounds_kb[2]);
    values[6] = Int64GetDatum(detail->model_version);
    values[7] = Int64GetDatum(detail->leaf_id);
    values[8] = Int64GetDatum(detail->calibration_version);
    values[9] = Float8GetDatum(detail->calibration_scale);

    tuple = heap_form_tuple(tupdesc, values, nulls);
    return tuple;
}

Datum
gs_workmem_dtree_predict(PG_FUNCTION_ARGS)
{
    ArrayType *feature_array = PG_GETARG_ARRAYTYPE_P(0);
    double features[MEMTUNE_WORKMEM_FEATURE_COUNT];
    Datum result_values[3];
    ArrayType *result = NULL;
    MemTuneWorkMemBounds bounds;

    extract_float8_features(feature_array, features);
    bounds = memtune_predict_workmem_bounds(features);

    result_values[0] = Float8GetDatum(bounds.cache_mb);
    result_values[1] = Float8GetDatum(bounds.one_pass_mb);
    result_values[2] = Float8GetDatum(bounds.multi_pass_mb);
    result = construct_array(result_values, 3, FLOAT8OID, sizeof(float8), FLOAT8PASSBYVAL, 'd');

    PG_RETURN_POINTER(result);
}

Datum
gs_workmem_dtree_predict_detail(PG_FUNCTION_ARGS)
{
    ArrayType *feature_array = PG_GETARG_ARRAYTYPE_P(0);
    double features[MEMTUNE_WORKMEM_FEATURE_COUNT];
    MemTuneWorkMemPrediction prediction;
    GsAmmDtreeDetail detail;
    HeapTuple tuple;

    extract_float8_features(feature_array, features);
    prediction = memtune_predict_workmem_detail(features);
    detail_from_prediction(&prediction, &detail);
    tuple = form_detail_tuple(fcinfo, &detail);

    PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}
