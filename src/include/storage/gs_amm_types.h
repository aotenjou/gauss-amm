/* -------------------------------------------------------------------------
 *
 * gs_amm_types.h
 *      Low-level AMM ownership tokens shared by storage and memory contexts.
 *
 * -------------------------------------------------------------------------
 */
#ifndef GS_AMM_TYPES_H
#define GS_AMM_TYPES_H

#include "c.h"

typedef struct GsAmmGrantToken {
    uint64 grant_id;
    uint64 grant_generation;
} GsAmmGrantToken;

#endif /* GS_AMM_TYPES_H */
