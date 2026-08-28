/* -------------------------------------------------------------------------
 *
 * ammgranule.h
 *      AMM AP grant-backed MemoryContext.
 *
 * -------------------------------------------------------------------------
 */
#ifndef AMMGRANULE_H
#define AMMGRANULE_H

#include "storage/gs_amm_types.h"
#include "utils/memutils.h"

typedef struct AmmGranuleContextStatsData {
    uint64 grant_id;
    Size max_bytes;
    Size allocated_bytes;
    Size live_bytes;
    Size total_bytes;
    Size free_bytes;
} AmmGranuleContextStatsData;

extern MemoryContext AmmGranuleContextCreate(MemoryContext parent, const char *name, uint64 grant_id, Size max_bytes);
extern bool AmmGranuleContextStats(MemoryContext context, AmmGranuleContextStatsData *stats);
extern bool AmmGranuleContextCanAllocate(MemoryContext context, Size size);
extern void *AmmGranuleContextTryAlloc(MemoryContext context, Size size);
extern Size AmmGranuleContextReleaseFreeMemory(MemoryContext context);
extern Size AmmGranuleContextReleaseFreeMemoryTree(MemoryContext context);
#ifdef MEMORY_CONTEXT_CHECKING
extern void AmmGranuleContextCheckPointer(MemoryContext context, void *pointer);
#endif

#endif /* AMMGRANULE_H */
