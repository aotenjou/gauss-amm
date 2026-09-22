/* -------------------------------------------------------------------------
 *
 * ammgranule.cpp
 *      AMM AP grant-backed MemoryContext implementation.
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "storage/gs_amm.h"
#include "utils/ammgranule.h"
#include "utils/memutils.h"

typedef struct AmmGranuleChunkData {
    struct AmmGranuleChunkData* next;
    struct AmmGranuleChunkData* prev;
    struct AmmGranuleChunkData* free_next;
    Size total_size;
    Size payload_size;
    bool is_free;
    bool dynamic_memory;
} AmmGranuleChunkData;

typedef AmmGranuleChunkData* AmmGranuleChunk;
typedef AmmGranuleChunkData AmmGranuleBlock;

static uint64 AmmGranuleContextCurrentCapacity(AmmGranuleContextPtr context)
{
    uint64 pool;
    uint64 limit;
    uint64 capacity;

    if (context == NULL)
        return 0;

    /*
     * The AP grant capacity visible to an operator must be constrained by
     * both the physical pool (granules + dynamic quota currently owned by
     * this backend) and the controller's current effective grant limit.
     * A TP-driven downgrade lowers only record->effective_grant_kb, so
     * taking Min(pool, limit) is what turns a revoke into a hard capacity
     * drop that makes sort/hash spill and release granules.
     */
    pool = GsAmmCurrentBackendGrantPoolBytes();
    limit = GsAmmGrantEffectiveMemoryLimit(context->grant_token.grant_id, context->maxBytes);
    if (pool == 0)
        capacity = limit;
    else if (limit == 0)
        capacity = pool;
    else
        capacity = Min(pool, limit);
    if (capacity == 0)
        capacity = context->maxBytes;

    /*
     * A TP-driven revoke must become visible as a hard allocation stop even
     * when record->effective_grant_kb is not below the current pool (for
     * example a multipass grant already sized below its one-pass bound).
     * While this backend's reclaim is pending, cap capacity at liveBytes so
     * any new tuple allocation fails; the operator then spills, releases
     * free chunks, and confirms the reclaim via GsAmmGrantProcessPendingReclaim.
     * Do not store this transient cap in context->maxBytes: maxBytes remains
     * the grant request used for GsAmmGrantEffectiveMemoryLimit().
     */
    if (GsAmmGrantReclaimPending() && context->liveBytes < capacity)
        capacity = context->liveBytes;

    context->set.maxSpaceSize = capacity;
    return capacity;
}

#define AMMGRANULE_CHUNKHDRSZ MAXALIGN(sizeof(AmmGranuleChunkData))
#define AmmGranulePointerGetStandardHeader(pointer) \
    ((StandardChunkHeader*)((char*)(pointer) - STANDARDCHUNKHEADERSIZE))
#define AmmGranulePointerGetChunk(pointer) \
    ((AmmGranuleChunk)((char*)AmmGranulePointerGetStandardHeader(pointer) - AMMGRANULE_CHUNKHDRSZ))
#define AmmGranuleChunkGetStandardHeader(chunk) \
    ((StandardChunkHeader*)((char*)(chunk) + AMMGRANULE_CHUNKHDRSZ))
#define AmmGranuleChunkGetPointer(chunk) \
    ((void*)((char*)AmmGranuleChunkGetStandardHeader(chunk) + STANDARDCHUNKHEADERSIZE))

static void* AmmGranuleAlloc(MemoryContext context, Size align, Size size, const char* file, int line);
static void AmmGranuleFree(MemoryContext context, void* pointer);
static void* AmmGranuleRealloc(MemoryContext context, void* pointer, Size align, Size size, const char* file, int line);
static void AmmGranuleInit(MemoryContext context);
static void AmmGranuleReset(MemoryContext context);
static void AmmGranuleDelete(MemoryContext context);
static Size AmmGranuleGetChunkSpace(MemoryContext context, void* pointer);
static bool AmmGranuleIsEmpty(MemoryContext context);
static void AmmGranuleStats(MemoryContext context, int level);
static Size AmmGranuleChunkTotalSize(Size payload_size);
#ifdef MEMORY_CONTEXT_CHECKING
static void AmmGranuleCheck(MemoryContext context);
#endif

static void AmmGranuleSetMethods(MemoryContextMethods* methods)
{
    methods->alloc = AmmGranuleAlloc;
    methods->free_p = AmmGranuleFree;
    methods->realloc = AmmGranuleRealloc;
    methods->init = AmmGranuleInit;
    methods->reset = AmmGranuleReset;
    methods->delete_context = AmmGranuleDelete;
    methods->get_chunk_space = AmmGranuleGetChunkSpace;
    methods->is_empty = AmmGranuleIsEmpty;
    methods->stats = AmmGranuleStats;
#ifdef MEMORY_CONTEXT_CHECKING
    methods->check = AmmGranuleCheck;
#endif
}

static int AmmGranuleFreeIndex(Size payload_size)
{
    int index = 0;
    Size class_size = MAXALIGN(8);

    payload_size = Max(payload_size, (Size)MAXALIGN(sizeof(void*)));
    while (index < AMMGRANULE_NUM_FREELISTS - 1 && class_size < payload_size) {
        class_size <<= 1;
        index++;
    }
    return index;
}

static void AmmGranuleLinkChunk(AmmGranuleContextPtr context, AmmGranuleChunk chunk)
{
    chunk->prev = NULL;
    chunk->next = (AmmGranuleChunk)context->chunks;
    if (chunk->next != NULL)
        chunk->next->prev = chunk;
    context->chunks = chunk;
}

static void AmmGranulePushFreeChunk(AmmGranuleContextPtr context, AmmGranuleChunk chunk)
{
    int index = AmmGranuleFreeIndex(chunk->payload_size);

    chunk->is_free = true;
    chunk->free_next = (AmmGranuleChunk)context->freelist[index];
    context->freelist[index] = chunk;
}

static AmmGranuleChunk AmmGranulePopFreeChunk(AmmGranuleContextPtr context, Size payload_size)
{
    int start = AmmGranuleFreeIndex(payload_size);

    for (int index = start; index < AMMGRANULE_NUM_FREELISTS; index++) {
        AmmGranuleChunk previous = NULL;

        for (AmmGranuleChunk chunk = (AmmGranuleChunk)context->freelist[index]; chunk != NULL;
             chunk = chunk->free_next) {
            if (chunk->payload_size < payload_size) {
                previous = chunk;
                continue;
            }
            if (previous == NULL)
                context->freelist[index] = chunk->free_next;
            else
                previous->free_next = chunk->free_next;
            chunk->free_next = NULL;
            chunk->is_free = false;
            return chunk;
        }
    }
    return NULL;
}

static AmmGranuleChunk AmmGranuleSplitFreeChunk(
    AmmGranuleContextPtr context, AmmGranuleChunk chunk, Size payload_size)
{
    Size allocated_total_size = AmmGranuleChunkTotalSize(payload_size);
    Size minimum_tail_size = AmmGranuleChunkTotalSize(MAXALIGN(sizeof(void*)));
    AmmGranuleChunk tail;

    if (chunk->total_size < allocated_total_size + minimum_tail_size)
        return chunk;

    tail = (AmmGranuleChunk)((char*)chunk + allocated_total_size);
    tail->next = chunk->next;
    tail->prev = chunk;
    if (tail->next != NULL)
        tail->next->prev = tail;
    tail->free_next = NULL;
    tail->total_size = chunk->total_size - allocated_total_size;
    tail->payload_size = tail->total_size - AMMGRANULE_CHUNKHDRSZ - STANDARDCHUNKHEADERSIZE;
    tail->is_free = false;
    tail->dynamic_memory = chunk->dynamic_memory;
    chunk->next = tail;
    chunk->total_size = allocated_total_size;
    chunk->payload_size = payload_size;
    AmmGranulePushFreeChunk(context, tail);

    return chunk;
}

static bool AmmGranuleHasFreeChunk(AmmGranuleContextPtr context, Size payload_size)
{
    int start = AmmGranuleFreeIndex(payload_size);

    for (int index = start; index < AMMGRANULE_NUM_FREELISTS; index++) {
        for (AmmGranuleChunk chunk = (AmmGranuleChunk)context->freelist[index]; chunk != NULL;
             chunk = chunk->free_next) {
            if (chunk->payload_size >= payload_size)
                return true;
        }
    }
    return false;
}

static Size AmmGranuleChunkTotalSize(Size payload_size)
{
    payload_size = Max(MAXALIGN(payload_size), (Size)MAXALIGN(sizeof(void*)));
    return AMMGRANULE_CHUNKHDRSZ + STANDARDCHUNKHEADERSIZE + payload_size;
}

MemoryContext AmmGranuleContextCreate(MemoryContext parent, const char *name, uint64 grant_id, Size max_bytes)
{
    AmmGranuleContextPtr context;
    uint64 grant_generation = GsAmmCurrentBackendGrantGeneration();

    if (grant_id == 0 || max_bytes == 0 || grant_id != GsAmmCurrentBackendGrantId() || grant_generation == 0)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("cannot create AMM granule context without an active AMM grant")));

    context = (AmmGranuleContextPtr)MemoryContextCreate(
        T_AmmGranuleContext, sizeof(AmmGranuleContext), parent, name, __FILE__, __LINE__);
    AmmGranuleSetMethods(((MemoryContext)context)->methods);

    context->set.maxSpaceSize = max_bytes;
    context->set.totalSpace = 0;
    context->set.freeSpace = 0;
    context->set.track = NULL;
    context->grant_token.grant_id = grant_id;
    context->grant_token.grant_generation = grant_generation;
    context->maxBytes = max_bytes;
    context->allocatedBytes = 0;
    context->liveBytes = 0;
    context->chunks = NULL;
    errno_t rc = memset_s(context->freelist, sizeof(context->freelist), 0, sizeof(context->freelist));
    securec_check(rc, "\0", "\0");

    return (MemoryContext)context;
}

bool AmmGranuleContextStats(MemoryContext memory_context, AmmGranuleContextStatsData *stats)
{
    AmmGranuleContextPtr context = (AmmGranuleContextPtr)memory_context;

    if (stats == NULL || memory_context == NULL || !IsA(memory_context, AmmGranuleContext))
        return false;

    stats->grant_id = context->grant_token.grant_id;
    stats->max_bytes = context->maxBytes;
    stats->allocated_bytes = context->allocatedBytes;
    stats->live_bytes = context->liveBytes;
    stats->total_bytes = context->set.totalSpace;
    stats->free_bytes = context->set.freeSpace;
    return true;
}

Size AmmGranuleContextReleaseFreeMemory(MemoryContext memory_context)
{
    AmmGranuleContextPtr context = (AmmGranuleContextPtr)memory_context;
    AmmGranuleChunk chunk;
    Size released_bytes = 0;
    errno_t rc;

    if (memory_context == NULL || !IsA(memory_context, AmmGranuleContext))
        return 0;
    if (!GsAmmGrantTokenIsValid(context->grant_token))
        return 0;

    /* Rebuild after the scan so no freelist entry can retain a returned chunk. */
    rc = memset_s(context->freelist, sizeof(context->freelist), 0, sizeof(context->freelist));
    securec_check(rc, "\0", "\0");

    for (chunk = (AmmGranuleChunk)context->chunks; chunk != NULL;) {
        AmmGranuleChunk next = chunk->next;
        AmmGranuleChunk previous = chunk->prev;
        Size chunk_bytes = chunk->total_size;

        if (!chunk->is_free) {
            chunk->free_next = NULL;
            chunk = next;
            continue;
        }

        if (previous == NULL)
            context->chunks = next;
        else
            previous->next = next;
        if (next != NULL)
            next->prev = previous;

        bool returned = chunk->dynamic_memory ?
            GsAmmGrantReturnDynamicMemory(context->grant_token, chunk, chunk_bytes) :
            GsAmmGrantReturnMemory(context->grant_token, chunk, chunk_bytes);
        if (!returned) {
            chunk->prev = previous;
            chunk->next = next;
            if (previous == NULL)
                context->chunks = chunk;
            else
                previous->next = chunk;
            if (next != NULL)
                next->prev = chunk;
            chunk = next;
            continue;
        }

        if (context->allocatedBytes >= chunk_bytes)
            context->allocatedBytes -= chunk_bytes;
        else
            context->allocatedBytes = 0;
        if (context->set.totalSpace >= chunk_bytes)
            context->set.totalSpace -= chunk_bytes;
        else
            context->set.totalSpace = 0;
        if (context->set.freeSpace >= chunk_bytes)
            context->set.freeSpace -= chunk_bytes;
        else
            context->set.freeSpace = 0;
        released_bytes += chunk_bytes;
        chunk = next;
    }

    for (chunk = (AmmGranuleChunk)context->chunks; chunk != NULL; chunk = chunk->next) {
        chunk->free_next = NULL;
        if (chunk->is_free)
            AmmGranulePushFreeChunk(context, chunk);
    }
    memory_context->isReset = context->liveBytes == 0;
    return released_bytes;
}

/*
 * Release free chunks in an AP query's context tree.  Sort and hash keep
 * their AMM contexts below the query context, so a backend reclaim poll can
 * walk the tree without knowing the operator-specific state layout.
 */
Size AmmGranuleContextReleaseFreeMemoryTree(MemoryContext memory_context)
{
    MemoryContext child;
    Size released_bytes = 0;

    if (memory_context == NULL)
        return 0;

    for (child = memory_context->firstchild; child != NULL; child = child->nextchild)
        released_bytes += AmmGranuleContextReleaseFreeMemoryTree(child);

    if (IsA(memory_context, AmmGranuleContext))
        released_bytes += AmmGranuleContextReleaseFreeMemory(memory_context);

    return released_bytes;
}

bool AmmGranuleContextCanAllocate(MemoryContext memory_context, Size size)
{
    AmmGranuleContextPtr context = (AmmGranuleContextPtr)memory_context;
    Size payload_size;
    Size total_size;
    uint64 effective_max_bytes;

    if (memory_context == NULL || !IsA(memory_context, AmmGranuleContext) || size == 0)
        return false;
    if (!GsAmmGrantTokenIsValid(context->grant_token))
        return false;

    payload_size = Max(MAXALIGN(size), (Size)MAXALIGN(sizeof(void*)));
    total_size = AmmGranuleChunkTotalSize(payload_size);
    effective_max_bytes = AmmGranuleContextCurrentCapacity(context);
    if (context->liveBytes + total_size > effective_max_bytes)
        return false;
    if (AmmGranuleHasFreeChunk(context, payload_size))
        return true;
    if (context->allocatedBytes + total_size > effective_max_bytes)
        return false;

    return GsAmmGrantCanAllocateMemory(context->grant_token, total_size);
}

void *AmmGranuleContextTryAlloc(MemoryContext memory_context, Size size)
{
    if (memory_context == NULL || !IsA(memory_context, AmmGranuleContext) || size == 0 || !AllocSizeIsValid(size))
        return NULL;
    return AmmGranuleAlloc(memory_context, 0, size, __FILE__, __LINE__);
}

#ifdef MEMORY_CONTEXT_CHECKING
void AmmGranuleContextCheckPointer(MemoryContext memory_context, void *pointer)
{
    AmmGranuleContextPtr context = (AmmGranuleContextPtr)memory_context;
    StandardChunkHeader *header = AmmGranulePointerGetStandardHeader(pointer);

    Assert(memory_context != NULL && IsA(memory_context, AmmGranuleContext));
    Assert(pointer != NULL && pointer == (void *)MAXALIGN(pointer));
    Assert(header->context == memory_context);
    Assert(header->requested_size <= header->size);

    for (AmmGranuleChunk chunk = (AmmGranuleChunk)context->chunks; chunk != NULL; chunk = chunk->next) {
        if (AmmGranuleChunkGetPointer(chunk) != pointer)
            continue;
        Assert(!chunk->is_free);
        Assert(header->size == chunk->payload_size);
        Assert(chunk->total_size == AmmGranuleChunkTotalSize(chunk->payload_size));
        return;
    }

    Assert(false);
}
#endif

static void* AmmGranuleAlloc(MemoryContext memory_context, Size align, Size size, const char* file, int line)
{
    AmmGranuleContextPtr context = (AmmGranuleContextPtr)memory_context;
    Size payload_size;
    Size total_size;
    uint64 effective_max_bytes;
    AmmGranuleChunk chunk;
    StandardChunkHeader* header;

    (void)file;
    (void)line;
    AssertArg(align == 0);

    if (!GsAmmGrantTokenIsValid(context->grant_token))
        return NULL;

    payload_size = Max(MAXALIGN(size), (Size)MAXALIGN(sizeof(void*)));
    total_size = AmmGranuleChunkTotalSize(payload_size);
    effective_max_bytes = AmmGranuleContextCurrentCapacity(context);
    if (context->liveBytes + total_size > effective_max_bytes)
        return NULL;

    chunk = AmmGranulePopFreeChunk(context, payload_size);
    if (chunk != NULL) {
        chunk = AmmGranuleSplitFreeChunk(context, chunk, payload_size);
        header = AmmGranuleChunkGetStandardHeader(chunk);
        header->context = memory_context;
        header->size = chunk->payload_size;
#ifdef MEMORY_CONTEXT_CHECKING
        header->requested_size = size;
#endif
#ifdef MEMORY_CONTEXT_TRACK
        header->file = file;
        header->line = line;
#endif
        if (chunk->dynamic_memory)
            GsAmmGrantAccountUsedDynamicMemory(context->grant_token, chunk->total_size);
        else
            GsAmmGrantAccountUsedMemory(context->grant_token, chunk, chunk->total_size);
        context->liveBytes += chunk->total_size;
        context->set.freeSpace -= chunk->total_size;
        memory_context->isReset = false;
        return AmmGranuleChunkGetPointer(chunk);
    }

    if (context->allocatedBytes + total_size > effective_max_bytes)
        return NULL;

    chunk = NULL;
    if (GsAmmGrantDynamicMemoryAvailable(context->grant_token, total_size)) {
        chunk = (AmmGranuleChunk)GsAmmGrantAllocDynamicMemory(context->grant_token, total_size);
        if (chunk == NULL)
            return NULL;
        chunk->dynamic_memory = true;
    } else {
        chunk = (AmmGranuleChunk)GsAmmGrantAllocMemory(context->grant_token, total_size);
        if (chunk == NULL)
            return NULL;
        chunk->dynamic_memory = false;
    }
    if (chunk == NULL)
        return NULL;

    chunk->free_next = NULL;
    chunk->total_size = total_size;
    chunk->payload_size = payload_size;
    chunk->is_free = false;
    AmmGranuleLinkChunk(context, chunk);

    header = AmmGranuleChunkGetStandardHeader(chunk);
    header->context = memory_context;
    header->size = payload_size;
#ifdef MEMORY_CONTEXT_CHECKING
    header->requested_size = size;
#endif
#ifdef MEMORY_CONTEXT_TRACK
    header->file = file;
    header->line = line;
#endif

    context->allocatedBytes += total_size;
    context->liveBytes += total_size;
    context->set.totalSpace += total_size;
    memory_context->isReset = false;
    return AmmGranuleChunkGetPointer(chunk);
}

static void AmmGranuleFree(MemoryContext memory_context, void* pointer)
{
    AmmGranuleContextPtr context = (AmmGranuleContextPtr)memory_context;
    AmmGranuleChunk chunk;

    if (!GsAmmGrantTokenIsValid(context->grant_token))
        return;
    chunk = AmmGranulePointerGetChunk(pointer);

    if (chunk->is_free)
        return;

    if (context->liveBytes >= chunk->total_size)
        context->liveBytes -= chunk->total_size;
    else
        context->liveBytes = 0;
    context->set.freeSpace += chunk->total_size;
    if (chunk->dynamic_memory)
        GsAmmGrantAccountFreedDynamicMemory(context->grant_token, chunk->total_size);
    else
        GsAmmGrantAccountFreedMemory(context->grant_token, chunk, chunk->total_size);
    AmmGranulePushFreeChunk(context, chunk);
    memory_context->isReset = context->liveBytes == 0;
}

static void* AmmGranuleRealloc(MemoryContext memory_context, void* pointer, Size align, Size size, const char* file, int line)
{
    AmmGranuleContextPtr context = (AmmGranuleContextPtr)memory_context;
    AmmGranuleChunk chunk;
    StandardChunkHeader* header;
    void* new_pointer;

    AssertArg(align == 0);
    if (!GsAmmGrantTokenIsValid(context->grant_token))
        return NULL;
    chunk = AmmGranulePointerGetChunk(pointer);
    header = AmmGranulePointerGetStandardHeader(pointer);

    if (MAXALIGN(size) <= chunk->payload_size) {
        header->size = chunk->payload_size;
#ifdef MEMORY_CONTEXT_CHECKING
        header->requested_size = size;
#endif
#ifdef MEMORY_CONTEXT_TRACK
        header->file = file;
        header->line = line;
#endif
        return pointer;
    }

    new_pointer = AmmGranuleAlloc(memory_context, align, size, file, line);
    if (new_pointer == NULL)
        return NULL;

    errno_t rc = memcpy_s(new_pointer, size, pointer, Min(header->size, size));
    securec_check(rc, "\0", "\0");
    AmmGranuleFree(memory_context, pointer);
    return new_pointer;
}

static void AmmGranuleInit(MemoryContext context)
{
    (void)context;
}

static void AmmGranuleReset(MemoryContext memory_context)
{
    AmmGranuleContextPtr context = (AmmGranuleContextPtr)memory_context;
    AmmGranuleChunk chunk;
    errno_t rc;

    if (!GsAmmGrantTokenIsValid(context->grant_token)) {
        context->chunks = NULL;
        context->allocatedBytes = 0;
        context->liveBytes = 0;
        context->set.totalSpace = 0;
        context->set.freeSpace = 0;
        rc = memset_s(context->freelist, sizeof(context->freelist), 0, sizeof(context->freelist));
        securec_check(rc, "\0", "\0");
        memory_context->isReset = true;
        return;
    }

    for (chunk = (AmmGranuleChunk)context->chunks; chunk != NULL;) {
        AmmGranuleChunk next = chunk->next;
        if (!chunk->is_free) {
            if (chunk->dynamic_memory)
                GsAmmGrantAccountFreedDynamicMemory(context->grant_token, chunk->total_size);
            else
                GsAmmGrantAccountFreedMemory(context->grant_token, chunk, chunk->total_size);
        }
        if (chunk->dynamic_memory)
            (void)GsAmmGrantReturnDynamicMemory(context->grant_token, chunk, chunk->total_size);
        else
            (void)GsAmmGrantReturnMemory(context->grant_token, chunk, chunk->total_size);
        chunk = next;
    }
    context->chunks = NULL;
    rc = memset_s(context->freelist, sizeof(context->freelist), 0, sizeof(context->freelist));
    securec_check(rc, "\0", "\0");
    context->allocatedBytes = 0;
    context->liveBytes = 0;
    context->set.totalSpace = 0;
    context->set.freeSpace = 0;
    memory_context->isReset = true;
}

static void AmmGranuleDelete(MemoryContext memory_context)
{
    AmmGranuleContextPtr context = (AmmGranuleContextPtr)memory_context;

    if (!GsAmmGrantTokenIsValid(context->grant_token)) {
        context->chunks = NULL;
        context->allocatedBytes = 0;
        context->liveBytes = 0;
        context->set.totalSpace = 0;
        context->set.freeSpace = 0;
        errno_t stale_rc = memset_s(context->freelist, sizeof(context->freelist), 0, sizeof(context->freelist));
        securec_check(stale_rc, "\0", "\0");
        return;
    }

    for (AmmGranuleChunk chunk = (AmmGranuleChunk)context->chunks; chunk != NULL;) {
        AmmGranuleChunk next = chunk->next;
        if (!chunk->is_free) {
            if (chunk->dynamic_memory)
                GsAmmGrantAccountFreedDynamicMemory(context->grant_token, chunk->total_size);
            else
                GsAmmGrantAccountFreedMemory(context->grant_token, chunk, chunk->total_size);
        }
        if (chunk->dynamic_memory)
            (void)GsAmmGrantReturnDynamicMemory(context->grant_token, chunk, chunk->total_size);
        else
            (void)GsAmmGrantReturnMemory(context->grant_token, chunk, chunk->total_size);
        chunk = next;
    }
    context->allocatedBytes = 0;
    context->liveBytes = 0;
    context->set.totalSpace = 0;
    context->set.freeSpace = 0;
    context->chunks = NULL;
    errno_t rc = memset_s(context->freelist, sizeof(context->freelist), 0, sizeof(context->freelist));
    securec_check(rc, "\0", "\0");
}

static Size AmmGranuleGetChunkSpace(MemoryContext context, void* pointer)
{
    (void)context;
    return AmmGranulePointerGetChunk(pointer)->total_size;
}

static bool AmmGranuleIsEmpty(MemoryContext memory_context)
{
    AmmGranuleContextPtr context = (AmmGranuleContextPtr)memory_context;

    return context->liveBytes == 0;
}

static void AmmGranuleStats(MemoryContext memory_context, int level)
{
    AmmGranuleContextPtr context = (AmmGranuleContextPtr)memory_context;

    fprintf(stderr, "%*s%s: grant=%llu allocated=%lu live=%lu free=%lu max=%lu\n",
        level,
        "",
        memory_context->name,
        (unsigned long long)context->grant_token.grant_id,
        (unsigned long)context->allocatedBytes,
        (unsigned long)context->liveBytes,
        (unsigned long)context->set.freeSpace,
        (unsigned long)context->maxBytes);
}

#ifdef MEMORY_CONTEXT_CHECKING
static void AmmGranuleCheck(MemoryContext context)
{
    (void)context;
}
#endif
