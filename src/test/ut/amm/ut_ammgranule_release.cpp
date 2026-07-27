#include "gtest/gtest.h"

#include <cstdlib>

#include "storage/gs_amm.h"
#include "utils/ammgranule.h"
#include "utils/memutils.h"
#include "utils/palloc.h"

static GsAmmGrantToken g_test_grant_token = {0, 0};
static uint64 g_test_return_calls = 0;
static Size g_test_returned_bytes = 0;
static uint64 g_test_return_attempts = 0;
static uint64 g_test_fail_return_attempt = 0;

static void ResetTestGrant(GsAmmGrantToken token)
{
    g_test_grant_token = token;
    g_test_return_calls = 0;
    g_test_returned_bytes = 0;
    g_test_return_attempts = 0;
    g_test_fail_return_attempt = 0;
}

static void FailTestGrantReturnAttempt(uint64 attempt)
{
    g_test_fail_return_attempt = attempt;
}

static void EnsureTestMemoryContext(void)
{
    static bool initialized = false;

    if (!initialized) {
        MemoryContextInit();
        initialized = true;
    }
}

uint64 GsAmmCurrentBackendGrantGeneration(void)
{
    return g_test_grant_token.grant_generation;
}

uint64 GsAmmCurrentBackendGrantId(void)
{
    return g_test_grant_token.grant_id;
}

bool GsAmmGrantTokenIsValid(GsAmmGrantToken token)
{
    return token.grant_id != 0 && token.grant_generation != 0 &&
        token.grant_id == g_test_grant_token.grant_id &&
        token.grant_generation == g_test_grant_token.grant_generation;
}

bool GsAmmGrantCanAllocateMemory(GsAmmGrantToken token, Size size)
{
    return GsAmmGrantTokenIsValid(token) && size > 0;
}

void* GsAmmGrantAllocMemory(GsAmmGrantToken token, Size size)
{
    if (!GsAmmGrantTokenIsValid(token) || size == 0)
        return NULL;
    return std::malloc(size);
}

bool GsAmmGrantReturnMemory(GsAmmGrantToken token, void* pointer, Size size)
{
    if (!GsAmmGrantTokenIsValid(token) || pointer == NULL || size == 0)
        return false;
    g_test_return_attempts++;
    if (g_test_return_attempts == g_test_fail_return_attempt)
        return false;
    g_test_return_calls++;
    g_test_returned_bytes += size;
    std::free(pointer);
    return true;
}

void GsAmmGrantAccountUsedMemory(GsAmmGrantToken token, void* pointer, Size size)
{
    (void)token;
    (void)pointer;
    (void)size;
}

void GsAmmGrantAccountFreedMemory(GsAmmGrantToken token, void* pointer, Size size)
{
    (void)token;
    (void)pointer;
    (void)size;
}

TEST(AmmGranuleRelease, ReleasesOnlyFreeChunksAndLowersAllocatedBytes)
{
    GsAmmGrantToken grant = {101, 17};
    MemoryContext context;
    AmmGranuleContextStatsData before;
    AmmGranuleContextStatsData after;
    void* retained;
    void* released;
    void* larger;
    Size returned;

    EnsureTestMemoryContext();
    ResetTestGrant(grant);
    context = AmmGranuleContextCreate(CurrentMemoryContext, "ammgranule release", grant.grant_id, 4096);
    retained = AmmGranuleContextTryAlloc(context, 256);
    released = AmmGranuleContextTryAlloc(context, 1500);
    ASSERT_NE(retained, nullptr);
    ASSERT_NE(released, nullptr);
    ASSERT_TRUE(AmmGranuleContextStats(context, &before));

    pfree(released);
    ASSERT_FALSE(AmmGranuleContextCanAllocate(context, 2500));

    returned = AmmGranuleContextReleaseFreeMemory(context);
    ASSERT_GT(returned, 0U);
    ASSERT_EQ(g_test_returned_bytes, returned);
    ASSERT_TRUE(AmmGranuleContextStats(context, &after));
    ASSERT_LT(after.allocated_bytes, before.allocated_bytes);
    ASSERT_EQ(after.live_bytes, before.live_bytes - returned);
    ASSERT_TRUE(AmmGranuleContextCanAllocate(context, 2500));

    larger = AmmGranuleContextTryAlloc(context, 2500);
    ASSERT_NE(larger, nullptr);
    pfree(larger);
    pfree(retained);
    (void)AmmGranuleContextReleaseFreeMemory(context);
    MemoryContextDelete(context);
}

TEST(AmmGranuleRelease, StaleTokenCannotReturnMemoryForNewGrant)
{
    GsAmmGrantToken old_grant = {201, 31};
    GsAmmGrantToken new_grant = {202, 32};
    MemoryContext old_context;
    MemoryContext new_context;
    void* old_pointer;
    void* new_pointer;
    uint64 return_calls_before;

    EnsureTestMemoryContext();
    ResetTestGrant(old_grant);
    old_context = AmmGranuleContextCreate(CurrentMemoryContext, "old amm grant", old_grant.grant_id, 4096);
    old_pointer = AmmGranuleContextTryAlloc(old_context, 1024);
    ASSERT_NE(old_pointer, nullptr);
    pfree(old_pointer);

    ResetTestGrant(new_grant);
    new_context = AmmGranuleContextCreate(CurrentMemoryContext, "new amm grant", new_grant.grant_id, 4096);
    new_pointer = AmmGranuleContextTryAlloc(new_context, 512);
    ASSERT_NE(new_pointer, nullptr);
    return_calls_before = g_test_return_calls;

    ASSERT_EQ(AmmGranuleContextReleaseFreeMemory(old_context), 0U);
    ASSERT_EQ(g_test_return_calls, return_calls_before);
    ASSERT_TRUE(AmmGranuleContextCanAllocate(new_context, 1024));

    pfree(new_pointer);
    (void)AmmGranuleContextReleaseFreeMemory(new_context);
    MemoryContextDelete(new_context);
    ResetTestGrant(old_grant);
    (void)AmmGranuleContextReleaseFreeMemory(old_context);
    MemoryContextDelete(old_context);
}

TEST(AmmGranuleRelease, FailedReturnKeepsOnlyThatChunkReusable)
{
    GsAmmGrantToken grant = {301, 41};
    MemoryContext context;
    AmmGranuleContextStatsData before;
    AmmGranuleContextStatsData partial;
    AmmGranuleContextStatsData complete;
    void* first;
    void* second;
    void* reused;
    Size partial_released;

    EnsureTestMemoryContext();
    ResetTestGrant(grant);
    context = AmmGranuleContextCreate(CurrentMemoryContext, "partial free release", grant.grant_id, 8192);
    first = AmmGranuleContextTryAlloc(context, 512);
    second = AmmGranuleContextTryAlloc(context, 1024);
    ASSERT_NE(first, nullptr);
    ASSERT_NE(second, nullptr);
    ASSERT_TRUE(AmmGranuleContextStats(context, &before));
    pfree(first);
    pfree(second);

    FailTestGrantReturnAttempt(1);
    partial_released = AmmGranuleContextReleaseFreeMemory(context);
    ASSERT_GT(partial_released, 0U);
    ASSERT_EQ(g_test_return_attempts, 2U);
    ASSERT_EQ(g_test_return_calls, 1U);
    ASSERT_TRUE(AmmGranuleContextStats(context, &partial));
    ASSERT_EQ(partial.allocated_bytes, before.allocated_bytes - partial_released);
    ASSERT_EQ(partial.live_bytes, 0U);
    ASSERT_EQ(partial.free_bytes, partial.allocated_bytes);

    reused = AmmGranuleContextTryAlloc(context, 1024);
    ASSERT_EQ(reused, second);
    pfree(reused);
    FailTestGrantReturnAttempt(0);
    ASSERT_GT(AmmGranuleContextReleaseFreeMemory(context), 0U);
    ASSERT_TRUE(AmmGranuleContextStats(context, &complete));
    ASSERT_EQ(complete.allocated_bytes, 0U);
    ASSERT_EQ(complete.free_bytes, 0U);
    MemoryContextDelete(context);
}
