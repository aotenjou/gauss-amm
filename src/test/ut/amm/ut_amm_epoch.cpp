#include "gtest/gtest.h"

#include "storage/gs_amm.h"

TEST(GsAmmEpochSaturation, OwnerEpochFailsClosedOnlyForOwnerChanges)
{
    ASSERT_TRUE(GsAmmGranuleCountersCanAdvance(1, PG_UINT32_MAX, false));
    ASSERT_FALSE(GsAmmGranuleCountersCanAdvance(1, PG_UINT32_MAX, true));
}

TEST(GsAmmEpochSaturation, GenerationAlwaysFailsClosedAtMaximum)
{
    ASSERT_FALSE(GsAmmGranuleCountersCanAdvance(PG_UINT32_MAX, 1, false));
    ASSERT_FALSE(GsAmmGranuleCountersCanAdvance(PG_UINT32_MAX, 1, true));
}

TEST(GsAmmEpochSaturation, ApLifecycleRequiresEveryGenerationAndOwnerEpochStep)
{
    ASSERT_TRUE(GsAmmGranuleCanCompleteApLifecycle(
        PG_UINT32_MAX - GS_AMM_AP_LIFECYCLE_GENERATION_STEPS,
        PG_UINT32_MAX - GS_AMM_AP_LIFECYCLE_OWNER_EPOCH_STEPS));
    ASSERT_FALSE(GsAmmGranuleCanCompleteApLifecycle(
        PG_UINT32_MAX - GS_AMM_AP_LIFECYCLE_GENERATION_STEPS + 1,
        PG_UINT32_MAX - GS_AMM_AP_LIFECYCLE_OWNER_EPOCH_STEPS));
    ASSERT_FALSE(GsAmmGranuleCanCompleteApLifecycle(
        PG_UINT32_MAX - GS_AMM_AP_LIFECYCLE_GENERATION_STEPS,
        PG_UINT32_MAX - GS_AMM_AP_LIFECYCLE_OWNER_EPOCH_STEPS + 1));
}
