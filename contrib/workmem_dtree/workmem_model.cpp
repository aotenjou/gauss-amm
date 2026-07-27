/* Auto-generated tree inference; no PostgreSQL or ML runtime dependency. */
#include "workmem_model.h"
#include <math.h>
#include <string.h>

#define MEMTUNE_FNV_OFFSET 1469598103934665603ULL
#define MEMTUNE_FNV_PRIME 1099511628211ULL

static unsigned long long
hash_u64(unsigned long long hash, unsigned long long value)
{
    int index;

    for (index = 0; index < 8; ++index) {
        hash ^= (value >> (index * 8)) & 0xffU;
        hash *= MEMTUNE_FNV_PRIME;
    }
    return hash;
}

static unsigned long long
hash_double(unsigned long long hash, double value)
{
    unsigned long long bits = 0;

    (void)memcpy(&bits, &value, sizeof(bits));
    return hash_u64(hash, bits);
}

static int64_t
leaf_id_from_bounds(const MemTuneWorkMemBounds *bounds)
{
    unsigned long long hash = MEMTUNE_FNV_OFFSET;

    hash = hash_double(hash, bounds->cache_mb);
    hash = hash_double(hash, bounds->one_pass_mb);
    hash = hash_double(hash, bounds->multi_pass_mb);
    hash &= 0x7fffffffffffffffULL;
    return hash == 0 ? 1 : (int64_t)hash;
}

static double predict_cache_mb(const double *x)
{
    if (x[4] <= 5.8494858741760254) /* max_plan_rows_log10 */
    {
        if (x[1] <= 1.5) /* sort_nodes */
        {
            if (x[6] <= 140579.875) /* total_cost */
            {
                if (x[8] <= 6) /* parallel_aware_nodes */
                {
                    if (x[5] <= 172) /* max_plan_width */
                    {
                        if (x[13] <= 205089) /* system_available_memory_mb */
                        {
                            return 0.34443049016091642;
                        }
                        else
                        {
                            return 0.34042354394797719;
                        }
                    }
                    else
                    {
                        if (x[9] <= 3.5) /* active_sessions */
                        {
                            return 0.33178266101534465;
                        }
                        else
                        {
                            return 0.31969720149097525;
                        }
                    }
                }
                else
                {
                    if (x[13] <= 220697) /* system_available_memory_mb */
                    {
                        if (x[6] <= 123408.84375) /* total_cost */
                        {
                            return 0.21505089005151518;
                        }
                        else
                        {
                            return 0.23730499999999985;
                        }
                    }
                    else
                    {
                        if (x[9] <= 2.5) /* active_sessions */
                        {
                            return 0.1149415;
                        }
                        else
                        {
                            return 0.1864038828902557;
                        }
                    }
                }
            }
            else
            {
                if (x[6] <= 157747.5859375) /* total_cost */
                {
                    if (x[6] <= 157731.578125) /* total_cost */
                    {
                        if (x[6] <= 157729.0234375) /* total_cost */
                        {
                            return 0.70228415899212682;
                        }
                        else
                        {
                            return 0.41639617310280502;
                        }
                    }
                    else
                    {
                        if (x[18] <= 11.968721866607666) /* total_cost_log1p */
                        {
                            return 0.7585394080688076;
                        }
                        else
                        {
                            return 0.76042190092123008;
                        }
                    }
                }
                else
                {
                    if (x[6] <= 157772.1796875) /* total_cost */
                    {
                        if (x[6] <= 157770.3359375) /* total_cost */
                        {
                            return 0.57762807514743275;
                        }
                        else
                        {
                            return 0.42223690242520329;
                        }
                    }
                    else
                    {
                        if (x[18] <= 11.969179630279541) /* total_cost_log1p */
                        {
                            return 0.68557142968223606;
                        }
                        else
                        {
                            return 0.78155095881963832;
                        }
                    }
                }
            }
        }
        else
        {
            if (x[4] <= 5.6412489414215088) /* max_plan_rows_log10 */
            {
                if (x[6] <= 71227.30859375) /* total_cost */
                {
                    if (x[14] <= 0.15724999457597733) /* memory_pressure_score */
                    {
                        if (x[6] <= 40867.64453125) /* total_cost */
                        {
                            return 3.6625840757721728;
                        }
                        else
                        {
                            return 4.3076688830923038;
                        }
                    }
                    else
                    {
                        if (x[13] <= 216282) /* system_available_memory_mb */
                        {
                            return 3.6071601182976551;
                        }
                        else
                        {
                            return 3.5501814396211624;
                        }
                    }
                }
                else
                {
                    if (x[4] <= 5.5013034343719482) /* max_plan_rows_log10 */
                    {
                        if (x[6] <= 196096.75) /* total_cost */
                        {
                            return 0.77045120206755557;
                        }
                        else
                        {
                            return 0.37119136884208359;
                        }
                    }
                    else
                    {
                        if (x[14] <= 0.1554500013589859) /* memory_pressure_score */
                        {
                            return 0.76493089209589393;
                        }
                        else
                        {
                            return 0.41379207748002261;
                        }
                    }
                }
            }
            else
            {
                if (x[15] <= 149.60150623321533) /* involved_table_total_size_mb */
                {
                    if (x[18] <= 12.217683792114258) /* total_cost_log1p */
                    {
                        if (x[13] <= 217016) /* system_available_memory_mb */
                        {
                            return 4.1877320283395401;
                        }
                        else
                        {
                            return 3.2596900222184395;
                        }
                    }
                    else
                    {
                        if (x[18] <= 12.224154472351074) /* total_cost_log1p */
                        {
                            return 6.2391467737044231;
                        }
                        else
                        {
                            return 7.1433919656147165;
                        }
                    }
                }
                else
                {
                    return 7.6337889999998687;
                }
            }
        }
    }
    else
    {
        if (x[5] <= 119.5) /* max_plan_width */
        {
            if (x[13] <= 217875) /* system_available_memory_mb */
            {
                if (x[13] <= 217689.5) /* system_available_memory_mb */
                {
                    if (x[13] <= 205069) /* system_available_memory_mb */
                    {
                        if (x[14] <= 0.2036999985575676) /* memory_pressure_score */
                        {
                            return 87.978866534966741;
                        }
                        else
                        {
                            return 160.47444699321971;
                        }
                    }
                    else
                    {
                        if (x[13] <= 205081) /* system_available_memory_mb */
                        {
                            return 270.26494663519446;
                        }
                        else
                        {
                            return 187.38043264604005;
                        }
                    }
                }
                else
                {
                    if (x[9] <= 2.5) /* active_sessions */
                    {
                        if (x[14] <= 0.15394999831914902) /* memory_pressure_score */
                        {
                            return 90.361816600000026;
                        }
                        else
                        {
                            return 165.6921335810909;
                        }
                    }
                    else
                    {
                        if (x[13] <= 217786) /* system_available_memory_mb */
                        {
                            return 88.029589899999962;
                        }
                        else
                        {
                            return 92.652965958690146;
                        }
                    }
                }
            }
            else
            {
                if (x[13] <= 220667.5) /* system_available_memory_mb */
                {
                    if (x[13] <= 220005) /* system_available_memory_mb */
                    {
                        if (x[13] <= 220001) /* system_available_memory_mb */
                        {
                            return 250.81070258283228;
                        }
                        else
                        {
                            return 87.865602334113234;
                        }
                    }
                    else
                    {
                        if (x[13] <= 220523.5) /* system_available_memory_mb */
                        {
                            return 296.0558548111112;
                        }
                        else
                        {
                            return 286.56595431351076;
                        }
                    }
                }
                else
                {
                    if (x[13] <= 220702) /* system_available_memory_mb */
                    {
                        if (x[13] <= 220696) /* system_available_memory_mb */
                        {
                            return 91.112125842268355;
                        }
                        else
                        {
                            return 160.12216250836053;
                        }
                    }
                    else
                    {
                        if (x[9] <= 2.5) /* active_sessions */
                        {
                            return 294.31159444735408;
                        }
                        else
                        {
                            return 300.66878845263864;
                        }
                    }
                }
            }
        }
        else
        {
            if (x[16] <= 12.883000135421753) /* involved_index_total_size_mb */
            {
                if (x[18] <= 12.408895969390869) /* total_cost_log1p */
                {
                    if (x[18] <= 12.35106897354126) /* total_cost_log1p */
                    {
                        if (x[14] <= 0.15439999848604202) /* memory_pressure_score */
                        {
                            return 10.943085342347741;
                        }
                        else
                        {
                            return 8.2795289447036957;
                        }
                    }
                    else
                    {
                        if (x[9] <= 3.5) /* active_sessions */
                        {
                            return 9.262977831476384;
                        }
                        else
                        {
                            return 9.9920830795257274;
                        }
                    }
                }
                else
                {
                    if (x[9] <= 3.5) /* active_sessions */
                    {
                        if (x[6] <= 282427.03125) /* total_cost */
                        {
                            return 10.092282544019728;
                        }
                        else
                        {
                            return 10.669041203369897;
                        }
                    }
                    else
                    {
                        if (x[13] <= 204741.5) /* system_available_memory_mb */
                        {
                            return 8.6724712789621723;
                        }
                        else
                        {
                            return 10.930832372394811;
                        }
                    }
                }
            }
            else
            {
                return 22.972656000001233;
            }
        }
    }
}

static double predict_one_pass_mb(const double *x)
{
    if (x[4] <= 5.8494858741760254) /* max_plan_rows_log10 */
    {
        if (x[6] <= 129956.5390625) /* total_cost */
        {
            if (x[4] <= 5.1050693988800049) /* max_plan_rows_log10 */
            {
                if (x[18] <= 11.527603149414062) /* total_cost_log1p */
                {
                    if (x[13] <= 204765.5) /* system_available_memory_mb */
                    {
                        if (x[13] <= 204741) /* system_available_memory_mb */
                        {
                            return 0.78222700000000001;
                        }
                        else
                        {
                            return 0.78171401892336556;
                        }
                    }
                    else
                    {
                        if (x[13] <= 216343.5) /* system_available_memory_mb */
                        {
                            return 0.78217881844585713;
                        }
                        else
                        {
                            return 0.78222699999999989;
                        }
                    }
                }
                else
                {
                    return 0.7822269999999989;
                }
            }
            else
            {
                if (x[17] <= 18.226499557495117) /* average_column_width */
                {
                    if (x[16] <= 19.847999334335327) /* involved_index_total_size_mb */
                    {
                        if (x[14] <= 0.14295000582933426) /* memory_pressure_score */
                        {
                            return 0.1463592594058753;
                        }
                        else
                        {
                            return 0.20634584590849644;
                        }
                    }
                    else
                    {
                        if (x[13] <= 205228.5) /* system_available_memory_mb */
                        {
                            return 0.21792282896078755;
                        }
                        else
                        {
                            return 0.21310410135819521;
                        }
                    }
                }
                else
                {
                    if (x[0] <= 2.5) /* hash_join_nodes */
                    {
                        if (x[6] <= 40849.53125) /* total_cost */
                        {
                            return 0.38756800598186208;
                        }
                        else
                        {
                            return 0.37070275542089365;
                        }
                    }
                    else
                    {
                        if (x[9] <= 3.5) /* active_sessions */
                        {
                            return 0.33178266101534459;
                        }
                        else
                        {
                            return 0.3196972014909753;
                        }
                    }
                }
            }
        }
        else
        {
            if (x[4] <= 5.6412489414215088) /* max_plan_rows_log10 */
            {
                if (x[4] <= 5.5013034343719482) /* max_plan_rows_log10 */
                {
                    if (x[4] <= 5.4940860271453857) /* max_plan_rows_log10 */
                    {
                        if (x[6] <= 157747.5859375) /* total_cost */
                        {
                            return 0.69511371000003097;
                        }
                        else
                        {
                            return 0.63034665521697664;
                        }
                    }
                    else
                    {
                        if (x[6] <= 196096.75) /* total_cost */
                        {
                            return 0.57752615830672482;
                        }
                        else
                        {
                            return 0.23759779999999997;
                        }
                    }
                }
                else
                {
                    if (x[13] <= 217474) /* system_available_memory_mb */
                    {
                        if (x[9] <= 2.5) /* active_sessions */
                        {
                            return 0.38330250363067025;
                        }
                        else
                        {
                            return 0.23759779999999986;
                        }
                    }
                    else
                    {
                        if (x[14] <= 0.1489500030875206) /* memory_pressure_score */
                        {
                            return 0.61835930000000006;
                        }
                        else
                        {
                            return 0.61835930000000006;
                        }
                    }
                }
            }
            else
            {
                if (x[8] <= 5.5) /* parallel_aware_nodes */
                {
                    if (x[6] <= 137043.484375) /* total_cost */
                    {
                        if (x[13] <= 204730.5) /* system_available_memory_mb */
                        {
                            return 5.5976780915739708;
                        }
                        else
                        {
                            return 5.0061678795620699;
                        }
                    }
                    else
                    {
                        if (x[13] <= 204725) /* system_available_memory_mb */
                        {
                            return 5.5976780915739708;
                        }
                        else
                        {
                            return 4.9071718515952485;
                        }
                    }
                }
                else
                {
                    if (x[6] <= 202334.9765625) /* total_cost */
                    {
                        if (x[13] <= 217016) /* system_available_memory_mb */
                        {
                            return 2.4841881976201936;
                        }
                        else
                        {
                            return 1.9920899000000001;
                        }
                    }
                    else
                    {
                        if (x[18] <= 12.224154472351074) /* total_cost_log1p */
                        {
                            return 3.5547608976423386;
                        }
                        else
                        {
                            return 3.9321823808823324;
                        }
                    }
                }
            }
        }
    }
    else
    {
        if (x[16] <= 76.632996559143066) /* involved_index_total_size_mb */
        {
            if (x[4] <= 6.1505858898162842) /* max_plan_rows_log10 */
            {
                if (x[9] <= 3.5) /* active_sessions */
                {
                    return 11.486328000000094;
                }
                else
                {
                    if (x[14] <= 0.15585000067949295) /* memory_pressure_score */
                    {
                        if (x[18] <= 11.109557151794434) /* total_cost_log1p */
                        {
                            return 11.486327999999999;
                        }
                        else
                        {
                            return 11.486327999999993;
                        }
                    }
                    else
                    {
                        return 11.48632800000007;
                    }
                }
            }
            else
            {
                if (x[8] <= 5.5) /* parallel_aware_nodes */
                {
                    if (x[14] <= 0.15439999848604202) /* memory_pressure_score */
                    {
                        if (x[13] <= 218842.5) /* system_available_memory_mb */
                        {
                            return 3.1404296999999999;
                        }
                        else
                        {
                            return 0.12988300000000003;
                        }
                    }
                    else
                    {
                        if (x[13] <= 216861) /* system_available_memory_mb */
                        {
                            return 0.26716661721899787;
                        }
                        else
                        {
                            return 0.19020679660919465;
                        }
                    }
                }
                else
                {
                    if (x[6] <= 282427.03125) /* total_cost */
                    {
                        if (x[8] <= 8.5) /* parallel_aware_nodes */
                        {
                            return 3.334184544230943;
                        }
                        else
                        {
                            return 4.0489263951914545;
                        }
                    }
                    else
                    {
                        if (x[6] <= 289085.40625) /* total_cost */
                        {
                            return 5.6242129966676364;
                        }
                        else
                        {
                            return 6.6235735515484579;
                        }
                    }
                }
            }
        }
        else
        {
            if (x[13] <= 217875) /* system_available_memory_mb */
            {
                if (x[13] <= 217689.5) /* system_available_memory_mb */
                {
                    if (x[13] <= 205069) /* system_available_memory_mb */
                    {
                        if (x[13] <= 205052) /* system_available_memory_mb */
                        {
                            return 158.80690461404006;
                        }
                        else
                        {
                            return 87.344547758695043;
                        }
                    }
                    else
                    {
                        if (x[13] <= 205081) /* system_available_memory_mb */
                        {
                            return 269.95043478781895;
                        }
                        else
                        {
                            return 185.43729441244952;
                        }
                    }
                }
                else
                {
                    if (x[9] <= 2.5) /* active_sessions */
                    {
                        if (x[13] <= 217776.5) /* system_available_memory_mb */
                        {
                            return 86.162988399999975;
                        }
                        else
                        {
                            return 161.53369488683467;
                        }
                    }
                    else
                    {
                        if (x[13] <= 217855) /* system_available_memory_mb */
                        {
                            return 88.540636508666694;
                        }
                        else
                        {
                            return 85.694818329523855;
                        }
                    }
                }
            }
            else
            {
                if (x[13] <= 220702) /* system_available_memory_mb */
                {
                    if (x[14] <= 0.14305000007152557) /* memory_pressure_score */
                    {
                        if (x[13] <= 220696) /* system_available_memory_mb */
                        {
                            return 88.439766234383228;
                        }
                        else
                        {
                            return 160.12216250836053;
                        }
                    }
                    else
                    {
                        if (x[13] <= 220005) /* system_available_memory_mb */
                        {
                            return 231.55274302253881;
                        }
                        else
                        {
                            return 290.46554559625991;
                        }
                    }
                }
                else
                {
                    if (x[9] <= 2.5) /* active_sessions */
                    {
                        if (x[13] <= 220710) /* system_available_memory_mb */
                        {
                            return 302.28671880000007;
                        }
                        else
                        {
                            return 286.54687499999989;
                        }
                    }
                    else
                    {
                        if (x[13] <= 220741.5) /* system_available_memory_mb */
                        {
                            return 299.7273546990881;
                        }
                        else
                        {
                            return 304.46318369999989;
                        }
                    }
                }
            }
        }
    }
}

static double predict_multi_pass_mb(const double *x)
{
    if (x[10] <= 2.8005000352859497) /* current_session_private_memory_mb */
    {
        return 0.062499999999974916;
    }
    else
    {
        if (x[13] <= 217412) /* system_available_memory_mb */
        {
            if (x[13] <= 216928) /* system_available_memory_mb */
            {
                if (x[13] <= 216856.5) /* system_available_memory_mb */
                {
                    if (x[13] <= 216410.5) /* system_available_memory_mb */
                    {
                        if (x[13] <= 207189.5) /* system_available_memory_mb */
                        {
                            return 0.060093808520603254;
                        }
                        else
                        {
                            return 0.057946141471526116;
                        }
                    }
                    else
                    {
                        return 0.062499999999999924;
                    }
                }
                else
                {
                    if (x[13] <= 216860.5) /* system_available_memory_mb */
                    {
                        return 0.019604492187499999;
                    }
                    else
                    {
                        if (x[13] <= 216919.5) /* system_available_memory_mb */
                        {
                            return 0.05455325565677898;
                        }
                        else
                        {
                            return 0.029687499999999999;
                        }
                    }
                }
            }
            else
            {
                return 0.062499999999999868;
            }
        }
        else
        {
            if (x[13] <= 217430.5) /* system_available_memory_mb */
            {
                if (x[13] <= 217417) /* system_available_memory_mb */
                {
                    if (x[13] <= 217413.5) /* system_available_memory_mb */
                    {
                        return 0.035004010651906017;
                    }
                    else
                    {
                        return 0.0625;
                    }
                }
                else
                {
                    if (x[14] <= 0.15564999729394913) /* memory_pressure_score */
                    {
                        return 0.043075152350281934;
                    }
                    else
                    {
                        if (x[13] <= 217419.5) /* system_available_memory_mb */
                        {
                            return 0.029687499999999999;
                        }
                        else
                        {
                            return 0.019604492187499999;
                        }
                    }
                }
            }
            else
            {
                if (x[13] <= 217533.5) /* system_available_memory_mb */
                {
                    return 0.062500000000000028;
                }
                else
                {
                    if (x[13] <= 217543.5) /* system_available_memory_mb */
                    {
                        if (x[13] <= 217540) /* system_available_memory_mb */
                        {
                            return 0.046773396987434282;
                        }
                        else
                        {
                            return 0.029687499999999999;
                        }
                    }
                    else
                    {
                        if (x[13] <= 217585) /* system_available_memory_mb */
                        {
                            return 0.051886385708513387;
                        }
                        else
                        {
                            return 0.059594920818907;
                        }
                    }
                }
            }
        }
    }
}

void
memtune_workmem_features_to_array(
    const MemTuneWorkMemFeatures *features,
    double output[MEMTUNE_WORKMEM_FEATURE_COUNT])
{
    const double values[MEMTUNE_WORKMEM_FEATURE_COUNT] = {
        features->hash_join_nodes /* MEMTUNE_FEATURE_HASH_JOIN_NODES */,
        features->sort_nodes /* MEMTUNE_FEATURE_SORT_NODES */,
        features->aggregate_nodes /* MEMTUNE_FEATURE_AGGREGATE_NODES */,
        features->window_nodes /* MEMTUNE_FEATURE_WINDOW_NODES */,
        features->max_plan_rows_log10 /* MEMTUNE_FEATURE_MAX_PLAN_ROWS_LOG10 */,
        features->max_plan_width /* MEMTUNE_FEATURE_MAX_PLAN_WIDTH */,
        features->total_cost /* MEMTUNE_FEATURE_TOTAL_COST */,
        features->parallel_workers_planned /* MEMTUNE_FEATURE_PARALLEL_WORKERS_PLANNED */,
        features->parallel_aware_nodes /* MEMTUNE_FEATURE_PARALLEL_AWARE_NODES */,
        features->active_sessions /* MEMTUNE_FEATURE_ACTIVE_SESSIONS */,
        features->current_session_private_memory_mb /* MEMTUNE_FEATURE_CURRENT_SESSION_PRIVATE_MEMORY_MB */,
        features->current_transaction_age_sec /* MEMTUNE_FEATURE_CURRENT_TRANSACTION_AGE_SEC */,
        features->system_total_memory_mb /* MEMTUNE_FEATURE_SYSTEM_TOTAL_MEMORY_MB */,
        features->system_available_memory_mb /* MEMTUNE_FEATURE_SYSTEM_AVAILABLE_MEMORY_MB */,
        features->memory_pressure_score /* MEMTUNE_FEATURE_MEMORY_PRESSURE_SCORE */,
        features->involved_table_total_size_mb /* MEMTUNE_FEATURE_INVOLVED_TABLE_TOTAL_SIZE_MB */,
        features->involved_index_total_size_mb /* MEMTUNE_FEATURE_INVOLVED_INDEX_TOTAL_SIZE_MB */,
        features->average_column_width /* MEMTUNE_FEATURE_AVERAGE_COLUMN_WIDTH */,
        features->total_cost_log1p /* MEMTUNE_FEATURE_TOTAL_COST_LOG1P */
    };
    int index;

    for (index = 0; index < MEMTUNE_WORKMEM_FEATURE_COUNT; ++index)
        output[index] = values[index];
}

MemTuneWorkMemBounds
memtune_predict_workmem_bounds(const double features[MEMTUNE_WORKMEM_FEATURE_COUNT])
{
    MemTuneWorkMemBounds result;

    result.cache_mb = predict_cache_mb(features);
    result.one_pass_mb = predict_one_pass_mb(features);
    result.multi_pass_mb = predict_multi_pass_mb(features);

    if (result.multi_pass_mb < 0.0625)
        result.multi_pass_mb = 0.0625;
    if (result.one_pass_mb < result.multi_pass_mb)
        result.one_pass_mb = result.multi_pass_mb;
    if (result.cache_mb < result.one_pass_mb)
        result.cache_mb = result.one_pass_mb;
    return result;
}

MemTuneWorkMemPrediction
memtune_predict_workmem_detail(const double features[MEMTUNE_WORKMEM_FEATURE_COUNT])
{
    MemTuneWorkMemPrediction result;

    result.bounds = memtune_predict_workmem_bounds(features);
    result.model_version = MEMTUNE_WORKMEM_MODEL_VERSION;
    result.leaf_id = leaf_id_from_bounds(&result.bounds);
    return result;
}
