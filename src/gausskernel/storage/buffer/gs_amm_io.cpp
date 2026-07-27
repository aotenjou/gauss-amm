/* -------------------------------------------------------------------------
 *
 * gs_amm_io.cpp
 *      Native device-I/O sampling for the GS AMM controller.
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <sys/stat.h>
#ifdef __linux__
#include <sys/sysmacros.h>
#endif

#include "access/xlog_basic.h"
#include "storage/gs_amm_io.h"
#include "storage/smgr/fd.h"
#include "utils/timestamp.h"

#define GS_AMM_IO_MAX_DEVICES 2

typedef struct GsAmmDeviceId {
    unsigned int major_id;
    unsigned int minor_id;
} GsAmmDeviceId;

typedef struct GsAmmDeviceIoHistory {
    bool initialized;
    int device_count;
    GsAmmDeviceId devices[GS_AMM_IO_MAX_DEVICES];
    uint64 io_ms[GS_AMM_IO_MAX_DEVICES];
    TimestampTz sampled_at;
} GsAmmDeviceIoHistory;

static THR_LOCAL GsAmmDeviceIoHistory gs_amm_committed_device_io_history;
static THR_LOCAL GsAmmDeviceIoHistory gs_amm_pending_device_io_history;

#ifdef __linux__
static bool gs_amm_add_device_for_path(
    const char *path, GsAmmDeviceId devices[GS_AMM_IO_MAX_DEVICES], int *device_count)
{
    struct stat stat_buffer;

    if (path == NULL || path[0] == '\0' || stat(path, &stat_buffer) != 0)
        return false;

    unsigned int major_id = major(stat_buffer.st_dev);
    unsigned int minor_id = minor(stat_buffer.st_dev);
    for (int index = 0; index < *device_count; index++) {
        if (devices[index].major_id == major_id && devices[index].minor_id == minor_id)
            return true;
    }
    if (*device_count >= GS_AMM_IO_MAX_DEVICES)
        return false;

    devices[*device_count].major_id = major_id;
    devices[*device_count].minor_id = minor_id;
    (*device_count)++;
    return true;
}

static bool gs_amm_read_diskstats(
    const GsAmmDeviceId devices[GS_AMM_IO_MAX_DEVICES], int device_count, uint64 io_ms[GS_AMM_IO_MAX_DEVICES],
    int *in_flight)
{
    FILE *diskstats = AllocateFile("/proc/diskstats", "r");
    char line[512];
    bool found[GS_AMM_IO_MAX_DEVICES] = {false};

    if (diskstats == NULL)
        return false;

    *in_flight = 0;
    while (fgets(line, sizeof(line), diskstats) != NULL) {
        unsigned int major_id;
        unsigned int minor_id;
        char device_name[64];
        unsigned long long reads_completed;
        unsigned long long reads_merged;
        unsigned long long sectors_read;
        unsigned long long read_ms;
        unsigned long long writes_completed;
        unsigned long long writes_merged;
        unsigned long long sectors_written;
        unsigned long long write_ms;
        unsigned long long current_in_flight;
        unsigned long long total_io_ms;
        unsigned long long weighted_io_ms;
        int parsed;

        parsed = sscanf(line,
            "%u %u %63s %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu", &major_id, &minor_id,
            device_name, &reads_completed, &reads_merged, &sectors_read, &read_ms, &writes_completed,
            &writes_merged, &sectors_written, &write_ms, &current_in_flight, &total_io_ms, &weighted_io_ms);
        if (parsed != 14)
            continue;

        for (int index = 0; index < device_count; index++) {
            if (devices[index].major_id != major_id || devices[index].minor_id != minor_id)
                continue;
            if (current_in_flight > (unsigned long long)INT_MAX ||
                current_in_flight > (unsigned long long)(INT_MAX - *in_flight)) {
                (void)FreeFile(diskstats);
                return false;
            }
            io_ms[index] = (uint64)total_io_ms;
            *in_flight += (int)current_in_flight;
            found[index] = true;
            break;
        }
    }
    (void)FreeFile(diskstats);

    for (int index = 0; index < device_count; index++) {
        if (!found[index])
            return false;
    }
    return true;
}

static bool gs_amm_history_matches(
    const GsAmmDeviceIoHistory *history, const GsAmmDeviceId devices[GS_AMM_IO_MAX_DEVICES], int device_count)
{
    if (!history->initialized || history->device_count != device_count)
        return false;
    for (int index = 0; index < device_count; index++) {
        if (history->devices[index].major_id != devices[index].major_id ||
            history->devices[index].minor_id != devices[index].minor_id)
            return false;
    }
    return true;
}

static void gs_amm_save_history(GsAmmDeviceIoHistory *history, const GsAmmDeviceId devices[GS_AMM_IO_MAX_DEVICES],
    int device_count, const uint64 io_ms[GS_AMM_IO_MAX_DEVICES], TimestampTz sampled_at)
{
    history->initialized = true;
    history->device_count = device_count;
    history->sampled_at = sampled_at;
    for (int index = 0; index < device_count; index++) {
        history->devices[index] = devices[index];
        history->io_ms[index] = io_ms[index];
    }
}
#endif

void GsAmmSampleDeviceIo(GsAmmDeviceIoSample *sample)
{
    errno_t rc;

    if (sample == NULL)
        return;
    rc = memset_s(sample, sizeof(*sample), 0, sizeof(*sample));
    securec_check(rc, "\0", "\0");

#ifdef __linux__
    GsAmmDeviceId devices[GS_AMM_IO_MAX_DEVICES];
    uint64 io_ms[GS_AMM_IO_MAX_DEVICES] = {0};
    char wal_path[MAXPGPATH];
    const char *wal_directory;
    TimestampTz now;
    int device_count = 0;
    int in_flight = 0;

    rc = memset_s(&gs_amm_pending_device_io_history, sizeof(gs_amm_pending_device_io_history), 0,
        sizeof(gs_amm_pending_device_io_history));
    securec_check(rc, "\0", "\0");

    if (t_thrd.proc_cxt.DataDir == NULL || t_thrd.proc_cxt.DataDir[0] == '\0')
        return;
    rc = snprintf_s(wal_path, sizeof(wal_path), sizeof(wal_path) - 1, "%s/%s", t_thrd.proc_cxt.DataDir, XLOGDIR);
    if (rc < 0)
        return;
    wal_directory = wal_path;
    if (ENABLE_DSS && SS_XLOGDIR != NULL && SS_XLOGDIR[0] != '\0')
        wal_directory = SS_XLOGDIR;

    if (!gs_amm_add_device_for_path(t_thrd.proc_cxt.DataDir, devices, &device_count) ||
        !gs_amm_add_device_for_path(wal_directory, devices, &device_count) || device_count == 0 ||
        !gs_amm_read_diskstats(devices, device_count, io_ms, &in_flight))
        return;

    now = GetCurrentTimestamp();
    sample->available = true;
    sample->in_flight = in_flight;
    if (gs_amm_history_matches(&gs_amm_committed_device_io_history, devices, device_count) &&
        now > gs_amm_committed_device_io_history.sampled_at) {
        TimestampTz elapsed_us = now - gs_amm_committed_device_io_history.sampled_at;
        double elapsed_seconds = (double)elapsed_us / 1000000.0;

        for (int index = 0; index < device_count; index++) {
            if (io_ms[index] < gs_amm_committed_device_io_history.io_ms[index]) {
                sample->available = false;
                break;
            }
            sample->io_ms_rate +=
                (double)(io_ms[index] - gs_amm_committed_device_io_history.io_ms[index]) / elapsed_seconds;
        }
    }
    gs_amm_save_history(&gs_amm_pending_device_io_history, devices, device_count, io_ms, now);
#endif
}

void GsAmmCommitDeviceIoSample(void)
{
#ifdef __linux__
    errno_t rc;

    if (!gs_amm_pending_device_io_history.initialized)
        return;
    rc = memcpy_s(&gs_amm_committed_device_io_history, sizeof(gs_amm_committed_device_io_history),
        &gs_amm_pending_device_io_history, sizeof(gs_amm_pending_device_io_history));
    securec_check(rc, "\0", "\0");
    gs_amm_pending_device_io_history.initialized = false;
#endif
}
