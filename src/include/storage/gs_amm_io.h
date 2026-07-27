/* -------------------------------------------------------------------------
 *
 * gs_amm_io.h
 *      Native device-I/O sampling for the GS AMM controller.
 *
 * -------------------------------------------------------------------------
 */
#ifndef GS_AMM_IO_H
#define GS_AMM_IO_H

#include "postgres.h"

typedef struct GsAmmDeviceIoSample {
    bool available;
    int in_flight;
    double io_ms_rate;
} GsAmmDeviceIoSample;

extern void GsAmmSampleDeviceIo(GsAmmDeviceIoSample *sample);
extern void GsAmmCommitDeviceIoSample(void);

#endif /* GS_AMM_IO_H */
