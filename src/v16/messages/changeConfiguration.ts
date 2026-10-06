import { z } from "zod";
import {
  HEARTBEAT_INTERVAL_KEY,
  METER_VALUE_SAMPLE_INTERVAL_KEY,
} from "../../chargePointConfiguration";
import { logger } from "../../logger";
import { type OcppCall, OcppIncoming } from "../../ocppMessage";
import type { VCP } from "../../vcp";

const ChangeConfigurationReqSchema = z.object({
  key: z.string().max(50),
  value: z.string().max(500),
});
type ChangeConfigurationReqType = typeof ChangeConfigurationReqSchema;

const ChangeConfigurationResSchema = z.object({
  status: z.enum(["Accepted", "Rejected", "RebootRequired", "NotSupported"]),
});
type ChangeConfigurationResType = typeof ChangeConfigurationResSchema;

class ChangeConfigurationOcppMessage extends OcppIncoming<
  ChangeConfigurationReqType,
  ChangeConfigurationResType
> {
  reqHandler = async (
    vcp: VCP,
    call: OcppCall<z.infer<ChangeConfigurationReqType>>,
  ): Promise<void> => {
    const { key, value } = call.payload;
    const status = vcp.configuration.set(key, value);
    vcp.respond(this.response(call, { status }));
    if (status !== "Accepted") {
      return;
    }
    // The two keys with a side effect are applied at once, re-timing a
    // running loop rather than waiting for the next session or boot. A
    // value that is not a positive number is stored but changes nothing.
    if (key === METER_VALUE_SAMPLE_INTERVAL_KEY) {
      const seconds = vcp.configuration.seconds(key);
      if (seconds !== null) {
        vcp.setMeterIntervalSeconds(seconds);
        logger.info(`MeterValues now every ${seconds} s (ChangeConfiguration)`);
      }
    }
    if (key === HEARTBEAT_INTERVAL_KEY) {
      const seconds = vcp.configuration.seconds(key);
      if (seconds !== null) {
        vcp.configureHeartbeat(seconds * 1000);
        logger.info(`Heartbeat now every ${seconds} s (ChangeConfiguration)`);
      }
    }
  };
}

export const changeConfigurationOcppMessage =
  new ChangeConfigurationOcppMessage(
    "ChangeConfiguration",
    ChangeConfigurationReqSchema,
    ChangeConfigurationResSchema,
  );
