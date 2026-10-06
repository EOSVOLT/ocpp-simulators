import { z } from "zod";
import { type OcppCall, OcppIncoming } from "../../ocppMessage";
import type { VCP } from "../../vcp";

const GetConfigurationReqSchema = z.object({
  key: z.array(z.string().max(50)).nullish(),
});
type GetConfigurationReqType = typeof GetConfigurationReqSchema;

const GetConfigurationResSchema = z.object({
  configurationKey: z
    .array(
      z.object({
        key: z.string().max(50),
        readonly: z.boolean(),
        value: z.string().max(500).nullish(),
      }),
    )
    .nullish(),
  unknownKey: z.array(z.string().max(50)).nullish(),
});
type GetConfigurationResType = typeof GetConfigurationResSchema;

class GetConfigurationOcppMessage extends OcppIncoming<
  GetConfigurationReqType,
  GetConfigurationResType
> {
  reqHandler = async (
    vcp: VCP,
    call: OcppCall<z.infer<GetConfigurationReqType>>,
  ): Promise<void> => {
    // No key list means everything; otherwise only the asked-for keys, with
    // the ones this station does not have reported in unknownKey.
    const requested = call.payload.key ?? [];
    if (requested.length === 0) {
      vcp.respond(
        this.response(call, {
          configurationKey: vcp.configuration.all(),
          unknownKey: [],
        }),
      );
      return;
    }
    const configurationKey = [];
    const unknownKey = [];
    for (const key of requested) {
      const entry = vcp.configuration.get(key);
      if (entry) {
        configurationKey.push({ key, ...entry });
      } else {
        unknownKey.push(key);
      }
    }
    vcp.respond(this.response(call, { configurationKey, unknownKey }));
  };
}

export const getConfigurationOcppMessage = new GetConfigurationOcppMessage(
  "GetConfiguration",
  GetConfigurationReqSchema,
  GetConfigurationResSchema,
);
