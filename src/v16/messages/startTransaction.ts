import { z } from "zod";
import {
  type OcppCall,
  type OcppCallResult,
  OcppOutgoing,
} from "../../ocppMessage";
import { resolveTokenPlaceholder } from "../../tokenPlaceholder";
import type { VCP } from "../../vcp";
import { sampledValues } from "../meterSamples";
import { ConnectorIdSchema, IdTagInfoSchema, IdTokenSchema } from "./_common";
import { meterValuesOcppMessage } from "./meterValues";
import { statusNotificationOcppMessage } from "./statusNotification";
import { stopTransactionOcppMessage } from "./stopTransaction";

const StartTransactionReqSchema = z.object({
  connectorId: ConnectorIdSchema,
  idTag: IdTokenSchema,
  meterStart: z.number().int(),
  reservationId: z.number().int().nullish(),
  timestamp: z.string().datetime(),
});
type StartTransactionReqType = typeof StartTransactionReqSchema;

const StartTransactionResSchema = z.object({
  idTagInfo: IdTagInfoSchema,
  transactionId: z.number().int(),
});
type StartTransactionResType = typeof StartTransactionResSchema;

class StartTransactionOcppMessage extends OcppOutgoing<
  StartTransactionReqType,
  StartTransactionResType
> {
  beforeSend = (
    _vcp: VCP,
    payload: z.infer<StartTransactionReqType>,
  ): z.infer<StartTransactionReqType> => {
    return { ...payload, idTag: resolveTokenPlaceholder(payload.idTag) };
  };

  resHandler = async (
    vcp: VCP,
    call: OcppCall<z.infer<StartTransactionReqType>>,
    result: OcppCallResult<z.infer<StartTransactionResType>>,
  ): Promise<void> => {
    vcp.transactionManager.startTransaction(vcp, {
      transactionId: result.payload.transactionId,
      idTag: call.payload.idTag,
      connectorId: call.payload.connectorId,
      meterValuesCallback: async (transactionState) => {
        // Auto-stop at the armed energy target: the instant the register meets
        // the target, stop the session with meterStop forced to the EXACT
        // target (no overshoot -- e.g. lands on 1.000 kWh, never 1.05). Works
        // for app-initiated (RemoteStart) sessions too. Skip the normal (over-
        // target) MeterValues report on this tick.
        if (
          vcp.chargeTargetWh != null &&
          transactionState.meterValue >= vcp.chargeTargetWh
        ) {
          const meterStop = vcp.chargeTargetWh;
          vcp.send(
            stopTransactionOcppMessage.request({
              transactionId: result.payload.transactionId,
              meterStop,
              reason: "Local",
              timestamp: new Date().toISOString(),
            }),
          );
          vcp.send(
            statusNotificationOcppMessage.request({
              connectorId: call.payload.connectorId,
              errorCode: "NoError",
              status: "Available",
            }),
          );
          // Clear local state + stop the meter timer NOW (don't wait for the
          // StopTransaction.conf) so no further tick fires a duplicate stop.
          // The target is sticky and stays armed for the next session.
          vcp.transactionManager.stopTransaction(result.payload.transactionId);
          return;
        }
        vcp.send(
          meterValuesOcppMessage.request({
            connectorId: call.payload.connectorId,
            transactionId: result.payload.transactionId,
            meterValue: [
              {
                timestamp: new Date().toISOString(),
                sampledValue: sampledValues(
                  vcp,
                  result.payload.transactionId,
                  transactionState.meterValue,
                ),
              },
            ],
          }),
        );
      },
    });
    if (result.payload.idTagInfo.status !== "Accepted") {
      vcp.send(
        stopTransactionOcppMessage.request({
          transactionId: result.payload.transactionId,
          meterStop: 0,
          reason: "DeAuthorized",
          timestamp: new Date().toISOString(),
        }),
      );
      vcp.send(
        statusNotificationOcppMessage.request({
          connectorId: call.payload.connectorId,
          errorCode: "NoError",
          status: "Available",
        }),
      );
      return;
    }
  };
}

export const startTransactionOcppMessage = new StartTransactionOcppMessage(
  "StartTransaction",
  StartTransactionReqSchema,
  StartTransactionResSchema,
);
