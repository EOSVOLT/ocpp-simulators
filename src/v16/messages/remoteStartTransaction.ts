import { z } from "zod";
import { logger } from "../../logger";
import { type OcppCall, OcppIncoming } from "../../ocppMessage";
import { delay } from "../../utils";
import type { VCP } from "../../vcp";
import {
  ChargingProfileSchema,
  ConnectorIdSchema,
  IdTokenSchema,
} from "./_common";
import { authorizeOcppMessage } from "./authorize";
import { startTransactionOcppMessage } from "./startTransaction";
import { statusNotificationOcppMessage } from "./statusNotification";

const RemoteStartTransactionReqSchema = z.object({
  connectorId: ConnectorIdSchema.nullish(),
  idTag: IdTokenSchema,
  chargingProfile: ChargingProfileSchema.nullish(),
});
type RemoteStartTransactionReqType = typeof RemoteStartTransactionReqSchema;

const RemoteStartTransactionResSchema = z.object({
  status: z.enum(["Accepted", "Rejected"]),
});
type RemoteStartTransactionResType = typeof RemoteStartTransactionResSchema;

class RemoteStartTransactionOcppMessage extends OcppIncoming<
  RemoteStartTransactionReqType,
  RemoteStartTransactionResType
> {
  reqHandler = async (
    vcp: VCP,
    call: OcppCall<z.infer<RemoteStartTransactionReqType>>,
  ): Promise<void> => {
    // Fault injection: simulate a charger that fails to start so the backend's
    // authorization-recapture path can be exercised. In every fail mode the
    // connector is left Available (no StartTransaction, no Charging status).
    const failMode = vcp.remoteStartFailMode;
    if (failMode && failMode !== "off") {
      if (failMode === "ignore") {
        logger.warn(
          "RemoteStartTransaction ignored (fail mode=ignore): no response sent, connector stays Available",
        );
        return; // send nothing -- backend times out and must recapture
      }
      if (failMode === "reject") {
        logger.warn(
          "RemoteStartTransaction rejected (fail mode=reject): connector stays Available",
        );
        vcp.respond(this.response(call, { status: "Rejected" }));
        return;
      }
      if (failMode === "accept_no_start") {
        logger.warn(
          "RemoteStartTransaction accepted but not started (fail mode=accept_no_start): connector stays Available",
        );
        vcp.respond(this.response(call, { status: "Accepted" }));
        return; // no StartTransaction, no Charging status
      }
    }
    if (!call.payload.connectorId) {
      if (process.env.CONNECTORLESS_FLOW_CONNECTOR_ID) {
        call.payload.connectorId = Number(
          process.env.CONNECTORLESS_FLOW_CONNECTOR_ID,
        );
        logger.info(
          `RemoteStartTransaction has no connectorId - using the preconfigured CONNECTORLESS_FLOW_CONNECTOR_ID=${call.payload.connectorId}`,
        );
      } else {
        logger.warn(
          "Rejecting RemoteStartTransaction: no connectorId in the request. Set CONNECTORLESS_FLOW_CONNECTOR_ID to accept it on a fixed connector.",
        );
        vcp.respond(this.response(call, { status: "Rejected" }));
        return;
      }
    }
    if (
      !vcp.transactionManager.canStartNewTransaction(call.payload.connectorId)
    ) {
      logger.warn(
        `Rejecting RemoteStartTransaction: connector ${call.payload.connectorId} already has an ongoing transaction.`,
      );
      vcp.respond(this.response(call, { status: "Rejected" }));
      return;
    }
    vcp.respond(this.response(call, { status: "Accepted" }));
    // The answer and the action run on separate clocks (see VCP.actDelayMs):
    // a real charger accepts long before the cable is energised.
    const connectorId = call.payload.connectorId;
    await delay(vcp.actDelayMs);
    if (vcp.configuration.get("AuthorizeRemoteTxRequests")?.value === "true") {
      vcp.send(authorizeOcppMessage.request({ idTag: call.payload.idTag }));
    }
    vcp.send(
      startTransactionOcppMessage.request({
        connectorId,
        idTag: call.payload.idTag,
        meterStart: 0,
        timestamp: new Date().toISOString(),
      }),
    );
    vcp.send(
      statusNotificationOcppMessage.request({
        connectorId,
        errorCode: "NoError",
        status: "Charging",
      }),
    );
  };
}

export const remoteStartTransactionOcppMessage =
  new RemoteStartTransactionOcppMessage(
    "RemoteStartTransaction",
    RemoteStartTransactionReqSchema,
    RemoteStartTransactionResSchema,
  );
