require("dotenv").config();

import { OcppVersion } from "./src/ocppVersion";
import { registerVcp } from "./src/close";
import { bootNotificationOcppMessage } from "./src/v16/messages/bootNotification";
import { statusNotificationOcppMessage } from "./src/v16/messages/statusNotification";
import { countFromEnv } from "./src/utils";
import { VCP } from "./src/vcp";
import { readWsUrlOverride } from "./src/wsUrlOverride";

async function main(): Promise<VCP> {
  const connectors = countFromEnv("CONNECTORS");
  const chargePointId = process.env.CP_ID ?? "123456";
  const vcp = new VCP({
    // A persisted per-CP override (set via the /ws-url admin endpoint) wins over
    // the WS_URL baseline from .env, so a repointed charger stays repointed
    // across reconnects and restarts until it's reset.
    endpoint:
      readWsUrlOverride(chargePointId) ??
      process.env.WS_URL ??
      "ws://localhost:3000",
    chargePointId,
    ocppVersion: OcppVersion.OCPP_1_6,
    basicAuthPassword: process.env.PASSWORD ?? undefined,
    adminPort: Number.parseInt(process.env.ADMIN_PORT ?? "9999"),
  });
  await vcp.connect();
  vcp.send(
    bootNotificationOcppMessage.request({
      chargePointVendor: "Solidstudio",
      chargePointModel: "VirtualChargePoint",
      chargePointSerialNumber: "S001",
      firmwareVersion: "1.0.0",
    }),
  );
  for (let connectorId = 1; connectorId <= connectors; connectorId++) {
    vcp.send(
      statusNotificationOcppMessage.request({
        connectorId,
        errorCode: "NoError",
        status: "Available",
      }),
    );
  }
  return vcp;
}

main().then((vcp) => registerVcp(vcp, main));
