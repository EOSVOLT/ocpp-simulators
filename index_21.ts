require("dotenv").config();

import { OcppVersion } from "./src/ocppVersion";
import { registerVcp } from "./src/close";
import { bootNotificationOcppOutgoing } from "./src/v21/messages/bootNotification";
import { statusNotificationOcppOutgoing } from "./src/v21/messages/statusNotification";
import { countFromEnv } from "./src/utils";
import { VCP } from "./src/vcp";

async function main(): Promise<VCP> {
  const evses = countFromEnv("EVSES");
  const connectors = countFromEnv("CONNECTORS");
  const vcp = new VCP({
    endpoint: process.env.WS_URL ?? "ws://localhost:3000",
    chargePointId: process.env.CP_ID ?? "123456",
    ocppVersion: OcppVersion.OCPP_2_1,
    basicAuthPassword: process.env.PASSWORD ?? undefined,
    adminPort: Number.parseInt(process.env.ADMIN_PORT ?? "9999"),
    // Runs once the socket is open: on this connect and again after an admin
    // /disconnect + /connect.
    boot: (station) => {
      station.send(
        bootNotificationOcppOutgoing.request({
          reason: "PowerUp",
          chargingStation: {
            model: "VirtualChargePoint",
            vendorName: "Solidstudio",
          },
        }),
      );
      for (let evseId = 1; evseId <= evses; evseId++) {
        for (let connectorId = 1; connectorId <= connectors; connectorId++) {
          station.send(
            statusNotificationOcppOutgoing.request({
              evseId,
              connectorId,
              connectorStatus: "Available",
              timestamp: new Date().toISOString(),
            }),
          );
        }
      }
    },
  });
  await vcp.connect();
  return vcp;
}

main().then((vcp) => registerVcp(vcp, main));
