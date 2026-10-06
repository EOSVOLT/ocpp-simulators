// Get a station past Cosmos's door by writing its charge box document.
//
// Cosmos refuses the WebSocket handshake for a charge box it does not know
// (`allowedToConnect` looks the id up in the `chargeboxes` collection, insists
// on status "open" and checks the Authorization header against
// `authorizationKey` when one is set), and refuses a StatusNotification for a
// connector missing from the document with a SecurityError.
//
// With cosmos-hub running, as in the local docker stack, Spark registers the
// charger itself: creating a charger row with this serial makes Spark call the
// hub's POST chargebox / PATCH chargebox/connector and the document appears.
// This script is only a shortcut for a bare Cosmos node, or for a station that
// does not exist in Spark yet and should still be allowed to connect.
//
// The shape is what Cosmos's ChargeBox entity reads: `chargeBox` upper cased
// (the entity's constructor and the WebSocket server both upper case, so a
// lower case document is never found), `maxKw` in watts despite the name, and
// a `connectors` map whose entries carry the live `used`/`status` the node
// keeps on them. Idempotent: an existing document is updated in place.
//
//   npm run register                                 # CP_ID from .env
//   CP_ID=SIM-0007 CONNECTORS=2 npm run register
//   MONGO_URI=mongodb://host.docker.internal:27017/cosmos npm run register
require("dotenv").config();

import { MongoClient } from "mongodb";

const mongoUri = process.env.MONGO_URI ?? "mongodb://localhost:27017/cosmos";
const chargeBox = (process.env.CP_ID ?? "123456").toUpperCase();
const connectorCount = Math.max(
  1,
  Number.parseInt(process.env.CONNECTORS ?? "1") || 1,
);
const maxWatts = Number.parseInt(process.env.MAX_WATTS ?? "22000") || 22000;
const intervalSeconds =
  Number.parseInt(process.env.HEARTBEAT_INTERVAL ?? "300") || 300;
const authorizationKey = process.env.AUTH_KEY ?? "";

async function main(): Promise<void> {
  const connectors: Record<
    string,
    { maxKw: number; used: number; status: string }
  > = {};
  for (let connectorId = 1; connectorId <= connectorCount; connectorId++) {
    connectors[String(connectorId)] = {
      maxKw: maxWatts,
      used: 0,
      status: "Available",
    };
  }
  const document = {
    chargeBox,
    authorizationKey,
    interval: intervalSeconds,
    maxKw: maxWatts,
    connectors,
    status: "open",
  };
  const client = new MongoClient(mongoUri);
  try {
    await client.connect();
    const result = await client
      .db()
      .collection("chargeboxes")
      .updateOne(
        { chargeBox },
        { $set: document, $setOnInsert: { timestamp: new Date() } },
        { upsert: true },
      );
    console.log(
      `${result.upsertedCount > 0 ? "inserted" : "updated"} chargeboxes/${chargeBox} in ${mongoUri}: ${JSON.stringify(document)}`,
    );
  } finally {
    await client.close();
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});
