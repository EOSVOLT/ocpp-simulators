// The hub Cosmos insists on, stubbed.
//
// A single Cosmos node asks its HUB whether a charge box is online somewhere
// else before it accepts a connection: GET {HUB}session/online/:chargeBox must
// answer {"state":[<online: boolean>, <node: string>]} or the node refuses to
// serve. A docker stack that runs a real cosmos-hub does not need it, so
// this is only for running a bare Cosmos node (`node ace serve` from the
// CoSMos checkout) with no hub at all: it answers "not online anywhere" for
// every id and 404 for anything else, which is what a one-node deployment
// needs. Point Cosmos at it with HUB=http://localhost:3009/ in its .env.
//
//   npm run hub-stub            # port 3009
//   npm run hub-stub -- 3010    # another port; keep HUB in step
import { createServer } from "node:http";

const port = Number(process.argv[2] ?? process.env.HUB_STUB_PORT ?? 3009);
const online = /^\/(?:api\/v1\/)?session\/online\/([^/]+)\/?$/;

const server = createServer((req, res) => {
  const match = req.method === "GET" ? online.exec(req.url ?? "") : null;
  if (match === null) {
    res.writeHead(404, { "content-type": "application/json" });
    res.end(
      JSON.stringify({
        message: "hub stub: only GET /session/online/:chargeBox is served",
      }),
    );
    return;
  }
  res.writeHead(200, { "content-type": "application/json" });
  res.end(JSON.stringify({ state: [false, ""] }));
  console.log(
    `[hub-stub] ${decodeURIComponent(match[1])} -> not online anywhere`,
  );
});

server.listen(port, "127.0.0.1", () => {
  console.log(
    `[hub-stub] answering GET /session/online/:chargeBox on http://127.0.0.1:${port}/`,
  );
});
