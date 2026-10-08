# Spark and Cosmos notes

Reference for running these simulators against EOSVOLT's stack (charger, Cosmos, Spark, Cosmos, charger)
without hardware. Setup and everyday use are in the [README](../README.md).

## Pointing a station at the local stack

Cosmos listens on `ws://localhost:9000` and routes by the station id in the path,
so a station connects to `ws://localhost:9000/<CP_ID>` with the `ocpp1.6` subprotocol. The env
file for one station:

```bash
WS_URL=ws://localhost:9000
CP_ID=SIM-0002
ADMIN_PORT=9910
CONNECTORS=1
CONTINUE_ON_UNKNOWN_MESSAGE_ID=true   # see "Cosmos behaviours" below
```

```bash
npm start sim2 index_16.ts            # reads .env.sim2
curl -s localhost:9910/health         # {"status":"OK","cpId":"SIM-0002","connected":true,"delays":{...},"meter":{...}}
```

**Registration.** Cosmos refuses the WebSocket handshake for a charge box it does not know, and
refuses a StatusNotification for a connector missing from the charge box document. With cosmos-hub
running (as in Spark's docker stack) **Spark registers the charger itself**: create a
charger in Spark's panel whose serial equals `CP_ID` and the `chargeboxes` document appears. For a
bare Cosmos node, or a station that should connect before it exists in Spark, there is a shortcut
that upserts the document straight into Cosmos's Mongo (`chargeBox` upper cased, `status: "open"`,
`maxKw` in watts despite the name, one `connectors` entry per connector):

```bash
CP_ID=SIM-0007 CONNECTORS=2 npm run register                      # mongodb://localhost:27017/cosmos
MONGO_URI=mongodb://host.docker.internal:27017/cosmos npm run register
```

Spark only attaches a station's messages to anything when it has a `chargers` row with that
`serial_id`. Without one the ingest answers a rejection that is correct and not a simulator fault:
`transactionId` 0 for StartTransaction, `Invalid` for Authorize, `{}` for everything else, while
boot and heartbeat look perfectly healthy. A StartTransaction gets an allocated id only when Spark
holds an `ACCEPTED` session on that plug for that idTag, which is why the `remote-start` scenario is
the one that exercises the full path. Two things on Spark's side that are easy to miss:
`CHARGING_FAKE_CHARGER_CONTROL=false` and a running queue worker (RemoteStart and RemoteStop are
queued jobs).

**Hub stub.** A bare Cosmos node (`node ace serve` from the CoSMos checkout) asks its `HUB`
whether the charge box is online elsewhere before it serves. `npm run hub-stub` answers
`GET /session/online/:chargeBox` with `{"state":[false,""]}` on `http://127.0.0.1:3009/`; set
`HUB=http://localhost:3009/` in Cosmos's `.env`. Not needed with the docker stack's real hub.

## The two delay clocks

Every station has two independent, runtime-settable delays, defaulting to `REPLY_DELAY_MS=0` and
`ACT_DELAY_MS=1000`:

```bash
curl -s localhost:9910/delays                                              # {"replyMs":0,"actMs":1000}
curl -s -X POST localhost:9910/delays -H 'content-type: application/json' -d '{"replyMs":12000}'
curl -s -X POST localhost:9910/delays -H 'content-type: application/json' -d '{"replyMs":0,"actMs":1000}'
```

- **Reply delay** holds every CALLRESULT and CALLERROR to a CSMS command (RemoteStart, RemoteStop,
  Reset, TriggerMessage, all of them) for that long before sending it, and nothing else: heartbeats,
  status reports and meter values keep flowing, so the socket stays alive. It reproduces a connected
  but slow charger. A 12 s reply delay sets it past Spark's `COSMOS_TIMEOUT` (8 s by
  default, 10 s in the dev env): Spark reports the command `CosmosUnreachable`, fails the session and
  releases the hold, while the station still accepts the RemoteStart at second 12 and its
  StartTransaction is then refused with transaction id 0. A read timeout on a command never counts
  against Spark's circuit breaker, which this is the easiest way to see.
- **Act delay** is how long the station waits after receiving RemoteStart or RemoteStop before doing
  anything about it (Authorize when `AuthorizeRemoteTxRequests` is `true`, StartTransaction, the
  status reports; or StopTransaction). Default 1 s, which is what a real charger does after
  answering. A 12 s act delay makes Spark fail the session at its timeout before the charger has
  done anything, so the late StartTransaction finds no accepted session and is refused with
  transaction id 0.

The two are independent, so both orders a real charger produces can be reproduced: answer first then
act (the usual), or hold the answer and act at once, which is how a StartTransaction reaches Spark
while the RemoteStart is still unanswered.

## Meter values

While a transaction runs the station sends one MeterValues per `MeterValueSampleInterval` (15 s by
default; a `ChangeConfiguration` of that key or of `HeartbeatInterval` re-times the running loop at
once, and `GetConfiguration` reports the current values). Each report carries
`Energy.Active.Import.Register` in Wh, `Power.Active.Import` in W and one `Current.Import` sample
per phase (L1, L2, L3), plus `SoC` in percent while a battery is simulated (`POST /soc`). The phase samples are there on purpose: Spark's
`Ocpp16MeterValueParser` must skip every sampled value carrying a `phase`, and nothing else in the
stack exercises that. `METER_PHASE_SAMPLES=false` restores upstream's single kWh register. The energy
follows the charging power set through `POST /charging-power {"kw": 11}` (energy = power × time); with
no power set the legacy synthetic rate (10 Wh/s) applies. `POST /charging-power {"fluctuate": true}` makes the
power wander between 82 % and 98 % of the set value, and `POST /soc {..., "curve": true}` tapers it as
the battery fills (AC up to 22 kW: full power to 80 %, then down to a tenth at 100 %; DC eases off
from 50 % already).

```bash
curl -s localhost:9910/meter                                                           # {"auto":true,"intervalMs":15000,"kw":null}
curl -s -X POST localhost:9910/meter -H 'content-type: application/json' -d '{"auto":false,"intervalSeconds":10}'
curl -s -X POST localhost:9910/meter-tick -H 'content-type: application/json' -d '{"connectorId":1}'   # one MeterValues now
```

## Admin API additions

| Endpoint | |
| --- | --- |
| `GET /health` | JSON: `cpId`, `connected`, `offline` (held closed by `/disconnect`), `delays`, `meter`, open `transactions` |
| `GET` / `POST /delays` | the two clocks, `{"replyMs", "actMs"}`, either optional on POST |
| `GET /frames?limit=N` | the last N OCPP frames in and out (500 kept) and `lastReply`, the CSMS's last CALLRESULT/CALLERROR to one of the station's own calls |
| `GET` / `POST /meter` | periodic MeterValues `auto` on/off and `intervalSeconds` |
| `POST /meter-tick` | one MeterValues now for `{"connectorId"}` (409 without a transaction) |
| `POST /execute-sync` | like `/execute`, but waits for and returns the CSMS's reply to that exact call |
| `GET /transactions` | the open transactions with their live `meterWh` |
| `POST /disconnect` / `POST /connect` | close the OCPP socket and keep it closed (no auto-reconnect; the process and the admin API stay up) / reopen it with a fresh BootNotification; `{"ok", "connected"}`, 502 when the CSMS refuses |
| `/charging-power`, `/charge-target`, `/fail-mode`, `/fault`, `/ws-url`, `/restart` | the fleet's fault injection and speed controls (see `src/vcp.ts`) |

## Scenarios

A scenario is a JSON file in `scenarios/` that one station walks through in order against a
running VCP's admin API, recording Spark's reply to every call. `expect` compares the last reply
against a subset of fields and `expectTransactionId` asserts an allocated id (or a refusal); a
mismatch fails the run with exit code 1.

```bash
npm run scenario -- scenarios/boot.json --admin http://localhost:9910
npm run scenario -- scenarios/session.json --admin http://localhost:9910 --id-tag AABBCCDD
npm run scenario -- scenarios/remote-start.json --admin http://localhost:9910   # then start/stop the session in Spark
```

Steps: `boot`, `heartbeat`, `status`, `authorize`, `startTransaction`, `expectTransactionId`,
`meterValues` (`seconds`, `intervalSeconds`, `powerW`), `stopTransaction`, `session` (the whole
sequence in one step), `wait`, `waitForCommand` (`action`, `timeoutSeconds`; blocks until the CSMS
sends that command), `expect`, `disconnect` (`POST /disconnect`: the socket closes and stays
closed) and `connect` (`POST /connect`, then waits for `/health` to report the socket open; the
station boots again by itself). `--id-tag` rewrites the idTag in every step. The `fleet`
scenario runs one station per invocation and measures no round-trip percentiles; run a fleet with
`run_simulators.sh` and point the runner at each admin port.

## Going offline

**Go offline** closes the station's OCPP socket and nothing else: the process and its admin API stay up,
the station's own auto-restart is held off, and the panel's restart loop never fires because the process
never exits, so the station stays offline until **Go online** or **Restart**. In Spark this is a charger
dropping off the network: Cosmos raises its Disconnect event, the charger turns offline and any session on it
is left to Spark's own handling. Go online reopens the socket and the station boots as on a fresh start.
Restart is the heavier tool: it exits the process, which reloads `.env` and code and reconnects in about 3 s.

`AUTO_EXCLUDE_CP_IDS` names the test chargers: the transaction scheduler never auto-cycles them and their
pages get the test controls. `AUTO_EXCLUDE_CP_IDS=*` makes every station a test charger, which is what a
local stack wants (the Docker image sets it).

## Adding and removing stations

The panel is also a process manager. The "Add station" form in the sidebar (or
`POST /api/sims` with `{"cpId": "SIM-0004", "connectors": 1, "wsUrl": "ws://..."}`, `wsUrl`
optional) writes `.env.sim<N>` into `SIM_PROFILES_DIR` with the lowest free `N`, the lowest free
`ADMIN_PORT` from `SIM_ADMIN_PORT_BASE` (9901) upwards, `WS_URL` from the request or the panel's
own `WS_URL` (`ws://localhost:9000`), and `TOKEN`, `METER_PHASE_SAMPLES`, `ACT_DELAY_MS`,
`REPLY_DELAY_MS`, `AUTHORIZE_REMOTE_TX_REQUESTS` from the panel's environment with the usual
defaults. It then runs the station the way `run_one_sim.sh` does (`node --env-file` with the tsx
loader, `AUTO_RESTART=true`, output appended to `SIM_LOG_DIR/sim<N>.log`) and relaunches it 3 s
after any exit, so the station page's Restart button and the OCPP URL override, both of which exit
the process, keep working. 201 with the station, 409 for a duplicate id (case-insensitive, Cosmos
upper-cases ids), 400 for a bad body. The id has to exist as a charger in Spark with that serial:
Spark registers it with Cosmos through cosmos-hub, and until then Cosmos refuses the handshake and
the station keeps retrying.

Remove on the station page (or `DELETE /api/sims/<sim id or CP_ID>`) stops the loop, ends the process
(SIGTERM, SIGKILL after 5 s), deletes the profile and keeps the log. At startup the panel starts a
process for every profile already in `SIM_PROFILES_DIR`, which is how the Spark dev container's
stations survive a restart. A profile whose admin port already answers belongs to a shell script's
loop: the panel lists it with `managed: false`, leaves it alone and refuses to remove it, so
`run_simulators.sh` or `run_one_sim.sh` on the same host keep working next to it. `GET /api/sims`
reports `managed`, `pid`, `profile` and `log` per station. Stopping the panel (SIGINT or SIGTERM)
stops the stations it manages.

In Spark's dev stack (`docker compose --profile simulator up`) the container starts with no
stations, `SIM_PROFILES_DIR=/app/profiles`, `SIM_LOG_DIR=/app/logs`, `WS_URL` set to its Cosmos node
and `AUTO_EXCLUDE_CP_IDS=*`; stations are added from the panel on `:8190`. Setting
`OCPP_SIM_STATIONS` still pre-provisions profiles at start, in the same shape, for ids without one.

## Fleet scripts

`run_simulators.sh` launches `NUM_SIMS` stations from `.env.sim1..N` under a crash-restart loop
(`WS_MODE=shared` routes every station through one host by `CP_ID`; set `SHARED_WS_URL` to that
host, e.g. `wss://csms.example.com`, or leave it empty for each profile's own `WS_URL`;
`WS_MODE=ports` needs `WS_HOST`). `run_one_sim.sh 11` runs a single profile the same way.
Both write `logs/sim<N>.log`, which the panel tails.

## Cosmos behaviours that affect a simulator

- **A status is forwarded only when it differs** from the one Cosmos holds in memory for that
  connector, so Spark never sees a repeated StatusNotification. Send a different status first if a
  repeat is the point.
- **An internal failure is answered with a CALLERROR carrying a fresh uuid**, not the id of the
  call that failed (Spark answering 401, a thrown handler). A real charger cannot match it and waits
  for its own timeout; upstream's `CONTINUE_ON_UNKNOWN_MESSAGE_ID=true` keeps the VCP alive instead
  of throwing on the unknown id, and `/frames` shows it as the `lastReply`.
- **The boot sanitiser strips every character outside `[A-Za-z0-9_-]`** from BootNotification
  strings before forwarding: firmware `1.0.0` reaches Spark as `100`, a model with spaces loses
  them. Cosmos answers the BootNotification itself, so Spark can neither reject a boot nor set the
  heartbeat interval from there.
- **The hub sanitises station ids the same way**, so keep `CP_ID` to that alphabet (`SIM-0002`,
  not `SIM.0002`), or Spark's commands name a station Cosmos does not hold.
- **Messages are rate limited per station per minute**, excluding StartTransaction and
  StopTransaction, so a MeterValues cadence of a second or two across many stations trips it; keep
  `MeterValueSampleInterval` at a realistic 10 to 15 s for a fleet.
- **A second socket for a station that is already online is refused** (the node asks the hub), so
  a reconnect right after a drop earns a 403: the auto-restart's 3 s pause is there for that.
- Cosmos calls Spark synchronously inside the Authorize and StartTransaction handlers, so Spark's
  own latency is what the station sees on those two; StatusNotification, MeterValues and Heartbeat
  are forwarded on a queue and acknowledged by Cosmos itself.


## Docker image

The root `Dockerfile` builds the fleet image: the web control panel plus the stations it manages,
in one container, started by `docker/entrypoint.sh` (sets `SIM_PROFILES_DIR=/app/profiles` and
`SIM_LOG_DIR=/app/logs`, pre-creates the stations in `OCPP_SIM_STATIONS` if any, marks every
station a test charger so the scheduler never auto-cycles it, and starts the panel on `:8080`).
Mount `/app/profiles` and `/app/logs` to keep stations across restarts, and set `WS_URL` to the
Cosmos node (`ws://<cosmos host>:9000`; Spark's dev stack sets its own service). `Dockerfile.vcp` is upstream's image for one
headless station driven by its admin API. The image is not published anywhere: Spark's dev stack
builds it from a developer's checkout (`docker compose --profile simulator up -d --build` there).
