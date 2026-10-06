# OCPP Virtual Charge Point

Simple, configurable, terminal-based OCPP Charging Station simulator written in Node.js with Schema validation.

## Watch our video introduction

[![VCP Video](https://img.youtube.com/vi/YsXjnk0mhfA/0.jpg)](https://www.youtube.com/watch?v=YsXjnk0mhfA)

## Prerequisites

- Node.js 12+

Run:

```bash
npm install
```

## Running VCP

Configure env variables:

```
WS_URL - websocket endpoint
CP_ID - ID of this VCP
PASSWORD - if used for OCPP Authentication, otherwise can be left blank
```

Optional:

```
TOKEN - token this station authorizes with, substituted into admin commands (see below)
DISABLE_METER_VALUES - set to "true" to stop sending periodic MeterValues for ongoing transactions
CONNECTORLESS_FLOW_CONNECTOR_ID - connector to use when a RemoteStartTransaction arrives without a connectorId
CONNECTORS - number of connectors this VCP reports (defaults to 1)
EVSES - number of EVSEs this VCP reports, 2.0.1 and 2.1 only (defaults to 1)
```

By default a `RemoteStartTransaction` without a `connectorId` is rejected.
Setting `CONNECTORLESS_FLOW_CONNECTOR_ID` makes the VCP accept it on that fixed connector instead.

### Multiple connectors and EVSEs

`CONNECTORS` and `EVSES` make the VCP behave as a multi-connector station. On boot it sends an
`Available` `StatusNotification` for every connector, and a `ChangeAvailability` with
`Inoperative` reports `Unavailable` for every connector the request addresses.

In OCPP 1.6 there are no EVSEs, so only `CONNECTORS` applies: connectors are numbered
`1..CONNECTORS`, and a `ChangeAvailability` for connector `0` (the whole charge point) covers
all of them rather than only connector 1.

In 2.0.1 and 2.1 the two combine into `EVSES` x `CONNECTORS` connectors - `CONNECTORS` is the
number of connectors *per EVSE*, so `EVSES=2 CONNECTORS=2` reports `(1,1) (1,2) (2,1) (2,2)`.
How wide a `ChangeAvailability` fans out depends on how precisely it is addressed:

| Request | Reports `Unavailable` for |
| --- | --- |
| no `evse` | every connector of every EVSE |
| `evse.id` only | every connector of that EVSE |
| `evse.id` + `evse.connectorId` | that one connector |

Run OCPP 1.6:

```bash
npm start index_16.ts
```

Run OCPP 2.0.1:

```bash
npm start index_201.ts
```

When testing different configurations, you can create multiple `.env` files and pass the env file or the env file suffix as an argument, for example:

```bash
# uses .env
npm start .env index_16.ts
# uses .env if exists
npm start index_16.ts
# uses .env.production
npm start .env.production index_16.ts
# uses .env.production
npm start production index_16.ts
```

### Auto-restart

Normally, the VCP will exit after receiving the `Reset` message.
If you want to let the VCP re-establish the WS connection after receiving the `Reset` message, you can use the `npm start:auto-restart` command.

Example:
```bash
WS_URL=ws://localhost:3000 CP_ID=vcp_16_test npm run start:auto-restart index_16.ts

# ...
2026-03-06 09:55:51 info: Receive message ⬅️  [2,"248a82ba-58e3-4a3d-ae8f-74470add510f","Reset",{"type":"Hard"}]
2026-03-06 09:55:51 info: Responding with ➡️  [3,"248a82ba-58e3-4a3d-ae8f-74470add510f",{"status":"Accepted"}]
2026-03-06 09:55:51 info: Waiting for 3 seconds to close VCP...
2026-03-06 09:55:54 info: Closing VCP
2026-03-06 09:55:54 info: Auto-restart enabled. Closing old VCP...
2026-03-06 09:55:54 info: Waiting for 3 seconds...
2026-03-06 09:55:57 info: Starting new VCP
2026-03-06 09:55:57 info: Connecting... | {
  endpoint: 'ws://localhost:3000',
  chargePointId: 'vcp_16_test',
  ocppVersion: 'OCPP_1.6',
  basicAuthPassword: '123',
  adminPort: 9999
}
# ...
```

## Example

```bash
> WS_URL=ws://localhost:3000 CP_ID=vcp_16_test npm start index_16.ts

2023-03-27 13:09:17 info: Connecting... | {
  endpoint: 'ws://localhost:3000',
  chargePointId: 'vcp_16_test',
  ocppVersion: 'OCPP_1.6',
  basicAuthPassword: 'password',
  adminWsPort: 9999
}
2023-03-27 13:09:17 info: Sending message ➡️  [2,"5fe44756-05e1-4065-9c91-11b456b55913","BootNotification",{"chargePointVendor":"Solidstudio","chargePointModel":"test","chargePointSerialNumber":"S001","firmwareVersion":"1.0.0"}]
2023-03-27 13:09:17 info: Sending message ➡️  [2,"aad8d05d-3a6b-4c51-a9fc-7275d4a6cbc3","StatusNotification",{"connectorId":1,"errorCode":"NoError","status":"Available"}]
2023-03-27 13:09:17 info: Receive message ⬅️  [3,"5fe44756-05e1-4065-9c91-11b456b55913",{"currentTime":"2023-03-27T11:09:17.883Z","interval":30,"status":"Accepted"}]
2023-03-27 13:09:17 info: Receive message ⬅️  [2,"658c8f5b-9f86-487f-91f8-1d656453978a","ChangeConfiguration",{"key":"MeterValueSampleInterval","value":"60"}]
2023-03-27 13:09:17 info: Responding with ➡️  [3,"658c8f5b-9f86-487f-91f8-1d656453978a",{"status":"Accepted"}]
2023-03-27 13:09:17 info: Receive message ⬅️  [2,"34fc4673-deff-48d3-bb8e-d94d75fa619a","GetConfiguration",{"key":["SupportedFeatureProfiles"]}]
2023-03-27 13:09:17 info: Responding with ➡️  [3,"34fc4673-deff-48d3-bb8e-d94d75fa619a",{"configurationKey":[{"key":"SupportedFeatureProfiles","readonly":true,"value":"Core,FirmwareManagement,LocalAuthListManagement,Reservation,SmartCharging,RemoteTrigger"},{"key":"ChargeProfileMaxStackLevel","readonly":true,"value":"99"},{"key":"HeartbeatInterval","readonly":false,"value":"300"},{"key":"GetConfigurationMaxKeys","readonly":true,"value":"99"}]}]
2023-03-27 13:09:17 info: Receive message ⬅️  [3,"aad8d05d-3a6b-4c51-a9fc-7275d4a6cbc3",{}]
2023-03-27 13:09:18 info: Receive message ⬅️  [2,"d7610ad2-63d0-470f-9bd9-6e47d5483429","SetChargingProfile",{"connectorId":0,"csChargingProfiles":{"chargingProfileId":30,"stackLevel":0,"chargingProfilePurpose":"ChargePointMaxProfile","chargingProfileKind":"Absolute","chargingSchedule":{"chargingRateUnit":"A","chargingSchedulePeriod":[{"startPeriod":0,"limit":10.0}]}}}]
2023-03-27 13:09:18 info: Responding with ➡️  [3,"d7610ad2-63d0-470f-9bd9-6e47d5483429",{"status":"Accepted"}]
2023-03-27 13:10:17 info: Sending message ➡️  [2,"79a41b2e-2c4a-4a65-9d7e-417967a8f95f","Heartbeat",{}]
2023-03-27 13:10:17 info: Receive message ⬅️  [3,"79a41b2e-2c4a-4a65-9d7e-417967a8f95f",{"currentTime":"2023-03-27T11:10:17.955Z"}]
```

## Executing Admin Commands

Some messages are automatically sent by the VCP, for example, `BootNotification` or `StartTransaction` and `StopTransaction`.
However, for Operations initiated by Charge Point (compare e.g. with OCPP 1.6, Chapter 4) one can send the messages using `admin` functionality.
VCP exposes a separate Websocket endpoint that will "proxy" all messages to Central System Websocket.
For example usage, see `admin/` folder.

```bash
npx tsx admin/v16/Authorize/authorize.ts
```

### Placeholders in admin commands

The commands in `admin/` are shared across charge points, so they cannot hardcode a station's token or know the id of a transaction that is already running.
Instead they send placeholders, which the VCP substitutes from its own state just before the message goes out.
Substitution happens in the VCP process — the admin command only proxies the payload to it — so `TOKEN` belongs in the env file the VCP was started with, not on the admin command:

| Placeholder | Substituted with |
| --- | --- |
| token `__TOKEN__` | the `TOKEN` env var |
| `transactionId` of `0` (or `"0"` in 2.0.1/2.1) | the id of the ongoing transaction |

Both are best-effort and never guess:

- A token other than `__TOKEN__` is sent as-is, so a command that spells out a real token keeps working. If `TOKEN` is not set, the placeholder is sent unchanged — the Central System then rejects a recognisable value instead of the command silently authorizing as someone else.
- A `transactionId` is only resolved when there is exactly one ongoing transaction. With none, or more than one, the `0` is sent unchanged and the Central System decides how to respond. Set `TRANSACTION_ID` to target a specific transaction.

```bash
# .env.platform-dev.my-station
WS_URL=ws://localhost:3000
CP_ID=my-station
TOKEN=AABBCCDD
```

```bash
# the station is started with that env file...
npm start platform-dev.my-station index_16.ts

# ...then the same commands work against any station: the transaction starts
# with that station's TOKEN and stops without its id having to be looked up
npx tsx admin/v16/Transaction/startTransaction.ts
npx tsx admin/v16/Transaction/stopTransaction.ts
```

---

## Spark and Cosmos (Eosvolt fork)

This fork is Eosvolt's charger simulator for Spark 2.0. Everything upstream documents above still
holds; this section is what was added for driving the real stack — charger → Cosmos → Spark →
Cosmos → charger — without hardware, and for the staging fleet.

### Pointing a station at the local stack

Cosmos (`spark_cosmos`) listens on `ws://localhost:9000` and routes by the station id in the path,
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
running (`spark_cosmos_hub`, as in the docker stack) **Spark registers the charger itself**: create a
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

### The two delay clocks

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
  but slow charger. The panel's "Outlast Spark (12 s)" sets it past Spark's `COSMOS_TIMEOUT` (8 s by
  default, 10 s in the dev env): Spark reports the command `CosmosUnreachable`, fails the session and
  releases the hold, while the station still accepts the RemoteStart at second 12 and its
  StartTransaction is then refused with transaction id 0. A read timeout on a command never counts
  against Spark's circuit breaker, which this is the easiest way to see.
- **Act delay** is how long the station waits after receiving RemoteStart or RemoteStop before doing
  anything about it (Authorize when `AuthorizeRemoteTxRequests` is `true`, StartTransaction, the
  status reports; or StopTransaction). Default 1 s, which is what a real charger does after
  answering. "Slow charger (12 s)" makes Spark fail the session at its timeout before the charger has
  done anything, so the late StartTransaction finds no accepted session and is refused with
  transaction id 0.

The two are independent, so both orders a real charger produces can be reproduced: answer first then
act (the usual), or hold the answer and act at once, which is how a StartTransaction reaches Spark
while the RemoteStart is still unanswered.

### Meter values

While a transaction runs the station sends one MeterValues per `MeterValueSampleInterval` (15 s by
default; a `ChangeConfiguration` of that key or of `HeartbeatInterval` re-times the running loop at
once, and `GetConfiguration` reports the current values). Each report carries
`Energy.Active.Import.Register` in Wh, `Power.Active.Import` in W, `SoC` in percent and one
`Current.Import` sample per phase (L1, L2, L3). The phase samples are there on purpose: Spark's
`Ocpp16MeterValueParser` must skip every sampled value carrying a `phase`, and nothing else in the
stack exercises that. `METER_PHASE_SAMPLES=false` restores upstream's single kWh register. The energy
follows the charging power set through `POST /charging-power {"kw": 11}` (energy = power × time); with
no power set the legacy synthetic rate (10 Wh/s) applies.

```bash
curl -s localhost:9910/meter                                                           # {"auto":true,"intervalMs":15000,"kw":null}
curl -s -X POST localhost:9910/meter -H 'content-type: application/json' -d '{"auto":false,"intervalSeconds":10}'
curl -s -X POST localhost:9910/meter-tick -H 'content-type: application/json' -d '{"connectorId":1}'   # one MeterValues now
```

### Admin API additions

| Endpoint | |
| --- | --- |
| `GET /health` | JSON: `cpId`, `connected`, `delays`, `meter`, open `transactions` |
| `GET` / `POST /delays` | the two clocks, `{"replyMs", "actMs"}`, either optional on POST |
| `GET /frames?limit=N` | the last N OCPP frames in and out (500 kept) and `lastReply`, the CSMS's last CALLRESULT/CALLERROR to one of the station's own calls |
| `GET` / `POST /meter` | periodic MeterValues `auto` on/off and `intervalSeconds` |
| `POST /meter-tick` | one MeterValues now for `{"connectorId"}` (409 without a transaction) |
| `POST /execute-sync` | like `/execute`, but waits for and returns the CSMS's reply to that exact call |
| `GET /transactions` | the open transactions with their live `meterWh` |
| `/charging-power`, `/charge-target`, `/fail-mode`, `/fault`, `/ws-url`, `/restart` | the fleet's fault injection and speed controls (see `src/vcp.ts`) |

### Scenarios

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
sends that command), `expect`. `connect` only waits for the admin API and `disconnect` is a no-op,
because the VCP process owns its socket. `--id-tag` rewrites the idTag in every step. The `fleet`
scenario runs one station per invocation and measures no round-trip percentiles; run a fleet with
`run_simulators.sh` and point the runner at each admin port.

### Web panel

`npm run web` (port `WEB_PORT`, 8080, bound to `WEB_HOST`, 127.0.0.1) serves a control panel over
every `.env.sim<N>` profile in `SIM_PROFILES_DIR` (the repo root by default) and tails
`SIM_LOG_DIR` (`logs/` by default). The page is a sidebar and one main view. The sidebar lists an
Overview entry, every station with a connected/disconnected badge and its up/down state, and the
Add station form; the selection is remembered in the browser and falls back to the Overview when
the station is gone. The Overview holds the fleet: a table of all stations (admin port, connected,
transaction id and meter per connector, who runs the process, pid, the scheduler's mode with Resume
auto), the scheduler's state line, the charging keys editor and the transaction CSV log. A station
page has, top to bottom, a header (connected and process badges, admin port, Restart, Remove, the
active fail-mode and fault badges), a collapsible Settings card with every knob (OCPP URL override,
charging speed, the two delay knobs with their presets, auto meter, RemoteStart fail mode, plug
faults, auto-stop target), an Actions card with Boot, Heartbeat, the quick 0 / 1 kWh transactions
and one card per connector, and a Frames card with the last 200 OCPP frames both ways (Clear hides
the ones seen so far), the CSMS's last reply and the process log.

A connector card is the one place to drive that connector: its live line (the sim's transaction,
idTag and register; the scheduler's mode and running session with its elapsed time), Plug in
(Preparing), Unplug (Available), Send status with a status and errorCode picker, Authorize, Start
transaction, Meter tick and Stop transaction with a reason picker. Start with an empty idTag field
starts through the scheduler (the next free charging key; the session is tracked, timed and written
to the CSV log), Start with a typed idTag sends a raw StartTransaction (`__TOKEN__` means the
station's `TOKEN`). Stop closes a scheduler-tracked session through the scheduler, which returns
the key to the pool and logs the row, and any other live transaction, such as one the app started
with RemoteStart, with a raw StopTransaction quoting the connector's register and the chosen
reason. The fleet features are unchanged: the transaction scheduler with its charging-key pool and
CSV log, and, on the test chargers named in `AUTO_EXCLUDE_CP_IDS`, RemoteStart fail modes, plug
faults, charging speed, quick transactions, auto-stop targets and the OCPP URL override.
`AUTO_EXCLUDE_CP_IDS=*` makes every station a test charger: nothing auto-cycles and every station
page gets those controls, which is what a local stack wants.

#### Adding and removing stations from the panel

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
stations, `SIM_PROFILES_DIR=/app/profiles`, `SIM_LOG_DIR=/app/logs`, `WS_URL=ws://cosmos:9000`
and `AUTO_EXCLUDE_CP_IDS=*`; stations are added from the panel on `:8190`. Setting
`OCPP_SIM_STATIONS` still pre-provisions profiles at start, in the same shape, for ids without one.

### Fleet scripts

`run_simulators.sh` launches `NUM_SIMS` stations from `.env.sim1..N` under a crash-restart loop
(`WS_MODE=shared` routes every station through one host by `CP_ID`; set `SHARED_WS_URL` to that
host, e.g. `wss://csms.example.com`, or leave it empty for each profile's own `WS_URL`;
`WS_MODE=ports` needs `WS_HOST`). `run_one_sim.sh 11` runs a single profile the same way.
Both write `logs/sim<N>.log`, which the panel tails.

### Cosmos behaviours that affect a simulator

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
  `MeterValueSampleInterval` at a realistic 10–15 s for a fleet.
- **A second socket for a station that is already online is refused** (the node asks the hub), so
  a reconnect right after a drop earns a 403: the auto-restart's 3 s pause is there for that.
- Cosmos calls Spark synchronously inside the Authorize and StartTransaction handlers, so Spark's
  own latency is what the station sees on those two; StatusNotification, MeterValues and Heartbeat
  are forwarded on a queue and acknowledged by Cosmos itself.

---

## Docker image (Eosvolt fork)

The root `Dockerfile` builds the fleet image: the web control panel plus the stations it manages,
in one container, started by `docker/entrypoint.sh` (sets `SIM_PROFILES_DIR=/app/profiles` and
`SIM_LOG_DIR=/app/logs`, pre-creates the stations in `OCPP_SIM_STATIONS` if any, marks every
station a test charger so the scheduler never auto-cycles it, and starts the panel on `:8080`).
Mount `/app/profiles` and `/app/logs` to keep stations across restarts, and set `WS_URL` to the
Cosmos node (`ws://cosmos:9000` in Spark's dev stack). `Dockerfile.vcp` is upstream's image for one
headless station driven by its admin API. The image is not published anywhere: Spark's dev stack
builds it from a developer's checkout (`docker compose --profile simulator up -d --build` there).

## Contributing

### Bug Reports & Feature Requests

Please use the [issue tracker](https://github.com/solidstudiosh/ocpp-virtual-charge-point/issues) to report any bugs or file feature requests.

### Developing

We encourage contributions through pull requests and follow the standard "fork-and-pull" git workflow. Feel free to create a fork of the repository, make your changes, and submit a pull request for review. We appreciate your contributions!

1. Fork the repository on GitHub.
2. Clone the forked repository to your local machine.
3. Create a new branch for your changes.
4. Make your changes to the code and commit them to your local branch.
5. Push the changes to your forked repository on GitHub.
6. Create a new pull request on the original repository.
7. Wait for feedback and make any necessary changes.
8. Once your pull request has been reviewed and accepted, it will be merged into the original repository.

When creating your pull request, please include a clear description of the changes you have made, and any relevant context or reasoning behind those changes.
