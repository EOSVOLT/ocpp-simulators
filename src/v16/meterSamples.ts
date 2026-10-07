// The sampled values one periodic MeterValues carries.
//
// Spark's telemetry ingest (Ocpp16MeterValueParser) reads the energy register
// in Wh, the power in W and the state of charge, and has to skip every sampled
// value carrying a `phase`: a per-phase Current.Import is a per-phase reading,
// never the total. Nothing else in the stack exercises that rule, which is
// why the phase samples are on by default for 1.6. METER_PHASE_SAMPLES=false
// restores upstream's single Energy.Active.Import.Register sample in kWh.
import { LEGACY_RATE_W, type TransactionId } from "../transactionManager";
import type { VCP } from "../vcp";

export const PHASE_SAMPLES_ENABLED =
  process.env.METER_PHASE_SAMPLES !== "false";

// Without a battery simulated on the connector (admin /soc), SoC is reported
// as if a 60 kWh battery started the session at 20 %, as it always was.
const BATTERY_WH = 60_000;
const SOC_START_PERCENT = 20;
const PHASE_VOLTAGE = 230;

// The subset of the MeterValueSchema sampled value this station produces,
// typed as literals so the handler's zod-inferred tuple accepts it.
interface SampledValue {
  value: string;
  context?: "Sample.Periodic";
  format?: "Raw";
  measurand:
    | "Energy.Active.Import.Register"
    | "Power.Active.Import"
    | "Current.Import"
    | "SoC";
  phase?: "L1" | "L2" | "L3";
  location?: "Outlet" | "EV";
  unit: "Wh" | "kWh" | "W" | "A" | "Percent";
}

type SampledValues = [SampledValue, ...SampledValue[]];

export const sampledValues = (
  vcp: VCP,
  transactionId: TransactionId,
  meterValueWh: number,
): SampledValues => {
  const snapshot = vcp.transactionManager.snapshot(transactionId);
  const common = { context: "Sample.Periodic", format: "Raw" } as const;
  // A simulated battery's SoC is reported whatever the sample set, since it
  // was asked for; the legacy fixed curve only rides along with the phases.
  const simulatedSoc: SampledValue[] =
    snapshot?.socPercent != null
      ? [
          {
            value: String(Math.floor(snapshot.socPercent)),
            ...common,
            measurand: "SoC",
            location: "EV",
            unit: "Percent",
          },
        ]
      : [];
  if (!PHASE_SAMPLES_ENABLED) {
    return [
      {
        value: (meterValueWh / 1000).toString(),
        measurand: "Energy.Active.Import.Register",
        unit: "kWh",
      },
      ...simulatedSoc,
    ];
  }
  const powerW =
    snapshot?.powerW ?? vcp.transactionManager.chargingPowerW ?? LEGACY_RATE_W;
  // Floored for a simulated battery so 100 means full, never 99.5.
  const soc =
    snapshot?.socPercent != null
      ? Math.floor(snapshot.socPercent)
      : Math.round(
          Math.min(100, SOC_START_PERCENT + (meterValueWh / BATTERY_WH) * 100),
        );
  const perPhaseA = Math.round((powerW / 3 / PHASE_VOLTAGE) * 10) / 10;
  const energy: SampledValue = {
    value: String(Math.round(meterValueWh)),
    ...common,
    measurand: "Energy.Active.Import.Register",
    location: "Outlet",
    unit: "Wh",
  };
  const power: SampledValue = {
    value: String(Math.round(powerW)),
    ...common,
    measurand: "Power.Active.Import",
    location: "Outlet",
    unit: "W",
  };
  const stateOfCharge: SampledValue = {
    value: String(soc),
    ...common,
    measurand: "SoC",
    location: "EV",
    unit: "Percent",
  };
  const phases: SampledValue[] = (["L1", "L2", "L3"] as const).map((phase) => ({
    value: String(perPhaseA),
    ...common,
    measurand: "Current.Import",
    phase,
    location: "Outlet",
    unit: "A",
  }));
  return [energy, power, stateOfCharge, ...phases];
};
