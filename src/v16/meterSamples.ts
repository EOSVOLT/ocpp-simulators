// The sampled values one periodic MeterValues carries.
//
// Spark's telemetry ingest (Ocpp16MeterValueParser) reads the energy register
// in Wh, the power in W and the state of charge, and has to skip every sampled
// value carrying a `phase`: a per-phase Current.Import is a per-phase reading,
// never the total. Nothing else in the stack exercises that rule, which is
// why the phase samples are on by default for 1.6. METER_PHASE_SAMPLES=false
// restores upstream's single Energy.Active.Import.Register sample in kWh.
import type { VCP } from "../vcp";

export const PHASE_SAMPLES_ENABLED =
  process.env.METER_PHASE_SAMPLES !== "false";

// The legacy fixed synthetic rate (transactionManager.getMeterValue with no
// charging power set) is 10 Wh per second, i.e. 36 kW.
const LEGACY_RATE_W = 36_000;
// A 60 kWh battery that starts a session at 20 %.
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
  meterValueWh: number,
): SampledValues => {
  if (!PHASE_SAMPLES_ENABLED) {
    return [
      {
        value: (meterValueWh / 1000).toString(),
        measurand: "Energy.Active.Import.Register",
        unit: "kWh",
      },
    ];
  }
  const powerW = vcp.transactionManager.chargingPowerW ?? LEGACY_RATE_W;
  const soc = Math.min(
    100,
    SOC_START_PERCENT + (meterValueWh / BATTERY_WH) * 100,
  );
  const perPhaseA = Math.round((powerW / 3 / PHASE_VOLTAGE) * 10) / 10;
  const common = { context: "Sample.Periodic", format: "Raw" } as const;
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
    value: String(Math.round(soc)),
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
