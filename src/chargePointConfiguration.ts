// The OCPP 1.6 configuration keys this station answers GetConfiguration with
// and lets ChangeConfiguration write. It is deliberately small: the keys a CSMS
// actually reads or sets when it adopts a charger (Spark/Cosmos set
// MeterValueSampleInterval and HeartbeatInterval right after boot), plus the
// read-only ones upstream always reported. The two interval keys have side
// effects, applied by the ChangeConfiguration handler, so the station re-times
// a running MeterValues loop or heartbeat instead of only echoing the value.
import { countFromEnv } from "./utils";

export interface ConfigurationEntry {
  value: string;
  readonly: boolean;
}

export const METER_VALUE_SAMPLE_INTERVAL_KEY = "MeterValueSampleInterval";
export const HEARTBEAT_INTERVAL_KEY = "HeartbeatInterval";

export class ChargePointConfiguration {
  private entries = new Map<string, ConfigurationEntry>();

  constructor(meterIntervalSeconds: number, heartbeatIntervalSeconds = 300) {
    this.entries.set("SupportedFeatureProfiles", {
      value:
        "Core,FirmwareManagement,LocalAuthListManagement,Reservation,SmartCharging,RemoteTrigger",
      readonly: true,
    });
    this.entries.set("ChargeProfileMaxStackLevel", {
      value: "99",
      readonly: true,
    });
    this.entries.set("GetConfigurationMaxKeys", {
      value: "99",
      readonly: true,
    });
    this.entries.set("NumberOfConnectors", {
      value: String(countFromEnv("CONNECTORS")),
      readonly: true,
    });
    this.entries.set(HEARTBEAT_INTERVAL_KEY, {
      value: String(heartbeatIntervalSeconds),
      readonly: false,
    });
    this.entries.set(METER_VALUE_SAMPLE_INTERVAL_KEY, {
      value: String(meterIntervalSeconds),
      readonly: false,
    });
    this.entries.set("MeterValuesSampledData", {
      value:
        "Energy.Active.Import.Register,Power.Active.Import,Current.Import,SoC",
      readonly: false,
    });
    this.entries.set("ConnectionTimeOut", { value: "60", readonly: false });
    this.entries.set("AuthorizeRemoteTxRequests", {
      value: String(process.env.AUTHORIZE_REMOTE_TX_REQUESTS === "true"),
      readonly: false,
    });
  }

  get(key: string): ConfigurationEntry | undefined {
    return this.entries.get(key);
  }

  // Unknown keys are stored too (upstream always answered Accepted, and a
  // CSMS that sets a vendor key expects it to read back), so the only refusal
  // is a read-only key.
  set(key: string, value: string): "Accepted" | "Rejected" {
    const existing = this.entries.get(key);
    if (existing?.readonly) {
      return "Rejected";
    }
    this.entries.set(key, { value, readonly: false });
    return "Accepted";
  }

  all(): { key: string; readonly: boolean; value: string }[] {
    return Array.from(this.entries.entries()).map(([key, entry]) => ({
      key,
      readonly: entry.readonly,
      value: entry.value,
    }));
  }

  // The interval a key carries, in seconds, or null when it is not a usable
  // positive number.
  seconds(key: string): number | null {
    const value = Number(this.entries.get(key)?.value);
    return Number.isFinite(value) && value > 0 ? value : null;
  }
}
