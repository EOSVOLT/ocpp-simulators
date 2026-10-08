import * as crypto from "node:crypto";
import * as fs from "node:fs";
import * as path from "node:path";

import dayjs from "dayjs";

interface OCMFInput {
  startTime: Date;
  startEnergy: number;
  endTime: Date;
  endEnergy: number;
  idTag: string;
}

const TM_TIME_FORMAT = "YYYY-MM-DDTHH:mm:ss,SSSZZ";

const generateOCMFData = (input: OCMFInput) => {
  return {
    FV: "1.0",
    GI: "SOLIDSTUDIO METER",
    GS: "90001337",
    GV: "123",
    PG: "T99",
    MV: "SOL",
    MM: "SOL.M.001",
    MS: "1234567890",
    MF: "999",
    IS: true,
    IT: "CENTRAL_2",
    ID: input.idTag,
    CT: "EVSEID",
    CI: "PLSOLE007",
    RD: [
      {
        TM: `${dayjs(input.startTime).format(TM_TIME_FORMAT)} I`,
        TX: "B",
        RV: input.startEnergy.toString(),
        RI: "01-00:98.08.00.FF",
        RU: "kWh",
        RT: "DC",
        EF: "",
        ST: "G",
      },
      {
        TM: `${dayjs(input.endTime).format(TM_TIME_FORMAT)} I`,
        TX: "E",
        RV: input.endEnergy.toString(),
        RI: "01-00:98.08.00.FF",
        RU: "kWh",
        RT: "DC",
        EF: "",
        ST: "G",
      },
    ],
  };
};

// The key pair in cert/ is kept out of the image (.dockerignore), so a container
// has none: fall back to a throwaway secp256k1 pair, generated once per process,
// rather than crashing the station on its first remote stop.
let signingKeys: { privateKey: string; publicKey: Buffer } | null = null;

const loadSigningKeys = () => {
  if (signingKeys) {
    return signingKeys;
  }
  const privatePath = path.resolve("./cert/vcp.pem");
  const publicPath = path.resolve("./cert/vcp.pub");
  if (fs.existsSync(privatePath) && fs.existsSync(publicPath)) {
    signingKeys = {
      privateKey: fs.readFileSync(privatePath, "utf8"),
      publicKey: fs.readFileSync(publicPath),
    };
  } else {
    const pair = crypto.generateKeyPairSync("ec", { namedCurve: "secp256k1" });
    signingKeys = {
      privateKey: pair.privateKey.export({ type: "sec1", format: "pem" }).toString(),
      publicKey: Buffer.from(pair.publicKey.export({ type: "spki", format: "pem" }).toString()),
    };
    console.warn("No OCMF key pair in ./cert, signing with an ephemeral one");
  }
  return signingKeys;
};

const generateOCMFSignature = (data: string) => {
  const sign = crypto.createSign("sha256");
  sign.update(data);
  const signature = sign
    .sign({
      key: loadSigningKeys().privateKey,
    })
    .toString("hex");
  return { SA: "ECDSA-secp256k1-SHA256", SD: signature };
};

export const generateOCMF = (input: OCMFInput) => {
  const data = generateOCMFData(input);
  const signature = generateOCMFSignature(JSON.stringify(data));
  return `OCMF|${JSON.stringify(data)}|${JSON.stringify(signature)}`;
};

export const getOCMFPublicKey = () => {
  return loadSigningKeys().publicKey;
};
