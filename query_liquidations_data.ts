import {
  Client,
  QueryObjectResult,
} from "https://deno.land/x/postgres@v0.19.3/mod.ts";
import { load } from "https://deno.land/std@0.223.0/dotenv/mod.ts";
import { pairIdToToken } from "./constants.ts";

interface SpotPriceEntry {
  timestamp: number;
  source: bigint;
  price: bigint;
}

interface SerializedSubmittedSpotEntry {
  block_timestamp: Date;
  source_timestamp: bigint;
  event_index: bigint;
  source: Uint8Array;
  publisher: Uint8Array;
  token_symbol: typeof pairIdToToken[keyof typeof pairIdToToken];
  price: bigint;
  volume: Uint8Array;
}

interface SerializedLiquidation {
  block_timestamp: number;
  transaction_hash: Uint8Array;
  liquidated_user_address: Uint8Array;
  collateral_token: typeof pairIdToToken[keyof typeof pairIdToToken];
  collateral_token_amount: number;
  debt_token: typeof pairIdToToken[keyof typeof pairIdToToken];
  debt_token_amount: number;
}

const env = await load();

const KEYS = [
  `PG_USERNAME`,
  `PG_PASSWORD`,
  `PG_DATABASE`,
  `PG_PORT`,
  `PG_HOSTNAME`,
] as const;

type A = typeof KEYS[number];

type Environment = {
  [key in A]: string;
};

const ENV = KEYS.reduce((acc, key) => {
  if (!env[key]) {
    throw new Error(`Environment variable ${key} is missing`);
  }
  acc[key] = env[key];
  return acc;
}, {} as Environment);

const client = new Client({
  user: ENV.PG_USERNAME,
  database: ENV.PG_DATABASE,
  hostname: ENV.PG_HOSTNAME,
  port: ENV.PG_PORT,
  password: ENV.PG_PASSWORD,
});
await client.connect();

class PriceWorkSheet {
  PRUNE_THRESHOLD = 9000;
  BACKWARD_TIMESTAMP_BUFFER = 7800;
  MAX_WORKSHEET_SIZE = 16;

  private worksheet: SpotPriceEntry[] = [];
  tokenSymbol: typeof pairIdToToken[keyof typeof pairIdToToken];

  constructor(tokenSymbol: typeof pairIdToToken[keyof typeof pairIdToToken]) {
    this.tokenSymbol = tokenSymbol;
  }

  update(
    entry: SpotPriceEntry,
  ) {
    for (const e of this.worksheet) {
      if (e.source === entry.source) {
        e.price = entry.price;
        e.timestamp = entry.timestamp;
        return;
      }

      if (e.timestamp <= entry.timestamp - this.PRUNE_THRESHOLD) {
        e.source = entry.source;
        e.price = entry.price;
        e.timestamp = entry.timestamp;
        return;
      }
    }

    this.worksheet.push(entry);

    if (this.worksheet.length > this.MAX_WORKSHEET_SIZE) {
      throw new Error("Price worksheet is full");
    }
  }

  getMedianPrice(blockTimestamp: number): bigint | null {
    const entryWithLatestTimestamp = this.worksheet.reduce((a, b) => {
      return a.timestamp > b.timestamp ? a : b;
    });

    const conservativeCurrentTimestamp = entryWithLatestTimestamp
      ? blockTimestamp < entryWithLatestTimestamp.timestamp
        ? blockTimestamp
        : entryWithLatestTimestamp.timestamp
      : null;

    if (!conservativeCurrentTimestamp) {
      return null;
    }

    const prices = this.worksheet
      .filter((e) => {
        return e.timestamp >
          conservativeCurrentTimestamp - this.BACKWARD_TIMESTAMP_BUFFER;
      })
      .map((e) => e.price);

    prices.sort();
    const midIndex = Math.floor(prices.length / 2);

    if (prices.length % 2 === 0) {
      return (prices[midIndex - 1] + prices[midIndex]) / 2n;
    } else {
      return prices[midIndex];
    }
  }
}

class PriceWorksheetManager {
  private worksheets: Record<
    SerializedSubmittedSpotEntry["token_symbol"],
    PriceWorkSheet
  > = {
    "DAI": new PriceWorkSheet("DAI"),
    "USDC": new PriceWorkSheet("USDC"),
    "USDT": new PriceWorkSheet("USDT"),
    "WBTC": new PriceWorkSheet("WBTC"),
    "ETH": new PriceWorkSheet("ETH"),
    "WSTETH": new PriceWorkSheet("WSTETH"),
    "STRK": new PriceWorkSheet("STRK"),
  };

  getWorksheet(
    tokenSymbol: SerializedSubmittedSpotEntry["token_symbol"],
  ): PriceWorkSheet {
    return this.worksheets[tokenSymbol];
  }
}

// async function querySubmittedSpotEntries() {
//   let last: null | {
//     sourceTimestamp: bigint;
//     index: bigint;
//   } = null;

//   let totalRows = 0;

//   while (true) {
//     const query = last === null
//       // Ignore possibly first few liquidations
//       ? `SELECT * FROM submitted_spot_entries ORDER BY source_timestamp, event_index LIMIT 1000`
//       : `SELECT * FROM submitted_spot_entries
//         WHERE (
//           source_timestamp = ${last.sourceTimestamp}
//           AND event_index > ${last.index}
//         )
//         OR
//           source_timestamp > ${last.sourceTimestamp}
//         ORDER BY
//           source_timestamp, event_index LIMIT 1000`;

//     console.log(query);

//     const result: QueryObjectResult<SerializedSubmittedSpotEntry> = await client
//       .queryObject<SerializedSubmittedSpotEntry>(
//         query,
//       );

//     if (result.rows.length === 0) {
//       break;
//     }

//     totalRows += result.rows.length;

//     for (const row of result.rows) {
//       console.log(row.source_timestamp);
//     }

//     const {
//       event_index: lastIndex,
//       source_timestamp: lastTimestamp,
//     } = result.rows[result.rows.length - 1];
//     last = {
//       sourceTimestamp: lastTimestamp,
//       index: lastIndex,
//     };
//     console.log(`Last id: ${lastTimestamp}-${lastIndex}`);
//     console.log(`Total rows: ${totalRows}`);
//   }
// }

async function iterateAllBlockTimestamps() {
  let lastTimestamp: bigint | null = null;
  let count = 0;
  const priceWorksheetManager = new PriceWorksheetManager();

  while (true) {
    const query = lastTimestamp === null
      ? `SELECT block_timestamp FROM liquidations UNION SELECT block_timestamp FROM submitted_spot_entries ORDER BY block_timestamp LIMIT 1000`
      : `SELECT block_timestamp FROM liquidations UNION SELECT block_timestamp FROM submitted_spot_entries WHERE block_timestamp > ${lastTimestamp} ORDER BY block_timestamp LIMIT 1000`;

    const result: QueryObjectResult<{ block_timestamp: bigint }> = await client
      .queryObject<{ block_timestamp: bigint }>(
        query,
      );

    if (result.rows.length === 0) {
      break;
    }

    for (const uniqueTimestampRow of result.rows) {
      const [submittedSpotEntries, liquidations] = await Promise.all([
        querySubmittedSpotEntries(
          uniqueTimestampRow.block_timestamp,
        ),
        queryLiquidations(uniqueTimestampRow.block_timestamp),
      ]);

      for (const liquidation of liquidations) {
        console.log(liquidation);
      }

      for (const entry of submittedSpotEntries) {
        const worksheet = priceWorksheetManager.getWorksheet(
          entry.token_symbol,
        );

        worksheet.update({
          timestamp: Number(entry.source_timestamp),
          source: uint8ArrayToBigInt(entry.source),
          price: entry.price,
        });

        console.log(
          worksheet.tokenSymbol,
          worksheet.getMedianPrice(Number(uniqueTimestampRow.block_timestamp)),
        );
      }
    }

    count += result.rows.length;
    console.log(count);
    lastTimestamp = result.rows[result.rows.length - 1].block_timestamp;
  }
}

async function querySubmittedSpotEntries(
  blockTimestamp: bigint,
): Promise<SerializedSubmittedSpotEntry[]> {
  const query =
    `SELECT * FROM submitted_spot_entries WHERE block_timestamp = ${blockTimestamp}`;
  const result: QueryObjectResult<SerializedSubmittedSpotEntry> = await client
    .queryObject<SerializedSubmittedSpotEntry>(
      query,
    );

  return result.rows;
}

async function queryLiquidations(
  blockTimestamp: bigint,
): Promise<SerializedLiquidation[]> {
  const query =
    `SELECT * FROM liquidations WHERE block_timestamp = ${blockTimestamp}`;
  const result: QueryObjectResult<SerializedLiquidation> = await client
    .queryObject<SerializedLiquidation>(
      query,
    );

  return (result.rows);
}

iterateAllBlockTimestamps();

function uint8ArrayToBigInt(uint8Array: Uint8Array) {
  return uint8Array.reduce((acc, value) => (acc << 8n) + BigInt(value), 0n);
}
