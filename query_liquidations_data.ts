import {
  Client,
  QueryObjectResult,
} from "https://deno.land/x/postgres@v0.19.3/mod.ts";
import { load } from "https://deno.land/std@0.223.0/dotenv/mod.ts";
import { SubmittedSpotEntry } from "./pragma_indexer.ts";
import { Liquidation } from "./liquidations_indexer.ts";

interface SpotPriceEntry {
  timestamp: number; // unix epoch in seconds
  source: bigint;
  price: bigint;
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

  getMedianPrice(blockTimestamp: number): bigint {
    const entryWithLatestTimestamp = this.worksheet.reduce((a, b) => {
      return a.timestamp > b.timestamp ? a : b;
    });

    const conservativeCurrentTimestamp = entryWithLatestTimestamp
      ? blockTimestamp < entryWithLatestTimestamp.timestamp
        ? blockTimestamp
        : entryWithLatestTimestamp.timestamp
      : null;

    if (!conservativeCurrentTimestamp) {
      throw new Error("No entry with latest timestamp");
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

async function querySubmittedSpotEntries() {
  let last: null | {
    timestamp: `0x${string}`;
    index: number;
  } = null;

  while (true) {
    const query = last === null
      ? `SELECT * FROM submitted_spot_entries ORDER BY timestamp, event_index LIMIT 1000`
      : `SELECT * FROM submitted_spot_entries WHERE timestamp > ${last.timestamp} AND id > ${last.index} ORDER BY timestamp, event_index LIMIT 1000`;

    const result: QueryObjectResult<SubmittedSpotEntry> = await client
      .queryObject<SubmittedSpotEntry>(
        query,
      );

    if (result.rows.length === 0) {
      break;
    }

    for (const row of result.rows) {
      console.log(row);
    }

    const {
      event_index: lastIndex,
      timestamp: lastTimestamp,
    } = result.rows[result.rows.length - 1];
    last = {
      timestamp: lastTimestamp,
      index: lastIndex,
    };
    console.log(`Last id: ${lastTimestamp}-${lastIndex}`);
  }
}

await querySubmittedSpotEntries();
