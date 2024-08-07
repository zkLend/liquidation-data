import {
  Client,
  QueryObjectResult,
} from "https://deno.land/x/postgres@v0.19.3/mod.ts";
import { load } from "https://deno.land/std@0.223.0/dotenv/mod.ts";
import { pairIdToToken, tokenSymbolToDecimals } from "./constants.ts";

const PRAGMA_V0_END_BLOCK_TIMESTAMP = 1706608996;

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
  transaction_hash: string;
  liquidated_user_address: string;
  collateral_token: typeof pairIdToToken[keyof typeof pairIdToToken];
  collateral_token_amount: number;
  debt_token: typeof pairIdToToken[keyof typeof pairIdToToken];
  debt_token_amount: number;
}

interface LiquidationCsvRow {
  block_timestamp: string;
  transaction_hash: string;
  liquidated_user_address: string;
  collateral_token: typeof pairIdToToken[keyof typeof pairIdToToken];
  collateral_token_amount: string;
  collateral_token_value: string;
  debt_token: typeof pairIdToToken[keyof typeof pairIdToToken];
  debt_token_amount: string;
  debt_token_value: string;
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
          Number(conservativeCurrentTimestamp) - Number(this.BACKWARD_TIMESTAMP_BUFFER);
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

async function iterateAllRecords() {
  let lastTimestamp: bigint | null = null;
  let count = 0;
  const priceWorksheetManager = new PriceWorksheetManager();
  const liquidationsCsv = await Deno.open("./liquidations.csv", {
    write: true,
    create: true,
    truncate: true,
  });
  const header = [
    `block_timestamp`,
    `transaction_hash`,
    `liquidated_user_address`,
    `collateral_token`,
    `collateral_token_amount`,
    `collateral_token_value`,
    `debt_token`,
    `debt_token_amount`,
    `debt_token_value`,
  ];
  await Deno.write(
    liquidationsCsv.rid,
    new TextEncoder().encode(header.join(",") + "\n"),
  );

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

    let liquidationCsvRows: LiquidationCsvRow[] = [];

    for (const uniqueTimestampRow of result.rows) {
      const [submittedSpotEntries, liquidations] = await Promise.all([
        querySubmittedSpotEntries(
          uniqueTimestampRow.block_timestamp,
        ),
        queryLiquidations(uniqueTimestampRow.block_timestamp),
      ]);

      for (const liquidation of liquidations) {
        const collateralTokenPrice = priceWorksheetManager.getWorksheet(
          liquidation.collateral_token,
        ).getMedianPrice(liquidation.block_timestamp);
        const debtTokenPrice = priceWorksheetManager.getWorksheet(
          liquidation.debt_token,
        ).getMedianPrice(liquidation.block_timestamp);

        if (!collateralTokenPrice || !debtTokenPrice) {
          // Ignore first few liquidations where we don't have price data
          console.log("Missing price for liquidation");
          continue;
        }

        const values = calculateLiquidationValues(
          liquidation,
          Number(collateralTokenPrice),
          Number(debtTokenPrice),
        );

        const liquidationCsvRow: LiquidationCsvRow = {
          block_timestamp: String(liquidation.block_timestamp),
          transaction_hash: liquidation.transaction_hash,
          liquidated_user_address: liquidation.liquidated_user_address,
          collateral_token: liquidation.collateral_token,
          collateral_token_amount: String(liquidation.collateral_token_amount),
          collateral_token_value: String(values.collateralTokenValue),
          debt_token: liquidation.debt_token,
          debt_token_amount: String(liquidation.debt_token_amount),
          debt_token_value: String(values.debtTokenValue),
        };

        liquidationCsvRows.push(liquidationCsvRow);
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
      }

      if (liquidationCsvRows.length > 0) {
        const stringifiedLiquidationCsvRows = liquidationCsvRows.map((row) => {
          return [
            row.block_timestamp,
            row.transaction_hash,
            row.liquidated_user_address,
            row.collateral_token,
            row.collateral_token_amount,
            row.collateral_token_value,
            row.debt_token,
            row.debt_token_amount,
            row.debt_token_value,
          ].join(",");
        }).join("\n");

        await Deno.write(
          liquidationsCsv.rid,
          new TextEncoder().encode(stringifiedLiquidationCsvRows + "\n"),
        );
      }

      liquidationCsvRows = [];
    }

    count += result.rows.length;
    console.log(count);
    lastTimestamp = result.rows[result.rows.length - 1].block_timestamp;
  }

  liquidationsCsv.close();
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

function uint8ArrayToBigInt(uint8Array: Uint8Array) {
  return uint8Array.reduce((acc, value) => (acc << 8n) + BigInt(value), 0n);
}

function getPriceDecimals(
  tokenSymbol: keyof typeof tokenSymbolToDecimals,
  blockTimestamp: number,
) {
  if (
    blockTimestamp > PRAGMA_V0_END_BLOCK_TIMESTAMP &&
    "priceV1Decimals" in tokenSymbolToDecimals[tokenSymbol]
  ) {
    // @ts-ignore: stupid TS typing
    return tokenSymbolToDecimals[tokenSymbol]["priceV1Decimals"] as number;
  } else {
    return tokenSymbolToDecimals[tokenSymbol].priceV0Decimals;
  }
}

function getTokenDecimals(
  tokenSymbol: keyof typeof tokenSymbolToDecimals,
) {
  return tokenSymbolToDecimals[tokenSymbol].tokenDecimals;
}

function calculateLiquidationValues(
  liquidation: SerializedLiquidation,
  collateralTokenPrice: number,
  debtTokenPrice: number,
) {
  const collateralTokenPriceDecimals = getPriceDecimals(
    liquidation.collateral_token,
    liquidation.block_timestamp,
  );
  const debtTokenPriceDecimals = getPriceDecimals(
    liquidation.debt_token,
    liquidation.block_timestamp,
  );
  const collateralTokenDecimals = getTokenDecimals(
    liquidation.collateral_token,
  );
  const debtTokenDecimals = getTokenDecimals(
    liquidation.debt_token,
  );

  const collateralTokenAmount = liquidation.collateral_token_amount /
    10 ** collateralTokenDecimals;
  const debtTokenAmount = liquidation.debt_token_amount /
    10 ** debtTokenDecimals;

  const collateralTokenValue = collateralTokenAmount * collateralTokenPrice /
    10 ** collateralTokenPriceDecimals;
  const debtTokenValue = debtTokenAmount * debtTokenPrice /
    10 ** debtTokenPriceDecimals;

  return {
    collateralTokenValue,
    debtTokenValue,
  };
}

iterateAllRecords();
