import { Client } from "https://deno.land/x/postgres@v0.19.3/mod.ts";
import { load } from "https://deno.land/std@0.223.0/dotenv/mod.ts";

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

await client.queryArray(`
  CREATE TABLE IF NOT EXISTS liquidations (
    block_timestamp bigint NOT NULL,
    transaction_hash bytea NOT NULL,

    liquidated_user_address bytea NOT NULL,
    collateral_token varchar(10) NOT NULL, 
    collateral_token_amount decimal NOT NULL, 
    -- collateral_token_value
    debt_token varchar(10) NOT NULL,
    debt_token_amount decimal NOT NULL,
    -- debt_token_value
    _cursor bigint -- REQUIRED: Apibara requires the target table to have a _cursor bigint column. This column is used to track at which block a row is inserted to handle chain reorganizations.
  );
`);
await client.queryArray(`
  CREATE INDEX IF NOT EXISTS liquidations_block_timestamp_index ON liquidations (block_timestamp);
`);

await client.end();

console.log("Table liquidations created successfully");
