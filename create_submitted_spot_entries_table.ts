// deno. connect with postgres
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
  CREATE TABLE IF NOT EXISTS submitted_spot_entries (
    block_timestamp timestamp NOT NULL,
    timestamp bytea NOT NULL, 
    source bytea NOT NULL,
    publisher bytea NOT NULL,
    pair_id bytea NOT NULL,
    price bytea NOT NULL,
    volume bytea NOT NULL,
    _cursor bigint -- REQUIRED: Apibara requires the target table to have a _cursor bigint column. This column is used to track at which block a row is inserted to handle chain reorganizations.
  );
`);

await client.end();

console.log("Table submitted_spot_entries created successfully");
