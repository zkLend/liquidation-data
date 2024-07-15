# liquidation-data

Rapid Apibara indexer for liquidation events

## How to run the indexer

```
docker run --name postgres-zklend \
    -e POSTGRES_USER=lemmein \
    -e POSTGRES_PASSWORD=lemmein \
    -e POSTGRES_DB=zklend \
    -p 5432:5432 \
    -d postgres

cp .env.example .env

deno run create_liquidation_table.ts

export APIBARA_KEY=<Get the API key from Apibara dashboard>
export POSTGRES_CONNECTION_STRING=postgresql://lemmein:lemmein@localhost:5432/zklend

apibara run indexer.ts -A "${APIBARA_KEY}"
```
