import type {
  Block,
  Filter,
} from "https://esm.sh/@apibara/indexer@0.4.1/starknet";
import type { Config } from "https://esm.sh/@apibara/indexer@0.4.1";
import { pairIdToToken } from "./constants.ts";

// 01 Jan 2024 HKT 00:00:45
const JAN_01_2024_BLOCK = 489712;
const PRAGMA_V0_END_BLOCK = 524957;
const PRAGMA_V0_CONTRACT =
  `0x0346c57f094d641ad94e43468628d8e9c574dcb2803ec372576ccc60a40be2c4`;
const PRAGMA_V1_CONTRACT =
  `0x2a85bd616f912537c50a49a4076db02c00b29b2cdc8a197ce92ed1837fa875b`;
const SUBMITTED_SPOT_ENTRY_SELECTOR =
  `0x0280bb2099800026f90c334a3a23888ffe718a2920ffbbf4f44c6d3d5efb613c`;
const SUBMITTED_SPOT_ENTRY_EVENT_DATA_LENGTH = 6;

export interface SubmittedSpotEntry {
  block_timestamp: number;
  source_timestamp: number;
  event_index: number;
  source: `0x${string}`;
  publisher: `0x${string}`;
  token_symbol: typeof pairIdToToken[keyof typeof pairIdToToken];
  price: string;
  volume: `0x${string}`;
}

const filter: Filter = {
  header: {},
  events: [
    {
      fromAddress: PRAGMA_V0_CONTRACT,
      keys: [SUBMITTED_SPOT_ENTRY_SELECTOR],
      includeReceipt: true,
      includeTransaction: false,
    },
    {
      fromAddress: PRAGMA_V1_CONTRACT,
      keys: [SUBMITTED_SPOT_ENTRY_SELECTOR],
      includeReceipt: true,
      includeTransaction: false,
    },
  ],
};

export const config: Config = {
  streamUrl: "https://mainnet.starknet.a5a.ch",
  startingBlock: JAN_01_2024_BLOCK,
  network: "starknet",
  finality: "DATA_STATUS_ACCEPTED",
  filter,
  sinkType: "postgres",
  sinkOptions: {
    tableName: "submitted_spot_entries",
  },
};

export default function transform({ header, events }: Block) {
  if (!events || events.length === 0 || !header) {
    return [];
  }

  const submittedSpotEntryEvents: SubmittedSpotEntry[] = [];
  let eventIndex = 0;

  for (
    const {
      event,
      receipt,
    } of events
  ) {
    const block_number = header.blockNumber;
    const block_timestamp = header.timestamp;
    const transaction_hash = receipt.transactionHash;
    const fromAddress = event.fromAddress;
    if (
      !event.data ||
      event.data.length !== SUBMITTED_SPOT_ENTRY_EVENT_DATA_LENGTH ||
      !block_number || !block_timestamp || !transaction_hash || !fromAddress
    ) {
      continue;
    }

    if (
      PRAGMA_V0_END_BLOCK >= Number(block_number) &&
      BigInt(fromAddress) === BigInt(PRAGMA_V0_CONTRACT)
    ) {
      const [
        source_timestamp,
        source,
        publisher,
        pairId,
        price,
        volume,
      ] = event.data;

      if (!(Number(pairId) in pairIdToToken)) {
        continue;
      }

      const submittedSpotEntryEvent: SubmittedSpotEntry = {
        block_timestamp: new Date(block_timestamp).getTime() / 1000,
        source_timestamp: Number(source_timestamp),
        event_index: eventIndex,
        source,
        publisher,
        token_symbol: pairIdToToken[Number(pairId)],
        price: Math.floor(Number(price)).toFixed(0),
        volume,
      };
      submittedSpotEntryEvents.push(submittedSpotEntryEvent);
    } else if (
      PRAGMA_V0_END_BLOCK < Number(block_number) &&
      BigInt(fromAddress) === BigInt(PRAGMA_V1_CONTRACT)
    ) {
      const [
        source_timestamp,
        source,
        publisher,
        // Pragma V1 has a different order of event data
        price,
        pairId,
        volume,
      ] = event.data;

      if (!(Number(pairId) in pairIdToToken)) {
        continue;
      }

      const submittedSpotEntryEvent: SubmittedSpotEntry = {
        block_timestamp: new Date(block_timestamp).getTime() / 1000,
        source_timestamp: Number(source_timestamp),
        event_index: eventIndex,
        source,
        publisher,
        token_symbol: pairIdToToken[Number(pairId)],
        price: Math.floor(Number(price)).toFixed(0),
        volume,
      };

      submittedSpotEntryEvents.push(submittedSpotEntryEvent);
    }
    eventIndex += 1;
  }

  console.log(submittedSpotEntryEvents);

  return submittedSpotEntryEvents;
}
