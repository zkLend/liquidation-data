import type {
  Block,
  Filter,
} from "https://esm.sh/@apibara/indexer@0.4.1/starknet";
import type { Config } from "https://esm.sh/@apibara/indexer@0.4.1";

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
  block_timestamp: string;
  timestamp: `0x${string}`;
  event_index: number;
  source: `0x${string}`;
  publisher: `0x${string}`;
  pair_id: `0x${string}`;
  price: `0x${string}`;
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

const allowedPairIds = new Set([
  // ETH
  `0x000000000000000000000000000000000000000000000000004554482f555344`,
  // USDC
  `0x000000000000000000000000000000000000000000000000555344432f555344`,
  // WBTC
  `0x000000000000000000000000000000000000000000000000004254432f555344`,
  // USDT
  `0x000000000000000000000000000000000000000000000000555344542f555344`,
  // WSTETH
  `0x000000000000000000000000000000000000000000005753544554482f555344`,
  // STRK
  `0x0000000000000000000000000000000000000000000000005354524b2f555344`,
  // DAI & DAIV0
  `0x000000000000000000000000000000000000000000000000004441492f555344`,
].map((pairId) => Number(pairId)));

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
        timestamp,
        source,
        publisher,
        pairId,
        price,
        volume,
      ] = event.data;

      if (!allowedPairIds.has(Number(pairId))) {
        continue;
      }

      const submittedSpotEntryEvent: SubmittedSpotEntry = {
        block_timestamp,
        timestamp,
        event_index: eventIndex,
        source,
        publisher,
        pair_id: pairId,
        price,
        volume,
      };
      submittedSpotEntryEvents.push(submittedSpotEntryEvent);
    } else if (
      PRAGMA_V0_END_BLOCK < Number(block_number) &&
      BigInt(fromAddress) === BigInt(PRAGMA_V1_CONTRACT)
    ) {
      const [
        timestamp,
        source,
        publisher,
        // Pragma V1 has a different order of event data
        price,
        pairId,
        volume,
      ] = event.data;

      if (!allowedPairIds.has(Number(pairId))) {
        continue;
      }

      const submittedSpotEntryEvent: SubmittedSpotEntry = {
        block_timestamp,
        timestamp,
        event_index: eventIndex,
        source,
        publisher,
        pair_id: pairId,
        price,
        volume,
      };

      submittedSpotEntryEvents.push(submittedSpotEntryEvent);
      eventIndex += 1;
    }
  }

  console.log(submittedSpotEntryEvents);

  return submittedSpotEntryEvents;
}
