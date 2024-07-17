import type {
  Block,
  Filter,
} from "https://esm.sh/@apibara/indexer@0.4.1/starknet";
import type { Config } from "https://esm.sh/@apibara/indexer@0.4.1";
import { tokenAddressToToken } from "./constants.ts";

// 01 Jan 2024 HKT 00:00:45
const JAN_01_2024_BLOCK = 489712;
const MARKET_CONTRACT =
  `0x04c0a5193d58f74fbace4b74dcf65481e734ed1714121bdc571da345540efa05`;
const LIQUIDATION_SELECTOR =
  `0x0238a25785a13ab3138feb8f8f517e5a21a377cc1ad47809e9fd5e76daf01df7`;
const LIQUIDATION_EVENT_DATA_LENGTH = 7;

export interface Liquidation {
  index: number;
  block_timestamp: number;
  transaction_hash: string;
  liquidated_user_address: string;
  collateral_token:
    typeof tokenAddressToToken[keyof typeof tokenAddressToToken];
  collateral_token_amount: number;
  debt_token: typeof tokenAddressToToken[keyof typeof tokenAddressToToken];
  debt_token_amount: number;
}

const filter: Filter = {
  header: {},
  events: [
    {
      fromAddress: MARKET_CONTRACT,
      keys: [LIQUIDATION_SELECTOR],
      includeReceipt: false,
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
    tableName: "liquidations",
  },
};

export default function transform({ header, events }: Block) {
  if (!events || events.length === 0 || !header) {
    return [];
  }

  const liquidations: Liquidation[] = [];

  for (
    const {
      event,
      transaction,
    } of events
  ) {
    const block_number = header.blockNumber;
    const block_timestamp = header.timestamp;
    const transactionHash = transaction.meta.hash;
    const eventIndex = event.index;
    if (
      !event.data || event.data.length !== LIQUIDATION_EVENT_DATA_LENGTH ||
      !block_number || !block_timestamp || !transactionHash ||
      eventIndex === undefined
    ) {
      continue;
    }

    const [
      _liquidatorAddress,
      liquidatedUserAddress,
      debtTokenAddress,
      _debtRawAmount,
      debtFaceAmount,
      collateralTokenAddress,
      collateralTokenAmount,
    ] = event.data;

    const liquidation: Liquidation = {
      index: eventIndex,
      block_timestamp: new Date(block_timestamp).getTime() / 1000,
      transaction_hash: strip0x(transactionHash),
      liquidated_user_address: strip0x(liquidatedUserAddress),
      collateral_token: tokenAddressToToken[Number(collateralTokenAddress)],
      collateral_token_amount: Number(collateralTokenAmount),
      debt_token: tokenAddressToToken[Number(debtTokenAddress)],
      debt_token_amount: Number(debtFaceAmount),
    };

    liquidations.push(liquidation);
  }

  return liquidations;
}

function strip0x(hex: string) {
  return hex.startsWith("0x") ? hex.slice(2) : hex;
}
