// 01 Jan 2024 HKT 00:00:45
const JAN_01_2024_BLOCK = 489712;

export const config = {
  streamUrl: "https://mainnet.starknet.a5a.ch",
  startingBlock: JAN_01_2024_BLOCK,
  network: "starknet",
  finality: "DATA_STATUS_ACCEPTED",
  filter: {
    header: {},
  },
  sinkType: "console",
  sinkOptions: {},
};

export default function transform(block) {
  return block;
}
