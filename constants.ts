export const pairIdToToken = {
  [
    Number("0x000000000000000000000000000000000000000000000000004554482f555344")
  ]: `ETH`,
  // USDC
  [
    Number("0x000000000000000000000000000000000000000000000000555344432f555344")
  ]: `USDC`,
  // WBTC
  [
    Number("0x000000000000000000000000000000000000000000000000004254432f555344")
  ]: `WBTC`,
  // USDT
  [
    Number("0x000000000000000000000000000000000000000000000000555344542f555344")
  ]: `USDT`,
  // WSTETH
  [
    Number("0x000000000000000000000000000000000000000000005753544554482f555344")
  ]: `WSTETH`,
  // STRK
  [
    Number("0x0000000000000000000000000000000000000000000000005354524b2f555344")
  ]: `STRK`,
  // DAI & DAIV0
  [
    Number("0x000000000000000000000000000000000000000000000000004441492f555344")
  ]: `DAI`,
} as const;

export const tokenAddressToToken = {
  [
    Number("0x049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7")
  ]: `ETH`,
  [
    Number("0x053c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8")
  ]: `USDC`,
  [
    Number("0x03fe2b97c1fd336e750087d68b9b867997fd64a2661ff3ca5a7c771641e8e7ac")
  ]: `WBTC`,
  [
    Number("0x068f5c6a61780768455de69077e07e89787839bf8166decfbf92b645209c0fb8")
  ]: `USDT`,
  [
    Number("0x042b8f0484674ca266ac5d08e4ac6a3fe65bd3129795def2dca5c34ecc5f96d2")
  ]: `WSTETH`,
  [
    Number("0x04718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d")
  ]: `STRK`,
  // DAIv0
  [
    Number("0x00da114221cb83fa859dbdb4c44beeaa0bb37c7537ad5ae66fe5e0efd20e6eb3")
  ]: `DAI`,
  [
    Number("0x05574eb6b8789a91466f902c380d978e472db68170ff82a5b650b95a58ddf4ad")
  ]: `DAI`,
} as const;

export const tokenSymbolToDecimals = {
  "ETH": {
    tokenDecimals: 18,
    priceV0Decimals: 8,
  },
  "USDC": {
    tokenDecimals: 6,
    priceV0Decimals: 8,
    priceV1Decimals: 6,
  },
  "WBTC": {
    tokenDecimals: 8,
    priceV0Decimals: 8,
  },
  "USDT": {
    tokenDecimals: 6,
    priceV0Decimals: 8,
    priceV1Decimals: 6,
  },
  "WSTETH": {
    tokenDecimals: 18,
    priceV0Decimals: 8,
  },
  "STRK": {
    tokenDecimals: 18,
    priceV0Decimals: 8,
  },
  "DAI": {
    tokenDecimals: 18,
    priceV0Decimals: 8,
  },
} as const;
