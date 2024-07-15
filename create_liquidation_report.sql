-- Run this inside Docker

\copy (
  SELECT 
    block_number,
    block_timestamp,
    convert_from(transaction_hash, 'UTF-8') as transaction_hash,
    convert_from(liquidated_user_address, 'UTF-8') as liquidated_user_address,
    convert_from(collateral_token_address, 'UTF-8') as collateral_token_address,
    convert_from(collateral_token_amount, 'UTF-8') as collateral_token_amount,
    convert_from(debt_token_address, 'UTF-8') as debt_token_address,
    convert_from(debt_token_amount, 'UTF-8') as debt_token_amount,
    _cursor
  FROM 
    liquidations
  ORDER BY 
    block_number DESC, _cursor DESC
) TO '/tmp/liquidations.csv' WITH CSV HEADER;
