# Real dataset (optional)

By default the `producer` service **generates synthetic transactions**
(schema-matched to the proposal, ~4.9% fraud rate) so the whole stack runs
end-to-end with zero setup - no 4.67GB download required.

To use the real dataset instead:

1. Download **CreditTransact** from Mendeley Data.
2. Place the CSV here as `./data/creditcard_transactions.csv` (matching
   `PRODUCER_DATASET_PATH` in `.env`), making sure its header includes at
   least: `transaction_id, timestamp, amount, merchant_id,
   merchant_category, card_type, country, city, device_id, ip_address,
   fraud_label`. Rename/remap columns first if the real file uses different
   names - the producer expects exactly these.
3. `docker compose restart producer`

The producer will then load real rows into HDFS `/raw` and stream real rows
to Kafka instead of synthetic ones.
