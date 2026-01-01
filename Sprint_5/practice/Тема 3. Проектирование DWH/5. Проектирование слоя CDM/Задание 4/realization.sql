ALTER TABLE cdm.dm_settlement_report ADD CONSTRAINT dm_settlement_report_orders_count_check
check (orders_count
>= 0); 
ALTER TABLE cdm.dm_settlement_report ADD CONSTRAINT dm_settlement_report_orders_total_sum_check
check (orders_total_sum
>= 0);
ALTER TABLE cdm.dm_settlement_report ADD CONSTRAINT dm_settlement_report_orders_bonus_payment_sum_check
check (orders_bonus_payment_sum
>= 0);
ALTER TABLE cdm.dm_settlement_report ADD CONSTRAINT dm_settlement_report_orders_bonus_granted_sum_check
check (orders_bonus_granted_sum
>= 0);
ALTER TABLE cdm.dm_settlement_report ADD CONSTRAINT dm_settlement_report_order_processing_fee_check
check (order_processing_fee
>= 0);
ALTER TABLE cdm.dm_settlement_report ADD CONSTRAINT dm_settlement_report_restaurant_reward_sum_check
check (restaurant_reward_sum
>= 0);