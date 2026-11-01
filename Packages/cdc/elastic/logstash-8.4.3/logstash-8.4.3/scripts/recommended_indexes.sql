-- Dev has no indexes on any table these queries use, so every scalar subquery is a full scan per row.
-- Measured without them: customer_data about 7.7 rows/s, transaction_history about 2.2 rows/s.
-- NOT applied. Review before running: this changes the shared dev schema and uses EMOB tablespace.
-- To undo: DROP INDEX for each name below.
CREATE INDEX ix_etl_customer_cust_no   ON emob.mb_customer_mst (cust_no);
CREATE INDEX ix_etl_point_point_id     ON emob.st_agent_point (point_id);
CREATE INDEX ix_etl_document_cust_no   ON emob.mb_document_mst (cust_no);
CREATE INDEX ix_etl_address_ref_no     ON emob.mb_address_mst (ref_no);
CREATE INDEX ix_etl_operator_ac_id     ON emob.mb_account_operator (ac_id);
CREATE INDEX ix_etl_nominee_ac_id      ON emob.nominee_mst (ac_id);
