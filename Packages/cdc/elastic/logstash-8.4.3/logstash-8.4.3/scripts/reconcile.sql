-- Source-side control totals. FROM and WHERE must stay identical to the pipeline statements.
set heading off feedback off pagesize 0 linesize 200 trimspool on verify off
select 'ac_balance_data|' || count(*) || '|' || to_char(sum(b.balance), 'TM9', 'NLS_NUMERIC_CHARACTERS=.,')
  from EMOB.mb_account_daily_balance b, emob.mb_account_mst a, emob.mb_customer_mst c, emob.mb_product_mst p, EMOB.ST_AGENT_POINT pt
 where a.ac_id = b.ac_id and a.cust_no = c.cust_no and a.ac_type_id = p.ac_type_id and a.point_id = pt.point_id(+);
select 'customer_data|' || count(*) || '|' || to_char(sum(a.initial_amt), 'TM9', 'NLS_NUMERIC_CHARACTERS=.,')
  from EMOB.MB_ACCOUNT_MST A, EMOB.MB_CUSTOMER_MST C, EMOB.ST_AGENT_POINT F, EMOB.MB_PRODUCT_MST P
 where A.CUST_NO = C.CUST_NO and C.AGENT_POINT_ID = F.POINT_ID(+) and A.POINT_ID = F.POINT_ID(+) and A.AC_TYPE_ID = P.AC_TYPE_ID;
select 'transaction_history_|' || count(*) || '|' || to_char(sum(a.tran_amt_lc), 'TM9', 'NLS_NUMERIC_CHARACTERS=.,')
  from EMOB.MB_TRANSACTION_DTL a, EMOB.MB_ACCOUNT_MST B, EMOB.ST_TRANSACTION_CODE C, EMOB.MB_PRODUCT_MST P
 where A.AC_ID = B.AC_ID and A.TRAN_CODE = C.TRAN_CODE and B.AC_TYPE_ID = P.AC_TYPE_ID and A.AC_ID is not null;
exit
