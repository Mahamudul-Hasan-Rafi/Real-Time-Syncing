SELECT B.AC_ID,
       B.BALANCE,
       B.VALUE_DATE AS BALANCE_DATE,
       A.AC_NO,
       A.AC_TITLE,
       A.CUST_NO,
       A.OPEN_DATE,
       A.CREATE_BY,
       A.CREATE_DATE,
       C.CUST_CODE,
       DECODE (C.CUST_TYPE,  'CUS', 'Customer',  'AGN', 'Agent')
           CUST_TYPE,
       c.cust_dob,
       TRUNC (MONTHS_BETWEEN (SYSDATE, c.cust_dob) / 12)
           AGE,
       DECODE (C.GENDER, 'M', 'Male', 'Female')
           GENDER,
       DECODE (c.maritual_status,  'SNG', 'Single',  'MAR', 'Married')
           Mar_status,
       (SELECT CODDES
          FROM EMOB.STCODMAS
         WHERE HARCOD = 'OCC' AND SOFCOD = C.OCCUPATION_CODE)
           OCCUPATION,
       (SELECT CODDES
          FROM EMOB.STCODMAS
         WHERE HARCOD = 'EDT' AND SOFCOD = C.LAST_EDUCATION)
           EDUCATION,
       P.FULL_DESC
           PRODUCT,
       DECODE (P.DEP_NATURE,
               'R', 'Savings/Current',
               'F', 'Fixed Deposit',
               'C', 'Recuring Deposit')
           DEP_NATURE,
       pt.point_name,
       (SELECT CATEGORY_NAME
          FROM EMOB.ST_POINT_CATEGORY CP
         WHERE CP.point_category_id = pt.POINT_CATEGORY_ID)
           POINT_CATEGORY,
       DECODE (pt.LOCATION_TYPE,  'U', 'Urban',  'R', 'Rural')
           LOCATION_TYPE,
       (SELECT DISTRICT_NAME
          FROM EMOB.DISTRICT_INFO d
         WHERE d.district_code = pt.district_CODE)
           DISTRICT
  FROM EMOB.mb_account_daily_balance  b,
       emob.mb_account_mst                 a,
       emob.mb_customer_mst                c,
       emob.mb_product_mst                 p,
       EMOB.ST_AGENT_POINT                 pt
 WHERE     a.ac_id = b.ac_id
       AND A.CUST_NO = C.CUST_NO
       AND a.ac_type_id = p.ac_type_id
       AND a.point_id = pt.point_id(+)
