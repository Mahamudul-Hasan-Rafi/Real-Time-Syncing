SELECT a.TRAN_ID,
       a.branch_code,
       B.AC_TITLE,
       B.OPEN_DATE,
       B.CLOSE_DATE,
       B.ALLTRN,
       B.AC_STATUS,
       B.INI_CHECK,
       B.INI_CHECK_BY,
       B.INI_CHECK_DATE,
       B.FINAL_CHECK,
       B.OPR_INS,
       B.EKYC_TYPE,
       B.VERIFY_BY,
       B.VERIFY_STATUS,
       B.VERIFY_DATE,
       B.FINAL_CHECK_BY,
       B.FINAL_CHECK_DATE,
       a.TRANS_BRANCH_CODE,
       a.SL_NUM,
       a.tran_type,
       TO_CHAR (a.CREATE_DATE, 'HH24:MI:SS')
           TRAN_TIME,
       C.TRAN_DESC,
       A.AC_ID,
       A.BAL_FLAG,
       A.GLAC_ID,
       B.AC_NO,
       B.AC_TYPE_ID,
       B.CUST_NO,
       P.FULL_DESC
           PRODUCT_NAME,
       P.AC_TYPE,
       P.DEP_NATURE,
       P.SHORT_DESC
           SHORT_NAME,
       P.AC_PREFIX,
       A.TRAN_AMT_LC
           AMOUNT,
       DECODE (a.tran_type,
               'CL', 'Clearing',
               'CH', 'Charge',
               'CS', 'Cash',
               'TR', 'Transfer',
               'IN', 'Interest',
               'RV', 'Reverse',
               'JP', 'Journal Posting',
               'JZ', 'Journal Other',
               'Other')
           Type_Desc,
       A.DOC_NUM,
       A.TRAN_POINT_ID,
       A.DOC_DATE,
       a.tran_code,
       A.NARATION,
       a.drcr_code,
       DECODE (A.DRCR_CODE,  'D', 'Debit',  'C', 'Credit')
           DR_CR,
       A.REV_TRAN_ID,
       A.CREATE_DATE,
       A.CREATE_BY,
       A.BAL_FLAG,
       A.TRANS_BRANCH_CODE,
       A.REF_DOC_NO,
       A.CHARGE_FLAG,
       A.VALUE_DATE,
       A.CHEQUE_SERISE,
       A.CHEQUE_NO,
       A.CHEQUE_DATE,
	   B.POINT_ID AC_POINT_ID,
       (SELECT POINT_NAME 
          FROM EMOB.ST_AGENT_POINT
         WHERE POINT_ID = B.POINT_ID)
           AC_POINT_NAME,
       (SELECT Y.POINT_CATEGORY_ID
          FROM EMOB.ST_AGENT_POINT X, EMOB.st_point_category Y
         WHERE     X.POINT_ID = B.POINT_ID
               AND x.point_category_id = y.point_category_id)
           AC_POINT_CATEGORY_ID,     
       (SELECT y.category_name
          FROM EMOB.ST_AGENT_POINT X, EMOB.st_point_category Y
         WHERE     X.POINT_ID = B.POINT_ID
               AND x.point_category_id = y.point_category_id)
           AC_POINT_CATEGORY,
       (SELECT DISTRICT_CODE||'-'||DISTRICT_NAME
          FROM EMOB.DISTRICT_INFO
         WHERE DISTRICT_CODE = (SELECT DISTRICT_CODE
                                  FROM EMOB.ST_AGENT_POINT
                                 WHERE POINT_ID = B.POINT_ID))
           AC_POINT_DISTRICT,
       (SELECT SECTOR_CODE || '-' || SECTOR_DESC
          FROM EMOB.ST_SECTOR_CODE
         WHERE SRLNO = B.SECTOR_CODE)
           SECTOR_CODE,
       (SELECT CODDES
          FROM EMOB.STCODMAS
         WHERE     HARCOD = 'OCC'
               AND SOFCOD = (SELECT OCCUPATION_CODE
                               FROM EMOB.MB_CUSTOMER_MST
                              WHERE CUST_NO = B.CUST_NO))
           OCCUPATION,
       (SELECT DECODE (LOCATION_TYPE,  'U', 'Urban',  'R', 'Rural')
          FROM EMOB.ST_AGENT_POINT
         WHERE POINT_ID = A.TRAN_POINT_ID)
           LOC_TYPE,
        (SELECT AREA_NAME
          FROM EMOB.ST_AGENT_POINT
         WHERE POINT_ID = A.TRAN_POINT_ID)
           AREA_NAME,
            (SELECT LATITUDE
          FROM EMOB.ST_AGENT_POINT
         WHERE POINT_ID = A.TRAN_POINT_ID)
           LAT,
            (SELECT LONGITUDE
          FROM EMOB.ST_AGENT_POINT
         WHERE POINT_ID = A.TRAN_POINT_ID)
           LNG,
          (SELECT POST_CODE
          FROM EMOB.ST_AGENT_POINT
         WHERE POINT_ID = A.TRAN_POINT_ID)
           POST_CODE,
          (SELECT STATUS
          FROM EMOB.ST_AGENT_POINT
         WHERE POINT_ID = A.TRAN_POINT_ID)
           STATUS,
          (SELECT LOCATION_TYPE
          FROM EMOB.ST_AGENT_POINT
         WHERE POINT_ID = A.TRAN_POINT_ID)
           LOCATION_TYPE,
          (SELECT AGENT_ID
          FROM EMOB.ST_AGENT_POINT
         WHERE POINT_ID = A.TRAN_POINT_ID)
           AGENT_ID,
           (SELECT AGENT_MAIL_ID
          FROM EMOB.ST_AGENT_POINT
         WHERE POINT_ID = A.TRAN_POINT_ID)
           AGENT_MAIL_ID,
       (SELECT y.category_name
          FROM EMOB.ST_AGENT_POINT X, EMOB.st_point_category Y
         WHERE     X.POINT_ID = A.TRAN_POINT_ID
               AND x.point_category_id = y.point_category_id)
           POINT_CATEGORY,
       (SELECT POINT_NAME
          FROM EMOB.ST_AGENT_POINT
         WHERE POINT_ID = A.TRAN_POINT_ID)
           POINT_NAME,
       (SELECT DISTRICT_CODE||'-'||DISTRICT_NAME
          FROM EMOB.DISTRICT_INFO
         WHERE DISTRICT_CODE = (SELECT DISTRICT_CODE
                                  FROM EMOB.ST_AGENT_POINT
                                 WHERE POINT_ID = A.TRAN_POINT_ID))
           POINT_DISTRICT,
        (SELECT UPAZILA_CODE||'-'||UPAZILA_NAME
          FROM EMOB.UPAZILA_INFO
         WHERE DISTRICT_CODE = (SELECT DISTRICT_CODE
                                  FROM EMOB.ST_AGENT_POINT
                                 WHERE POINT_ID = A.TRAN_POINT_ID)
           AND UPAZILA_CODE=(SELECT UPAZILA_CODE
                                  FROM EMOB.ST_AGENT_POINT
                                 WHERE POINT_ID = A.TRAN_POINT_ID))
           POINT_UPAZILLA,
          (SELECT UNION_CODE||'-'||UNION_NAME
          FROM EMOB.UNION_INFO
         WHERE DISTRICT_CODE = (SELECT DISTRICT_CODE
                                  FROM EMOB.ST_AGENT_POINT
                                 WHERE POINT_ID = A.TRAN_POINT_ID)
           AND UPAZILA_CODE=(SELECT UPAZILA_CODE
                                  FROM EMOB.ST_AGENT_POINT
                                 WHERE POINT_ID = A.TRAN_POINT_ID)
           AND UNION_CODE=(SELECT UNION_CODE
                                  FROM EMOB.ST_AGENT_POINT
                                 WHERE POINT_ID = A.TRAN_POINT_ID))
           POINT_UNION,
       (SELECT FIRST_NAME || ' ' || LAST_NAME
          FROM EMOB.MB_CUSTOMER_MST X, EMOB.ST_AGENT_POINT Y
         WHERE Y.POINT_ID = A.TRAN_POINT_ID AND x.cust_no = y.agent_id)
           AGENT_NAME,
        (SELECT CUST_STATUS
          FROM EMOB.MB_CUSTOMER_MST X
         WHERE x.cust_no = B.CUST_NO) CUST_STATUS,
        (SELECT CUST_TYPE
          FROM EMOB.MB_CUSTOMER_MST X
         WHERE x.cust_no = B.CUST_NO) CUST_TYPE,
        (SELECT REF_AGN
          FROM EMOB.MB_CUSTOMER_MST X
         WHERE x.cust_no = B.CUST_NO) REF_AGN,
        (SELECT MOBILE_NO
          FROM EMOB.MB_CUSTOMER_MST X
         WHERE x.cust_no = B.CUST_NO) MOBILE_NO,
         (SELECT MOBILE_NO2
          FROM EMOB.MB_CUSTOMER_MST X
         WHERE x.cust_no = B.CUST_NO) MOBILE_NO2,
       (SELECT TOKEN_NO
          FROM EMOB.MB_CUSTOMER_MST X
         WHERE x.cust_no = B.CUST_NO) AML_TOKEN_NO,
         (SELECT AML_SENDING_DATE
          FROM EMOB.MB_CUSTOMER_MST X
         WHERE x.cust_no = B.CUST_NO) AML_SENDING_DATE,
         (SELECT AML_STATUS_UPDATE_DATE
          FROM EMOB.MB_CUSTOMER_MST X
         WHERE x.cust_no = B.CUST_NO) AML_STATUS_UPDATE_DATE,
        (SELECT AML_STS
          FROM EMOB.MB_CUSTOMER_MST X
         WHERE x.cust_no = B.CUST_NO) AML_STS,
       'ABS'
           SRC,
       'C'
           CON_ISL
  FROM EMOB.MB_TRANSACTION_DTL   a,
       EMOB.MB_ACCOUNT_MST       B,
       EMOB.ST_TRANSACTION_CODE  C,
       EMOB.MB_PRODUCT_MST       P
 WHERE     A.AC_ID = B.AC_ID
       AND A.TRAN_CODE = C.TRAN_CODE
       AND B.AC_TYPE_ID = P.AC_TYPE_ID
       AND A.AC_ID IS NOT NULL
       AND A.AC_ID = B.AC_ID
