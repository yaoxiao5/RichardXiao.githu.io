library(mice)
library(ibmdbR)
library(miceadds)
library(DBI)
library(dbplyr)
library(odbc)
library(readxl)
library(sqldf)
library(rJava)
library(plyr)
library(dplyr)
library(readxl)
library(xlsxjars)
library(MASS)
library(dplyr)
library(shiny)
library(stringr)
library(scales) 
library(sqldf)
require(data.table)
library(Rcpp)
library(readr)
library(splitstackshape)
library(dplyr)
library(tidyr)
library(tidyverse)
library(fs)
library(data.table)
library(RMariaDB)
library(splitstackshape)
library(lubridate)
library(openxlsx)
library(readxl)
library(openxlsx)
library(stringr)
library(writexl)
library(Hmisc)
library(imputeTS)
library(VIM)
library(class)
library(foreach)
library(doParallel)
library(kknn)
library(stats)
library(imputeTS)
library(scales)
library(pivottabler)

detach("package:rlang", unload = TRUE)

#Purpose of this project is to create a ranking system of SKUs for company. These parameters used for ranking is called KVI which includes sales, units, elasticity and on hand amount

#Set up working directory + connections. Replace line 57 with your working directory.
setwd("C:/Users/Richard.Xiao/OneDrive - Advance Auto Parts/Documents/Excel_Files")


myconn <- DBI::dbConnect(odbc::odbc(), "SNOWFLAKE", uid="AAP_PRCNG_ANLYTCS_PROD_USER", pwd='p2JiJ@e1i86GXD6g',role="AAP_PRCNG_ANLYTCS_PROD_REPORTING_ROLE",warehouse='PRCNG_ANLYTCS_WH', schema = 'analysis')


myconn2 <- DBI::dbConnect(odbc::odbc(), "SNOWFLAKE", uid="AAP_PRCNG_ANLYTCS_PROD_USER", pwd='p2JiJ@e1i86GXD6g',role="AAP_FINERP_EPM_PROD_REPORTING_ROLE",warehouse='FINERP_EPM_WH', schema = 'analysis')



##Start of KVI queries

##Connection 1 for transaction/financial data

kvi_transaction <- DBI::dbGetQuery(myconn,"select distinct SKU_CD,SKU_NM,DESCRIPTION,DEPARTMENT_DESCRIPTION,DEPARTMENT_NUM,GROUP_DESCRIPTION,GROUP_NUM,DIVISION_NUM,DIVISION_DESCRIPTION,CLASS_NUM,CLASS_DESCRIPTION,SUBCLASS_NUM,SUBCLASS_DESCRIPTION,BRAND_DESCRIPTION,sum(COGS_AMT) as NEW_COGS_AMT,sum(PROFIT_AMT) as GM,sum(REVENUE) as SALES,sum(QUANTITY) as UNITS,count(distinct(ORDER_ID)) as invoice_count_product,GM/NULLIFZERO(SALES) as GM_Rate, UNITS * NEW_COGS_AMT as COGS
from AAP_PRCNG_ANLYTCS_PROD_DB.PUBLIC.vw_p_pricing_transactions as sls
left join AAP_PRCNG_ANLYTCS_PROD_DB.PUBLIC.vw_dim_date as dte
    on DATE(dte.FULL_DATE) = sls.ORDER_DT
left join AAP_PRCNG_ANLYTCS_PROD_DB.PUBLIC.vw_p_dim_product as prod
    on prod.SKU = sls.SKU_CD
left join AAP_PRCNG_ANLYTCS_PROD_DB.PUBLIC.vw_sku as skuss
    on skuss.SKU_ID = sls.SKU_CD
where  FISCAL_PERIOD_IN_YEAR in (12) and FISCAL_YEAR = '2024' and BRAND_DESCRIPTION not in ('WRLPC', 'CORE') and GROUP_DESCRIPTION not in ('ADVANCE CARE SERVICES','COMMERCIAL JACKS/LIFTS','EXTERNAL SUPPLY','NON-SKU MERCHANDISE','OTHER/CORES','SERVICE','STORE SUPPLIES & FIXTURES','UNDEFINED') and CHANNEL_NM = 'DIY'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14")











#Connection  for lifecycle data

kvi_lifecycle <- DBI::dbGetQuery(myconn2,"select AAP_SKU_NUMBER as SKU_NUMBER, SKU_EPOCH,SKU_EPOCH_SHARED
from AAP_DAT_PROD_DB.AVAN.VW_SKU_EPOCH_LATEST")


#Read in store data 

kvi_sku_report <- DBI::dbGetQuery(myconn2,"select AAP_SKU_NUMBER as SKU_NUMBER, COUNT_STORE_ASSORTED,COUNT_HUB_ASSORTED,COUNT_SUPER_HUB_ASSORTED
from aap_dat_prod_db.avan.vw_sku_reporting_base")

#Read in CPI information

kvi_CPI <- DBI::dbGetQuery(myconn,"WITH CAL AS (
SELECT DISTINCT 
        T.FISCAL_WEEK_START_DATE::DATE AS TW_FISCAL_WEEK_START_DATE,
        T.FISCAL_YEAR AS TW_FISCAL_YEAR,
        T.FISCAL_WEEK_OF_YEAR AS TW_FISCAL_WEEK_OF_YEAR,
        T.FISCAL_YEAR_WEEK AS TW_FISCAL_YEAR_WEEK,
        T.FISCAL_YEAR_PERIOD AS TW_FISCAL_YEAR_PERIOD,
        T.FISCAL_WEEK_END_DATE AS TW_FISCAL_WEEK_END_DATE,
        Y.FISCAL_WEEK_START_DATE::DATE AS LW_FISCAL_WEEK_START_DATE,
        Y.FISCAL_WEEK_OF_YEAR AS LW_FISCAL_WEEK_OF_YEAR,
        Y.FISCAL_YEAR AS LW_FISCAL_YEAR,
        Y.FISCAL_YEAR_WEEK AS LW_FISCAL_YEAR_WEEK,
        Y.FISCAL_WEEK_END_DATE::DATE AS LW_FISCAL_WEEK_END_DATE
    FROM AAP_PRCNG_CATALYST_PROD_DB.PUBLIC.VW_DIM_DATE AS T
        INNER JOIN AAP_PRCNG_CATALYST_PROD_DB.PUBLIC.VW_DIM_DATE AS Y
            ON DATEADD(DAY,-7,T.FISCAL_WEEK_START_DATE) = Y.FISCAL_WEEK_START_DATE
    WHERE 
         TW_FISCAL_YEAR_WEEK IN (202452)
    ),


    
PCAL AS (
    SELECT DISTINCT
        C1.FISCAL_YEAR_PERIOD,
        C.TW_FISCAL_YEAR_PERIOD AS FISCAL_PERIOD,
        DATE(C2.FISCAL_PERIOD_START_DATE) AS ROLLING_BGN_FISCAL_DATE,
        DATE(DATEADD(DAY,-1,C1.FISCAL_PERIOD_START_DATE)) AS ROLLING_END_FISCAL_DATE,
        C.TW_FISCAL_YEAR_WEEK
    FROM (SELECT DISTINCT TW_FISCAL_YEAR_PERIOD, TW_FISCAL_WEEK_START_DATE, TW_FISCAL_YEAR_WEEK FROM CAL) AS C
        INNER JOIN AAP_PRCNG_CATALYST_PROD_DB.PUBLIC.VW_DIM_DATE AS C1
            ON C.TW_FISCAL_YEAR_PERIOD = C1.FISCAL_YEAR_PERIOD
        INNER JOIN AAP_PRCNG_CATALYST_PROD_DB.PUBLIC.VW_DIM_DATE AS C2
            ON C1.FISCAL_YEAR_PERIOD = C2.FISCAL_YEAR_PERIOD + 100
),

MASTER_INTERCHANGE AS(
    SELECT DISTINCT 
        SKU
    FROM AAP_PRCNG_CATALYST_PROD_DB.PUBLIC.VW_COMPETITOR_PRICING_MASTER_INTERCHANGE
),


DIY_UNITS AS (
    SELECT
        A.SKU, 
        SUM(ZEROIFNULL(QUANTITY)) AS AAP_QUANTITY,
        PCAL.FISCAL_YEAR_PERIOD,
        PCAL.TW_FISCAL_YEAR_WEEK
    FROM AAP_PRCNG_CATALYST_PROD_DB.PUBLIC.VW_DIFM_DIY_TRANSACTIONS AS S
    INNER JOIN PCAL 
        ON S.ORDER_DT BETWEEN PCAL.ROLLING_BGN_FISCAL_DATE AND PCAL.ROLLING_END_FISCAL_DATE
    RIGHT JOIN MASTER_INTERCHANGE AS A 
        ON TRY_CAST(SKU_CD AS INTEGER) = A.SKU
    WHERE 
        CHANNEL_NM = 'DIY' 
        AND TRY_TO_NUMBER(SKU_CD) IS NOT NULL
    GROUP BY 
        A.SKU, 
        PCAL.FISCAL_YEAR_PERIOD,
        PCAL.TW_FISCAL_YEAR_WEEK
),

RETAIL_PRICE AS (
    SELECT 
        SKU,
        PRICE AS AAP_ITEM_PRICE,
        TW_FISCAL_YEAR_WEEK,
        TW_FISCAL_WEEK_START_DATE,
        TW_FISCAL_WEEK_END_DATE,
        EFFDATE,
        RN
    FROM(
        SELECT 
            A.SKU,
            PRICE, 
            CAL.TW_FISCAL_YEAR_WEEK,
            CAL.TW_FISCAL_WEEK_END_DATE,
            CAL.TW_FISCAL_WEEK_START_DATE,
            EFFDATE,
            ROW_NUMBER() OVER(PARTITION BY A.SKU, CAL.TW_FISCAL_YEAR_WEEK ORDER BY EFFDATE DESC) AS RN
        FROM AAP_PRCNG_CATALYST_PROD_DB.PUBLIC.VW_RETAIL_PRICE AS R 
            RIGHT JOIN MASTER_INTERCHANGE AS A 
                ON A.SKU = TRY_CAST(R.SKU AS INTEGER)
            INNER JOIN CAL 
                ON DATE(CAL.TW_FISCAL_WEEK_END_DATE) >= EFFDATE
        )
    WHERE RN = 1
    ),

PROMO_PRICE AS(
    SELECT 
        SKU,
        TW_FISCAL_YEAR_WEEK,
        PRICE AS AAP_PROMO_PRICE
    FROM( 
        SELECT
            A.SKU,
            CAL.TW_FISCAL_YEAR_WEEK,
            MEDIAN(PRICE) AS PRICE,
            ROW_NUMBER() OVER(PARTITION BY A.SKU, CAL.TW_FISCAL_YEAR_WEEK ORDER BY START_DATE DESC) AS RN
        FROM AAP_PRCNG_ANLYTCS_PROD_DB.PUBLIC.VW_PROMOTIONAL_PRICE_HISTORY AS Z 
            RIGHT JOIN MASTER_INTERCHANGE AS A 
                ON A.SKU = TRY_CAST(Z.SKU AS INTEGER)
            INNER JOIN CAL 
                ON CAL.TW_FISCAL_WEEK_START_DATE >= START_DATE
        GROUP BY 
            A.SKU,
            TW_FISCAL_YEAR_WEEK,
            START_DATE
        )
    WHERE RN = 1 
),

COMP_DATA_TW AS (
   SELECT
        A.SKU,
        CAL.TW_FISCAL_YEAR_WEEK,
        'AUTOZONE' AS RETAILER,
        MEDIAN(A.REGULAR_PRICE) AS TW_COMP_PRICE,
        MEDIAN(A.SALE_PRICE) AS TW_COMP_SALE_PRICE,
        B.AAP_QUANTITY,
        C.AAP_ITEM_PRICE,
        F.AAP_PROMO_PRICE,
        COALESCE(F.AAP_PROMO_PRICE, C.AAP_ITEM_PRICE) AS AAP_EFF_PRICE,
        COALESCE(TW_COMP_SALE_PRICE, TW_COMP_PRICE) AS COMP_EFF_PRICE,
        IFF(AAP_ITEM_PRICE > 0, COMP_EFF_PRICE*AAP_QUANTITY,0) AS COMP_EFF_SALES_EST,
        IFF(AAP_ITEM_PRICE > 0, TW_COMP_PRICE*AAP_QUANTITY, 0) AS COMP_SALES_EST,
        IFF(TW_COMP_PRICE >0, AAP_ITEM_PRICE*AAP_QUANTITY,0) AS AAP_SALES_EST,
        IFF(TW_COMP_PRICE >0, AAP_EFF_PRICE*AAP_QUANTITY,0) AS AAP_EFF_SALES_EST
    FROM AAP_PRCNG_CATALYST_PROD_DB.PUBLIC.VW_CPI_CLEANED_AUTOZONE AS A
        INNER JOIN CAL 
            ON A.FISCAL_WEEK_OF_YEAR = CAL.TW_FISCAL_WEEK_OF_YEAR 
            AND A.FISCAL_YEAR = CAL.TW_FISCAL_YEAR
        LEFT JOIN DIY_UNITS AS B
            ON CAL.TW_FISCAL_YEAR_WEEK = B.TW_FISCAL_YEAR_WEEK
            AND A.SKU = B.SKU 
        LEFT JOIN RETAIL_PRICE AS C 
            ON CAL.TW_FISCAL_YEAR_WEEK = C.TW_FISCAL_YEAR_WEEK 
            AND A.SKU = C.SKU 
        LEFT JOIN PROMO_PRICE AS F 
            ON CAL.TW_FISCAL_YEAR_WEEK = F.TW_FISCAL_YEAR_WEEK
            AND A.SKU = F.SKU
    GROUP BY 
        A.SKU,
        CAL.TW_FISCAL_YEAR_WEEK,
        A.RETAILER,
        B.AAP_QUANTITY,
        C.AAP_ITEM_PRICE,
        B.AAP_QUANTITY,
        F.AAP_PROMO_PRICE
        

UNION ALL 

   SELECT
        A.SKU,
        CAL.TW_FISCAL_YEAR_WEEK,
        'OREILLY' AS RETAILER,
        MEDIAN(A.REGULAR_PRICE) AS TW_COMP_PRICE,
        MEDIAN(A.SALE_PRICE) AS TW_COMP_SALE_PRICE,
        B.AAP_QUANTITY,
        C.AAP_ITEM_PRICE,
        F.AAP_PROMO_PRICE,
        COALESCE(F.AAP_PROMO_PRICE, C.AAP_ITEM_PRICE) AS AAP_EFF_PRICE,
        COALESCE(TW_COMP_SALE_PRICE, TW_COMP_PRICE) AS COMP_EFF_PRICE,
        IFF(AAP_ITEM_PRICE > 0, COMP_EFF_PRICE*AAP_QUANTITY,0) AS COMP_EFF_SALES_EST,
        IFF(AAP_ITEM_PRICE > 0, TW_COMP_PRICE*AAP_QUANTITY, 0) AS COMP_SALES_EST,
        IFF(TW_COMP_PRICE >0, AAP_ITEM_PRICE*AAP_QUANTITY,0) AS AAP_SALES_EST,
        IFF(TW_COMP_PRICE >0, AAP_EFF_PRICE*AAP_QUANTITY,0) AS AAP_EFF_SALES_EST
    FROM AAP_PRCNG_CATALYST_PROD_DB.PUBLIC.VW_CPI_CLEANED_OREILLY AS A
        INNER JOIN CAL 
            ON A.FISCAL_WEEK_OF_YEAR = CAL.TW_FISCAL_WEEK_OF_YEAR 
            AND A.FISCAL_YEAR = CAL.TW_FISCAL_YEAR
        LEFT JOIN DIY_UNITS AS B
            ON CAL.TW_FISCAL_YEAR_WEEK = B.TW_FISCAL_YEAR_WEEK
            AND A.SKU = B.SKU 
        LEFT JOIN RETAIL_PRICE AS C 
            ON CAL.TW_FISCAL_YEAR_WEEK = C.TW_FISCAL_YEAR_WEEK 
            AND A.SKU = C.SKU 
        LEFT JOIN PROMO_PRICE AS F 
            ON CAL.TW_FISCAL_YEAR_WEEK = F.TW_FISCAL_YEAR_WEEK
            AND A.SKU = F.SKU
    GROUP BY 
        A.SKU,
        CAL.TW_FISCAL_YEAR_WEEK,
        A.RETAILER,
        B.AAP_QUANTITY,
        C.AAP_ITEM_PRICE,
        B.AAP_QUANTITY,
        F.AAP_PROMO_PRICE

UNION ALL 

   SELECT
        A.SKU,
        CAL.TW_FISCAL_YEAR_WEEK,
        'NAPA' AS RETAILER,
        MEDIAN(A.REGULAR_PRICE) AS TW_COMP_PRICE,
        MEDIAN(A.SALE_PRICE) AS TW_COMP_SALE_PRICE,
        B.AAP_QUANTITY,
        C.AAP_ITEM_PRICE,
        F.AAP_PROMO_PRICE,
        COALESCE(F.AAP_PROMO_PRICE, C.AAP_ITEM_PRICE) AS AAP_EFF_PRICE,
        COALESCE(TW_COMP_SALE_PRICE, TW_COMP_PRICE) AS COMP_EFF_PRICE,
        IFF(AAP_ITEM_PRICE > 0, COMP_EFF_PRICE*AAP_QUANTITY,0) AS COMP_EFF_SALES_EST,
        IFF(AAP_ITEM_PRICE > 0, TW_COMP_PRICE*AAP_QUANTITY, 0) AS COMP_SALES_EST,
        IFF(TW_COMP_PRICE >0, AAP_ITEM_PRICE*AAP_QUANTITY,0) AS AAP_SALES_EST,
        IFF(TW_COMP_PRICE >0, AAP_EFF_PRICE*AAP_QUANTITY,0) AS AAP_EFF_SALES_EST
    FROM AAP_PRCNG_CATALYST_PROD_DB.PUBLIC.VW_CPI_CLEANED_NAPA AS A
        INNER JOIN CAL 
            ON A.FISCAL_WEEK_OF_YEAR = CAL.TW_FISCAL_WEEK_OF_YEAR 
            AND A.FISCAL_YEAR = CAL.TW_FISCAL_YEAR
        LEFT JOIN DIY_UNITS AS B
            ON CAL.TW_FISCAL_YEAR_WEEK = B.TW_FISCAL_YEAR_WEEK
            AND A.SKU = B.SKU 
        LEFT JOIN RETAIL_PRICE AS C 
            ON CAL.TW_FISCAL_YEAR_WEEK = C.TW_FISCAL_YEAR_WEEK 
            AND A.SKU = C.SKU 
        LEFT JOIN PROMO_PRICE AS F 
            ON CAL.TW_FISCAL_YEAR_WEEK = F.TW_FISCAL_YEAR_WEEK
            AND A.SKU = F.SKU
    GROUP BY 
        A.SKU,
        CAL.TW_FISCAL_YEAR_WEEK,
        A.RETAILER,
        B.AAP_QUANTITY,
        C.AAP_ITEM_PRICE,
        B.AAP_QUANTITY,
        F.AAP_PROMO_PRICE
),


COMP_DATA_LW AS (
    SELECT
        A.SKU,
        CAL.TW_FISCAL_YEAR_WEEK,
        'AUTOZONE' AS RETAILER,
        MEDIAN(A.REGULAR_PRICE) AS TW_COMP_PRICE,
        MEDIAN(A.SALE_PRICE) AS TW_COMP_SALE_PRICE,
        B.AAP_QUANTITY,
        C.AAP_ITEM_PRICE,
        F.AAP_PROMO_PRICE,
        COALESCE(F.AAP_PROMO_PRICE, C.AAP_ITEM_PRICE) AS AAP_EFF_PRICE,
        COALESCE(TW_COMP_SALE_PRICE, TW_COMP_PRICE) AS COMP_EFF_PRICE,
        IFF(AAP_ITEM_PRICE > 0, COMP_EFF_PRICE*AAP_QUANTITY,0) AS COMP_EFF_SALES_EST,
        IFF(AAP_ITEM_PRICE > 0, TW_COMP_PRICE*AAP_QUANTITY, 0) AS COMP_SALES_EST,
        IFF(TW_COMP_PRICE >0, AAP_ITEM_PRICE*AAP_QUANTITY,0) AS AAP_SALES_EST,
        IFF(TW_COMP_PRICE >0, AAP_EFF_PRICE*AAP_QUANTITY,0) AS AAP_EFF_SALES_EST
    FROM AAP_PRCNG_CATALYST_PROD_DB.PUBLIC.VW_CPI_CLEANED_AUTOZONE AS A
        INNER JOIN CAL 
            ON A.FISCAL_WEEK_OF_YEAR = CAL.LW_FISCAL_WEEK_OF_YEAR 
            AND A.FISCAL_YEAR = CAL.LW_FISCAL_YEAR
        LEFT JOIN DIY_UNITS AS B
            ON CAL.LW_FISCAL_YEAR_WEEK = B.TW_FISCAL_YEAR_WEEK
            AND A.SKU = B.SKU 
        LEFT JOIN RETAIL_PRICE AS C 
            ON CAL.LW_FISCAL_YEAR_WEEK = C.TW_FISCAL_YEAR_WEEK 
            AND A.SKU = C.SKU 
        LEFT JOIN PROMO_PRICE AS F 
            ON CAL.LW_FISCAL_YEAR_WEEK = F.TW_FISCAL_YEAR_WEEK
            AND A.SKU = F.SKU
    GROUP BY 
        A.SKU,
        CAL.TW_FISCAL_YEAR_WEEK,
        A.RETAILER,
        C.AAP_ITEM_PRICE,
        B.AAP_QUANTITY,
        F.AAP_PROMO_PRICE

UNION ALL 

    SELECT
        A.SKU,
        CAL.TW_FISCAL_YEAR_WEEK,
        'OREILLY' AS RETAILER,
        MEDIAN(A.REGULAR_PRICE) AS TW_COMP_PRICE,
        MEDIAN(A.SALE_PRICE) AS TW_COMP_SALE_PRICE,
        B.AAP_QUANTITY,
        C.AAP_ITEM_PRICE,
        F.AAP_PROMO_PRICE,
        COALESCE(F.AAP_PROMO_PRICE, C.AAP_ITEM_PRICE) AS AAP_EFF_PRICE,
        COALESCE(TW_COMP_SALE_PRICE, TW_COMP_PRICE) AS COMP_EFF_PRICE,
        IFF(AAP_ITEM_PRICE > 0, COMP_EFF_PRICE*AAP_QUANTITY,0) AS COMP_EFF_SALES_EST,
        IFF(AAP_ITEM_PRICE > 0, TW_COMP_PRICE*AAP_QUANTITY, 0) AS COMP_SALES_EST,
        IFF(TW_COMP_PRICE >0, AAP_ITEM_PRICE*AAP_QUANTITY,0) AS AAP_SALES_EST,
        IFF(TW_COMP_PRICE >0, AAP_EFF_PRICE*AAP_QUANTITY,0) AS AAP_EFF_SALES_EST
    FROM AAP_PRCNG_CATALYST_PROD_DB.PUBLIC.VW_CPI_CLEANED_OREILLY AS A
        INNER JOIN CAL 
            ON A.FISCAL_WEEK_OF_YEAR = CAL.LW_FISCAL_WEEK_OF_YEAR 
            AND A.FISCAL_YEAR = CAL.LW_FISCAL_YEAR
        LEFT JOIN DIY_UNITS AS B
            ON CAL.LW_FISCAL_YEAR_WEEK = B.TW_FISCAL_YEAR_WEEK
            AND A.SKU = B.SKU 
        LEFT JOIN RETAIL_PRICE AS C 
            ON CAL.LW_FISCAL_YEAR_WEEK = C.TW_FISCAL_YEAR_WEEK 
            AND A.SKU = C.SKU 
        LEFT JOIN PROMO_PRICE AS F 
            ON CAL.LW_FISCAL_YEAR_WEEK = F.TW_FISCAL_YEAR_WEEK
            AND A.SKU = F.SKU
    GROUP BY 
        A.SKU,
        CAL.TW_FISCAL_YEAR_WEEK,
        A.RETAILER,
        C.AAP_ITEM_PRICE,
        B.AAP_QUANTITY,
        F.AAP_PROMO_PRICE

UNION ALL 
    SELECT
        A.SKU,
        CAL.TW_FISCAL_YEAR_WEEK,
        'NAPA' AS RETAILER,
        MEDIAN(A.REGULAR_PRICE) AS TW_COMP_PRICE,
        MEDIAN(A.SALE_PRICE) AS TW_COMP_SALE_PRICE,
        B.AAP_QUANTITY,
        C.AAP_ITEM_PRICE,
        F.AAP_PROMO_PRICE,
        COALESCE(F.AAP_PROMO_PRICE, C.AAP_ITEM_PRICE) AS AAP_EFF_PRICE,
        COALESCE(TW_COMP_SALE_PRICE, TW_COMP_PRICE) AS COMP_EFF_PRICE,
        IFF(AAP_ITEM_PRICE > 0, COMP_EFF_PRICE*AAP_QUANTITY,0) AS COMP_EFF_SALES_EST,
        IFF(AAP_ITEM_PRICE > 0, TW_COMP_PRICE*AAP_QUANTITY, 0) AS COMP_SALES_EST,
        IFF(TW_COMP_PRICE >0, AAP_ITEM_PRICE*AAP_QUANTITY,0) AS AAP_SALES_EST,
        IFF(TW_COMP_PRICE >0, AAP_EFF_PRICE*AAP_QUANTITY,0) AS AAP_EFF_SALES_EST
    FROM AAP_PRCNG_CATALYST_PROD_DB.PUBLIC.VW_CPI_CLEANED_NAPA AS A
        INNER JOIN CAL 
            ON A.FISCAL_WEEK_OF_YEAR = CAL.LW_FISCAL_WEEK_OF_YEAR 
            AND A.FISCAL_YEAR = CAL.LW_FISCAL_YEAR
        LEFT JOIN DIY_UNITS AS B
            ON CAL.LW_FISCAL_YEAR_WEEK = B.TW_FISCAL_YEAR_WEEK
            AND A.SKU = B.SKU 
        LEFT JOIN RETAIL_PRICE AS C 
            ON CAL.LW_FISCAL_YEAR_WEEK = C.TW_FISCAL_YEAR_WEEK 
            AND A.SKU = C.SKU 
        LEFT JOIN PROMO_PRICE AS F 
            ON CAL.LW_FISCAL_YEAR_WEEK = F.TW_FISCAL_YEAR_WEEK
            AND A.SKU = F.SKU
    GROUP BY 
        A.SKU,
        CAL.TW_FISCAL_YEAR_WEEK,
        A.RETAILER,
        C.AAP_ITEM_PRICE,
        B.AAP_QUANTITY,
        F.AAP_PROMO_PRICE
)

    SELECT 
        CAL.TW_FISCAL_YEAR_WEEK AS FISCAL_WEEK,
        CAL.LW_FISCAL_YEAR_WEEK,
        CAL.TW_FISCAL_WEEK_START_DATE AS FISCAL_WEEK_START_DATE,
        A.SKU,
        A.RETAILER AS COMP_NAME,
        A.TW_COMP_PRICE AS COMP_ITEM_PRICE,
        A.TW_COMP_SALE_PRICE AS COMP_SALE_PRICE,
        B.TW_COMP_PRICE AS LW_COMP_ITEM_PRICE,
        A.AAP_ITEM_PRICE AS AAP_ITEM_PRICE,
        B.AAP_ITEM_PRICE AS AAP_ITEM_PRICE_LW,
        A.AAP_EFF_PRICE AS AAP_EFF_PRICE,
        SUM(A.AAP_EFF_SALES_EST) AS AAP_EFF_SALES_EST,
        COALESCE(COMP_SALE_PRICE, COMP_ITEM_PRICE) AS COMP_EFF_PRICE,
        IFF(A.AAP_ITEM_PRICE>0, A.COMP_EFF_PRICE*A.AAP_QUANTITY,0) AS COMP_EFF_SALES_EST,
        IFF((A.TW_COMP_PRICE) - (B.TW_COMP_PRICE) <> 0, 1, 0) AS PRICE_CHANGE,
        IFF((A.TW_COMP_PRICE) - (B.TW_COMP_PRICE) >  0, 1, 0) AS PRICE_UPSHIFT,
        IFF((A.TW_COMP_PRICE) - (B.TW_COMP_PRICE) <  0, 1, 0) AS PRICE_DOWNSHIFT,
        IFF((A.AAP_ITEM_PRICE - B.AAP_ITEM_PRICE) <> 0, 1, 0) AS AAP_PRICE_CHANGE,
        IFF((A.AAP_ITEM_PRICE - B.AAP_ITEM_PRICE) > 0, 1, 0) AS AAP_PRICE_UPSHIFT,
        IFF((A.AAP_ITEM_PRICE - B.AAP_ITEM_PRICE) < 0, 1, 0) AS AAP_PRICE_DOWNSHIFT,
        SUM(A.AAP_QUANTITY) AS AAP_QUANTITY,
        SUM(B.AAP_QUANTITY) AS AAP_QUANTITY_LW,
        SUM(A.COMP_SALES_EST) AS COMP_SALES_EST,
        SUM(A.AAP_SALES_EST) AS AAP_SALES_EST,
        DIV0(A.AAP_ITEM_PRICE,A.TW_COMP_PRICE) AS CPI,
        P.MERCHANDISE_GROUP_CODE,
        P.MERCHANDISE_SUBCLASS_CODE
    FROM CAL
        INNER JOIN COMP_DATA_TW AS A 
            ON A.TW_FISCAL_YEAR_WEEK = CAL.TW_FISCAL_YEAR_WEEK
        LEFT JOIN COMP_DATA_LW AS B 
            ON A.SKU = B.SKU
            AND A.RETAILER = B.RETAILER
            AND A.TW_FISCAL_YEAR_WEEK = B.TW_FISCAL_YEAR_WEEK
        INNER JOIN PEDW.PUBLIC.CR_DIM_PRODUCT_W_FIN_CGY_V AS P ON A.SKU = P.SKU

    GROUP BY 
        CAL.TW_FISCAL_YEAR_WEEK,
        CAL.LW_FISCAL_YEAR_WEEK,
        CAL.TW_FISCAL_WEEK_START_DATE,
        A.SKU,
        A.RETAILER,
        A.TW_COMP_PRICE,
        A.TW_COMP_SALE_PRICE,
        A.COMP_EFF_PRICE,
        A.AAP_QUANTITY,
        B.TW_COMP_PRICE,
        A.AAP_ITEM_PRICE,
        B.AAP_ITEM_PRICE,
        A.AAP_EFF_PRICE,
        B.AAP_EFF_PRICE,
        P.MERCHANDISE_GROUP_CODE,
        P.MERCHANDISE_SUBCLASS_CODE")







#Read in subclass data + data cleansing for excel file. Purpose of this excel file is to get elasticity info based on subclass. Here I did manipulation to join on sub class description.
elasticity_subclass <- read_excel("elasticitiessubclassupdate.xlsx")

elasticity_subclass$BPG <- trimws(elasticity_subclass$BPG)

elasticity_subclass <- elasticity_subclass %>% rename(SUBCLASS = `Color Selection (Hierarchy)`)

elasticity_subclass$SUBCLASS <- trimws(elasticity_subclass$SUBCLASS)
#Concatenate bpg and subclass names in excel file to later join on subclass

elasticity_subclass$BPG_Sub <- str_c(elasticity_subclass$BPG, ' ', elasticity_subclass$SUBCLASS)

#read in class level data for elasticity

elasticity_class <- read_excel("elasticitiesclass.xlsx")

elasticity_class$BPG <- trimws(elasticity_class$BPG)

elasticity_class <- elasticity_class %>% rename(CLASS = `Plot Detail Selection (Hierarchy)`)

elasticity_class$CLASS <- trimws(elasticity_class$CLASS)
#Concatenate bpg and class names in excel file to later join on subclass

elasticity_class$BPG_Class <- str_c(elasticity_class$BPG, ' ', elasticity_class$CLASS)

elasticity_class<- elasticity_class %>% rename(CLASS_ELASTICITY= 'Elasticity')


#read in department level data for elasticity

elasticity_department <- read_excel("elasticitiesdepartment.xlsx")

elasticity_department$BPG <- trimws(elasticity_department$BPG)

elasticity_department <- elasticity_department %>% rename(DEPARTMENT = `Plot Detail Selection (Hierarchy)`)

elasticity_department$DEPARTMENT <- trimws(elasticity_department$DEPARTMENT)



#Concatenate bpg and department names in excel file to later join on subclass

elasticity_department$BPG_Dep <- str_c(elasticity_department$BPG, ' ', elasticity_department$DEPARTMENT)

elasticity_department<- elasticity_department %>% rename(DEPARTMENT_ELASTICITY = 'Elasticity')

#Read in group level elasticity

elasticity_group <- read_excel("elasticity_group.xlsx")

elasticity_group$BPG <- trimws(elasticity_group$BPG)

elasticity_group <- elasticity_group %>% rename(GROUP = `Plot Detail Selection (Hierarchy)`)

elasticity_group$GROUP <- trimws(elasticity_group$GROUP)



#Concatenate bpg and department names in excel file to later join on subclass

elasticity_group$BPG_GROUP <- str_c(elasticity_group$BPG, ' ', elasticity_group$GROUP)

elasticity_group<- elasticity_group %>% rename(GROUP_ELASTICITY = 'Elasticity')



#For sku_data table, change sku id to character variable to join on other dataframe/table



kvi_sku_report <- kvi_sku_report %>% mutate(SKU_NUMBER = as.character(SKU_NUMBER))

kvi_sku_report <- kvi_sku_report %>% mutate(store_sum = rowSums(across(c(COUNT_STORE_ASSORTED,COUNT_HUB_ASSORTED,COUNT_SUPER_HUB_ASSORTED))))


#Read in elasticity info from snowflake

elasticity_sku <- DBI::dbGetQuery(myconn, "select distinct SKU, ELASTICITY from SHARE_EXT_BCG_AAP_OUTBOUND.OUTBOUND.VW_ELASTICITY where week_num in ('202437')")


#Joining transaction dataframe with sku_data table together





join_data_first <- kvi_transaction %>% left_join(unique(kvi_sku_report), by = c("SKU_CD" = "SKU_NUMBER"))






#Manipulating BPG and Subclass, class and department variables to prepare to join with excel file

join_data_first$BPG_DESC <- toupper(join_data_first$GROUP_DESCRIPTION)

join_data_first$BPG_DESC <- trimws(join_data_first$GROUP_DESCRIPTION)

join_data_first$SUB_DESC <- toupper(join_data_first$SUBCLASS_DESCRIPTION)

join_data_first$SUB_DESC <- trimws(join_data_first$SUBCLASS_DESCRIPTION)

join_data_first$BPG_SubClass <- str_c(join_data_first$GROUP_DESCRIPTION, ' ', join_data_first$SUBCLASS_DESCRIPTION)

join_data_first$BPG_Classcomb<- str_c(join_data_first$GROUP_DESCRIPTION, ' ', join_data_first$CLASS_DESCRIPTION)

join_data_first$BPG_Depcomb <- str_c(join_data_first$GROUP_DESCRIPTION, ' ', join_data_first$DEPARTMENT_DESCRIPTION)

join_data_first$BPG_Groupcomb <- str_c(join_data_first$GROUP_DESCRIPTION, ' ', join_data_first$GROUP_DESCRIPTION)

#Joining excel file based on subclass,class,group and department level

join_data_elasticity <- join_data_first %>% left_join(elasticity_subclass, by = c("BPG_SubClass" = "BPG_Sub"))

join_data_elasticity<- join_data_elasticity %>% left_join(elasticity_class, by = c("BPG_Classcomb" = "BPG_Class"))

join_data_elasticity<- join_data_elasticity %>% left_join(elasticity_department, by = c("BPG_Depcomb" = "BPG_Dep"))

join_data_elasticity<- join_data_elasticity %>% left_join(elasticity_group, by = c("BPG_Groupcomb" = "BPG_GROUP"))


#Joining dataframe with elasticity table

join_data_elasticity <- join_data_elasticity %>% left_join(elasticity_sku, by = c("SKU_CD" = "SKU"))





join_data_elasticity <- transform(join_data_elasticity, new_elasticity = ifelse(is.na(ELASTICITY),CLASS_ELASTICITY,ELASTICITY)) 

join_data_elasticity <- transform(join_data_elasticity, final_elasticity = ifelse(is.na(new_elasticity),DEPARTMENT_ELASTICITY,new_elasticity))

join_data_elasticity <- transform(join_data_elasticity, final_elasticity = ifelse(is.na(new_elasticity),GROUP_ELASTICITY,new_elasticity))




#Grab needed variables to prepare the final dataset to use for the rest of code.  

new_data_elasticity <- join_data_elasticity %>% dplyr::select(SKU_CD,SKU_NM,DESCRIPTION,DEPARTMENT_DESCRIPTION,DEPARTMENT_NUM,GROUP_DESCRIPTION,GROUP_NUM,DIVISION_NUM,DIVISION_DESCRIPTION,CLASS_NUM,CLASS_DESCRIPTION,SUBCLASS_NUM,SUBCLASS_DESCRIPTION,BRAND_DESCRIPTION,INVOICE_COUNT_PRODUCT,SALES,UNITS,GM,GM_RATE,final_elasticity
                                                              ,store_sum)

x_min_elasticity <- new_data_elasticity %>% summarise(global_min_elasticity = min(final_elasticity, na.rm = TRUE))

new_data_elasticity <- cbind(new_data_elasticity,x_min_elasticity) 


#Filters out Sales and units info being 0.

new_data_elasticity <- new_data_elasticity %>% filter(SALES != 0 & UNITS != 0) 








#Filtering out unneeded BPGs + unassigned Subclasses + warranty info

new_data_elasticity <- new_data_elasticity %>% filter(!GROUP_DESCRIPTION %in% c('ELECTRICAL','NUTS/BOLTS/MISC HARDWARE','REFERENCE'))

new_data_elasticity <- new_data_elasticity %>% filter(!SUBCLASS_DESCRIPTION %in% c('UNASSIGNED'))


new_data_elasticity <- new_data_elasticity %>% filter(!grepl('WARRANTY',CLASS_DESCRIPTION))


#This dataframe below is now cleansed and includes the inventory_cost variables.

new_data_elasticity <- new_data_elasticity %>% dplyr::select(SKU_CD,SKU_NM,DESCRIPTION,GROUP_DESCRIPTION,DEPARTMENT_DESCRIPTION,DEPARTMENT_NUM,GROUP_NUM,DIVISION_NUM,DIVISION_DESCRIPTION,CLASS_NUM,CLASS_DESCRIPTION,SUBCLASS_NUM,SUBCLASS_DESCRIPTION,BRAND_DESCRIPTION,INVOICE_COUNT_PRODUCT,SALES,UNITS,GM,GM_RATE,final_elasticity,store_sum,global_min_elasticity)





#impute model with mean by Group number to replace missing elasticity values

imputed_mean_model <- new_data_elasticity %>% group_by(GROUP_NUM) %>% mutate_at("final_elasticity", function(x) replace(x, is.na(x), mean(x,na.rm=TRUE)))






x_kvi_impute <- imputed_mean_model




kvi_output <- x_kvi_impute %>%
  mutate(
    salesunits = UNITS,
    units_x_elas = ifelse(!is.na(salesunits) & !is.na(final_elasticity), salesunits * final_elasticity, NA)
  )





#Replace other missing variables with NA for replacement.These NA values will be replaced later in the code


kvi_output[sapply(kvi_output, is.infinite)] <- NA
kvi_output[sapply(kvi_output, is.nan)] <- NA







#Establish weights.
sales_weight <- 0.22
invoicecountproduct_weight <- 0.20
salesunits_weight <- 0.20
gm_weight <- 0.11
gm_rate_weight <- 0.10
elasticity_weight <- 0.08
store_weight <- 0.09

#weightage for various conditions 
#dummy_store_weight = ifelse(GROUP_DESCRIPTION == c('ENGINES AND TRANSMISSIONS','BATTERIES'),0,0.09)



#establish pgp averages with imputed model. This dataframe establishes sum of sales variable, average of sales, invoice count product, units, gmroi, elasticity and the weighted elasticity.
x_pgp_averages_impute_mean <- kvi_output  %>%
  group_by(GROUP_NUM) %>%
  summarise(
    avg_sales = round(mean(SALES, na.rm = TRUE),2),
    avg_invctprd = mean(INVOICE_COUNT_PRODUCT, na.rm = TRUE),
    avg_salesunits = mean(salesunits, na.rm = TRUE),
    avg_gm = mean(GM, na.rm = TRUE),
    avg_gm_rate = mean(GM_RATE, na.rm = TRUE),
    avg_elasticity = mean(final_elasticity, na.rm = TRUE),
    weighted_elasticity = (sum(units_x_elas,na.rm = TRUE) / sum(salesunits,na.rm = TRUE)),
    avg_store_sum = round(mean(store_sum, na.rm = TRUE),2)
  ) %>% 
  arrange(GROUP_NUM)











#Join kvi_output frame with the pgp averages table.

competitive_score <- kvi_output %>%
  left_join(x_pgp_averages_impute_mean , by = "GROUP_NUM") %>%
  arrange(SKU_CD)




#This dataframe will replace NA values with averages.

competitive_score <- competitive_score %>%
  mutate(
    salesunits = ifelse(is.na(salesunits), avg_salesunits, salesunits),
    GM = ifelse(is.na(GM), 0, GM),
    GM_RATE = ifelse(is.na(GM_RATE), 0, GM_RATE),
    Elasticity = ifelse(is.na(final_elasticity), avg_elasticity, final_elasticity),
    store_sum = ifelse(is.na(store_sum),avg_store_sum,store_sum))


#establishing max values for sales, invoice count product, units, gmroi and elasticity

x_pgp_max <- competitive_score %>%
  group_by(GROUP_NUM) %>%
  summarise(
    max_sales = max(SALES, na.rm =TRUE),
    max_invctprd = max(INVOICE_COUNT_PRODUCT, na.rm = TRUE),
    max_salesunits = max(salesunits, na.rm = TRUE),
    max_gm = max(GM, na.rm = TRUE),
    max_gm_rate = max(GM_RATE, na.rm = TRUE),
    max_store = max(store_sum, na.rm = TRUE)
  ) %>%
  arrange(GROUP_NUM)

#replaces -infinity values with NAs. Next is replacing those NAs with 0s.

x_pgp_max <- as.data.frame(x_pgp_max)

x_pgp_max[sapply(x_pgp_max,is.infinite)] <- NA

x_pgp_max[is.na(x_pgp_max)]<-0

##Create x_competitive_score_2 df. This joins the x_pgp_max dataframe with competitive score dataframe.
x_competitive_score_2 <- competitive_score %>%
  left_join(x_pgp_max, by = "GROUP_NUM") %>%
  arrange(SKU_CD)

#Establish a global elasticity index to use for ranking elasticity.
x_competitive_score_2 <- x_competitive_score_2 %>% mutate(elasticity_index = final_elasticity / global_min_elasticity)


#Establish itemrankscore variable which will rank the SKUs.

x_competitive_score_2 <- x_competitive_score_2 %>%
  mutate(
    sales_index = SALES / max_sales,
    invoicecount_index = INVOICE_COUNT_PRODUCT / max_invctprd,
    units_index = salesunits / max_salesunits,
    gm_index = GM / max_gm,
    gm_rate_index = GM_RATE / max_gm_rate,
    store_index = ifelse(store_sum == 0,0,store_sum / max_store),
    itemrankscore = (
      sales_index * sales_weight +
        invoicecount_index * invoicecountproduct_weight +
        units_index * salesunits_weight +
        gm_index * gm_weight + gm_rate_index * gm_rate_weight + 
        elasticity_index * elasticity_weight + store_index * store_weight
    )
  )



#Sort by descending itemrankscore

x_competitive_score_2 <- x_competitive_score_2 %>%
  arrange(GROUP_NUM, desc(itemrankscore))





## Percentile count variable created for SKUs.

competitive_score <- x_competitive_score_2  %>% arrange(GROUP_NUM, desc(itemrankscore)) %>% group_by(GROUP_NUM) %>% mutate(percentilecount = cumsum(c(1,diff(itemrankscore)!= 0))) %>% ungroup()




#creates variable which is max percentile count by each product

x_pgp_count <- competitive_score %>%
  group_by(GROUP_NUM) %>%
  summarise(
    pgpcount = max(percentilecount)
  )



#joins competitive score and x_pgp_count

x_pgp_count_join <- left_join(competitive_score, x_pgp_count, by = "GROUP_NUM") %>%
  arrange(SKU_CD)




#calculates the percentile ranking for each product
competitive_score<- x_pgp_count_join %>%
  mutate(
    item_percentile = ifelse(percentilecount > 0, 1 - percentilecount / pgpcount, NA)
  )





#DIY role designations


x_role_join <- competitive_score %>% mutate(role = ifelse(GROUP_DESCRIPTION %in% c('BATTERY ACCESSORIES','FLUID MANAGEMENT ACCESSORIES','MOTOR OIL','PERFORMANCE & FUNCTIONAL CHEMICALS') | (GROUP_DESCRIPTION %in% c('BULK CHEMICALS - ANTIFREEZE AND WASHER SOLVENT') & DEPARTMENT_DESCRIPTION %in% c('ANTIFREEZE')),'Traffic', 
                                                          ifelse(GROUP_DESCRIPTION %in% c('AIR FRESHENERS','AIR TOOLS & ACCESSORIES', 'CONSUMABLES','ELECTRONICS','JACKS AND LIFTS','LOANER TOOLS','PROTECTIVE GEAR','SEALANTS, ADHESIVES AND COMPOUNDS','GENERAL MERCH AND BATTERIES','WINTER & SUMMER SEASONAL')| (GROUP_DESCRIPTION %in% c('BULK CHEMICALS - ANTIFREEZE AND WASHER SOLVENT') & DEPARTMENT_DESCRIPTION %in% c('WINDSHIELD WASH')),'Convenience',
                                                                 ifelse(GROUP_DESCRIPTION %in% c('AIR FILTERS','APPEARANCE ACCESSORIES','APPEARANCE CHEMICALS','BATTERIES','BEARINGS, SEALS, HUB ASSEMBLIES','CHASSIS PARTS','HAND & SPECIALTY TOOLS','LIGHTING','OIL FILTERS','RIDE CONTROL','STARTERS & ALTERNATORS','WIPERS','IGNITION/EMISSION - EMISSION SENSORS & VALVES
','IGNITION/EMISSION - IGNITION COMPONENTS','IGNITION/EMISSION - ELECTRICAL COMPONENTS','IGNITION/EMISSION -SPARK PLUGS') | (GROUP_DESCRIPTION %in% c('IGNITION/EMISSION - AIR INJECTION & O2 SENSORS') & DEPARTMENT_DESCRIPTION %in% c('OXYGEN SENSORS'))| (GROUP_DESCRIPTION %in% c('BRAKES - DRUMS AND ROTORS') & CLASS_DESCRIPTION %in% c('ROTORS'))
                                                                        | (GROUP_DESCRIPTION %in% c('BRAKES - FRICTION, PADS & SHOES') & DEPARTMENT_DESCRIPTION %in% c('BRAKE PADS','BRAKES - FRICTION, PADS & SHOES - NGF')),'Destination','Core'))))

#assign role percentiles

final_competitive_score <- x_role_join %>% mutate(item_classification = ifelse(role == 'Destination' & item_percentile <= .50,'Tail',
                                                                               ifelse(role == 'Destination' & item_percentile > .50 & item_percentile < .80,'Core',ifelse(role == 'Destination' & item_percentile >= 0.80,'Head',
                                                                                                                                                                          ifelse(role == 'Core' & item_percentile <= .65,'Tail',
                                                                                                                                                                                 ifelse(role == 'Core' & item_percentile > .65 & item_percentile < .90,'Core',ifelse(role == 'Core' & item_percentile >= .90, 'Head',ifelse(role == 'Convenience' & item_percentile <= .70,'Tail', 
                                                                                                                                                                                                                                                                                                                            ifelse(role == 'Convenience' & item_percentile > .70 & item_percentile < .95, 'Core', ifelse(role =='Convenience' & item_percentile >= .95, 'Head', 
                                                                                                                                                                                                                                                                                                                                                                                                                         ifelse(role == 'Traffic' & item_percentile <= .60, 'Tail', ifelse(role == 'Traffic' & item_percentile > .60 & item_percentile < .85, 'Core', 
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           ifelse(role == 'Traffic' & item_percentile >= 0.85, 'Head','Nothing')))))))))))))

#Mutate lifecycle sku number as character variable

kvi_lifecycle <- kvi_lifecycle %>% mutate(SKU_NUMBER = as.character(SKU_NUMBER))







#Joining lifecycle data
final_join <- final_competitive_score %>% left_join(unique(kvi_lifecycle), by = c("SKU_CD" = "SKU_NUMBER"))





#Arrange final dataframe in correct order as well as trimming down characters to allow files to be output

final_join <- arrange(final_join, percentilecount)

final_join$GROUP_DESCRIPTION <- chartr("-"," ",final_join$GROUP_DESCRIPTION)

final_join$GROUP_DESCRIPTION <- chartr("/"," ",final_join$GROUP_DESCRIPTION)

final_join <- final_join %>% rename(SKU = SKU_CD)


#CPI join. You should do this only when you ran the code to produce the master file and have the summary table breakdown. 

kvi_CPI <- kvi_CPI %>% mutate(SKU= as.character(SKU))

kvi_CPI <- kvi_CPI %>% dplyr::select(SKU,COMP_NAME,CPI)


final_join <- final_join %>% left_join(unique(kvi_CPI), by = c("SKU" = "SKU"))




#Here is where I check for distinct values in Description for BPGS. Can do this for class,department and subclass

final_join_check <- final_join %>% distinct(GROUP_DESCRIPTION,DEPARTMENT_DESCRIPTION,CLASS_DESCRIPTION)



#View table of percentage + count breakdown of roles




final_join_summary <- final_join %>% group_by(GROUP_DESCRIPTION,role,item_classification) %>% summarise(SALES = sum(SALES), observation_count = n())

final_join_summary <- final_join_summary %>% group_by(GROUP_DESCRIPTION) %>% mutate(total_revenue = sum(SALES))

final_join_summary <- final_join_summary %>% group_by(GROUP_DESCRIPTION) %>% mutate(total_count= sum(observation_count))

final_join_summary <- final_join_summary %>% mutate(percentage_revenue = round((SALES / total_revenue)*100,2), observation_percentage = round((observation_count / total_count)*100,2))


#View table of percentage + count breakdown of roles and lifecycle


final_join_summary_item_importance <- final_join %>% group_by(GROUP_DESCRIPTION,role,item_classification,SKU_EPOCH,SKU_EPOCH_SHARED) %>% summarise(SALES = sum(SALES), observation_count = n())

final_join_summary_item_importance <- final_join_summary_item_importance %>% group_by(GROUP_DESCRIPTION) %>% mutate(total_revenue = sum(SALES))

final_join_summary_item_importance <- final_join_summary_item_importance %>% group_by(GROUP_DESCRIPTION) %>% mutate(total_count= sum(observation_count))

final_join_summary_item_importance <- final_join_summary_item_importance %>% mutate(percentage_revenue = round((SALES / total_revenue)*100,2), observation_percentage = round((observation_count / total_count)*100,2))




#View table of percentage + count breakdown of roles and lifecycle by sku


final_join_summary_item_importance_sku <- final_join %>% group_by(SKU,GROUP_DESCRIPTION,role,item_classification,SKU_EPOCH,SKU_EPOCH_SHARED) %>% summarise(SALES = sum(SALES), observation_count = n())

final_join_summary_item_importance_sku <- final_join_summary_item_importance_sku %>% group_by(GROUP_DESCRIPTION) %>% mutate(total_revenue = sum(SALES))

final_join_summary_item_importance_sku <- final_join_summary_item_importance_sku %>% group_by(GROUP_DESCRIPTION) %>% mutate(total_count= sum(observation_count))

final_join_summary_item_importance_sku <- final_join_summary_item_importance_sku %>% mutate(percentage_revenue = round((SALES / total_revenue)*100,2), observation_percentage = round((observation_count / total_count)*100,2))

#final_join_item_importance_sku <- final_join_summary_item_importance_sku %>% mutate(item_importance_ranking = ifelse(SALES >= (0.95 * total_revenue),'Super_Kvi',ifelse(SALES > (0.85 * total_revenue) & SALES < 0.95 * total_revenue,'KVI',ifelse(SALES > 0.60 * total_revenue & SALES < 0.85 * total_revenue,'non_kvi_1','non_kvi_2'))))


#final_join_summary_CPI. Only do this when you produced the tables listed above before joining main queries with CPI

final_join_summary_item_importance_sku_cpi <- final_join %>% group_by(SKU,GROUP_DESCRIPTION,role,item_classification,SKU_EPOCH,SKU_EPOCH_SHARED,CPI,COMP_NAME) %>% summarise(SALES = sum(SALES), observation_count = n())

final_join_summary_item_importance_sku_cpi <- final_join_summary_item_importance_sku_cpi %>% group_by(GROUP_DESCRIPTION) %>% mutate(total_revenue = sum(SALES))

final_join_summary_item_importance_sku_cpi  <- final_join_summary_item_importance_sku_cpi  %>% group_by(GROUP_DESCRIPTION) %>% mutate(total_count= sum(observation_count))

final_join_summary_item_importance_sku_cpi  <-final_join_summary_item_importance_sku_cpi  %>% mutate(percentage_revenue = round((SALES / total_revenue)*100,2), observation_percentage = round((observation_count / total_count)*100,2))




#Code to write out all BPGs info to one file. Two for csv and excel files.

setwd("S:/Retail Pricing/Richard Xiao/KVI/KVI_masterfiles")

write.csv(final_join,"KVI_DIY_masterfile.csv")

write_xlsx(final_join,"KVI_DIY_no_GMROI_masterfile.xlsx")

#Use this bottom line to create master file after joining with CPI query

write_xlsx(final_join,"KVI_DIY_CPI_masterfile.xlsx")

write_xlsx(final_join,"KVI_DIY_CPI_noGmroimasterfile.xlsx")

write.csv(final_join,"KVI_DIY_CPInoGMROI_masterfile.csv")
#Writes out output to excel files.This is for raw data

setwd("C:/Users/Richard.Xiao/OneDrive - Advance Auto Parts/Documents/Excel_Files/KVI_BPG_DIY_excel_output")

customFun = function(DF) {
  write.xlsx(DF,paste0("DIY_Data_BPG_",unique(DF$GROUP_DESCRIPTION),".xlsx"))
  return(DF)
}

final_join %>%
  group_by(GROUP_DESCRIPTION) %>% do(customFun(.))



#Writes out output to excel files.This is for percentages.


setwd("S:/Retail Pricing/Richard Xiao/KVI/KVI_BPG_DIY_summary_breakdown")

customFun = function(DF) {
  write.xlsx(DF,paste0("DIY_Summary_BPG_",unique(DF$GROUP_DESCRIPTION),".xlsx"))
  return(DF)
}

final_join_summary %>%
  group_by(GROUP_DESCRIPTION) %>% do(customFun(.))



#summary by item importance

setwd("S:/Retail Pricing/Richard Xiao/KVI/KVI_item_importance_summary_DIY")

customFun = function(DF) {
  write.xlsx(DF,paste0("KVI_item_importance_Summary_BPG_DIY",unique(DF$GROUP_DESCRIPTION),".xlsx"))
  return(DF)
}

final_join_summary_item_importance %>%
  group_by(GROUP_DESCRIPTION) %>% do(customFun(.))




#Break out summary by sku 

setwd("S:/Retail Pricing/Richard Xiao/KVI/KVI_item_importance_summary_sku_DIY")

customFun = function(DF) {
  write.xlsx(DF,paste0("KVI_item_importance_Summary_sku_BPG_DIY",unique(DF$GROUP_DESCRIPTION),".xlsx"))
  return(DF)
}

final_join_summary_item_importance_sku %>%
  group_by(GROUP_DESCRIPTION) %>% do(customFun(.))



#Break out summary by sku and CPI. Do this only when you joined the final_join dataframe with the CPI query.

setwd("S:/Retail Pricing/Richard Xiao/KVI/KVI_sku_DIY_CPI_summary_table")

customFun = function(DF) {
  write.xlsx(DF,paste0("KVI_item_importance_Summary_sku_cpi_BPG_DIY",unique(DF$GROUP_DESCRIPTION),".xlsx"))
  return(DF)
}

final_join_summary_item_importance_sku_cpi %>%
  group_by(GROUP_DESCRIPTION) %>% do(customFun(.))




kvi standardize







library(mice)
library(ibmdbR)
library(miceadds)
library(DBI)
library(dbplyr)
library(odbc)
library(readxl)
library(sqldf)
library(rJava)
library(plyr)
library(dplyr)
library(readxl)
library(xlsxjars)
library(MASS)
library(dplyr)
library(shiny)
library(stringr)
library(scales) 
library(sqldf)
require(data.table)
library(Rcpp)
library(readr)
library(splitstackshape)
library(dplyr)
library(tidyr)
library(tidyverse)
library(fs)
library(data.table)
library(RMariaDB)
library(splitstackshape)
library(lubridate)
library(openxlsx)
library(readxl)
library(openxlsx)
library(stringr)
library(writexl)
library(Hmisc)
library(imputeTS)
library(VIM)
library(class)
library(foreach)
library(doParallel)
library(kknn)
library(stats)
library(imputeTS)
library(scales)
library(pivottabler)

detach("package:rlang", unload = TRUE)


#Set up working directory + connections. Replace line 54 with your working directory.
setwd("C:/Users/Richard.Xiao/OneDrive - Advance Auto Parts/Documents/Excel_Files")


myconn <- DBI::dbConnect(odbc::odbc(), "SNOWFLAKE", uid="AAP_PRCNG_ANLYTCS_PROD_USER", pwd='p2JiJ@e1i86GXD6g',role="AAP_PRCNG_ANLYTCS_PROD_REPORTING_ROLE",warehouse='PRCNG_ANLYTCS_WH', schema = 'analysis')


myconn2 <- DBI::dbConnect(odbc::odbc(), "SNOWFLAKE", uid="AAP_PRCNG_ANLYTCS_PROD_USER", pwd='p2JiJ@e1i86GXD6g',role="AAP_FINERP_EPM_PROD_REPORTING_ROLE",warehouse='FINERP_EPM_WH', schema = 'analysis')



##Start of KVI queries

##Connection 1 for transaction/financial data

kvi_transaction <- DBI::dbGetQuery(myconn,"select distinct SKU_CD,SKU_NM,DESCRIPTION,DEPARTMENT_DESCRIPTION,DEPARTMENT_NUM,GROUP_DESCRIPTION,GROUP_NUM,DIVISION_NUM,DIVISION_DESCRIPTION,CLASS_NUM,CLASS_DESCRIPTION,SUBCLASS_NUM,SUBCLASS_DESCRIPTION,BRAND_DESCRIPTION,sum(COGS_AMT) as NEW_COGS_AMT,sum(PROFIT_AMT) as GM,sum(REVENUE) as SALES,sum(QUANTITY) as UNITS,count(distinct(ORDER_ID)) as invoice_count_product,GM/NULLIFZERO(SALES) as GM_Rate, UNITS * NEW_COGS_AMT as COGS
from AAP_PRCNG_ANLYTCS_PROD_DB.PUBLIC.vw_p_pricing_transactions as sls
left join AAP_PRCNG_ANLYTCS_PROD_DB.PUBLIC.vw_dim_date as dte
    on DATE(dte.FULL_DATE) = sls.ORDER_DT
left join AAP_PRCNG_ANLYTCS_PROD_DB.PUBLIC.vw_p_dim_product as prod
    on prod.SKU = sls.SKU_CD
left join AAP_PRCNG_ANLYTCS_PROD_DB.PUBLIC.vw_sku as skuss
    on skuss.SKU_ID = sls.SKU_CD
where  FISCAL_PERIOD_IN_YEAR in (6,7,8) and FISCAL_YEAR = '2024' and BRAND_DESCRIPTION not in ('WRLPC', 'CORE') and GROUP_DESCRIPTION not in ('ADVANCE CARE SERVICES','COMMERCIAL JACKS/LIFTS','EXTERNAL SUPPLY','NON-SKU MERCHANDISE','OTHER/CORES','SERVICE','STORE SUPPLIES & FIXTURES','UNDEFINED') and CHANNEL_NM = 'DIY'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14")






##Connections 2-3 for store and dc inventory data


kvi_inventory_period_dc <- DBI::dbGetQuery(myconn2,"select SKU_NUMBER,sum(dc.ON_HAND_COUNT) as committed_inventory
from AAP_DAT_PROD_DB.DAT_BV.dc_inventory_history as dc
where dc.FISCAL_YEAR = 2024 and dc.fiscal_period in (6,7,8)
group by 1")


kvi_inventory_period_store <- DBI::dbGetQuery(myconn2,"select SKU_NUMBER,sum(store.ON_HAND_COUNT) as physical_inventory
from AAP_DAT_PROD_DB.DAT_BV.store_inventory_history as store
where store.FISCAL_YEAR = 2024 and store.fiscal_period in (6,7,8)
group by 1")





#Connection 4 for lifecycle data

kvi_lifecycle <- DBI::dbGetQuery(myconn2,"select AAP_SKU_NUMBER as SKU_NUMBER, SKU_EPOCH,SKU_EPOCH_SHARED
from AAP_DAT_PROD_DB.AVAN.VW_SKU_EPOCH_LATEST")

#Connection 5 for sku_data. This is needed to calculate GMROI
kvi_sku_data <- DBI::dbGetQuery(myconn2,"select SKU_NUMBER, UNIT_COST
from AAP_DAT_PROD_DB.DAT_BV.sku_data
where DISCONTINUED_FLG = 'N'")

#Read in store data 

kvi_sku_report <- DBI::dbGetQuery(myconn2,"select AAP_SKU_NUMBER as SKU_NUMBER, COUNT_STORE_ASSORTED,COUNT_HUB_ASSORTED,COUNT_SUPER_HUB_ASSORTED
from aap_dat_prod_db.avan.vw_sku_reporting_base")

#Read in CPI information

kvi_CPI <- DBI::dbGetQuery(myconn,"WITH CAL AS (
SELECT DISTINCT 
        T.FISCAL_WEEK_START_DATE::DATE AS TW_FISCAL_WEEK_START_DATE,
        T.FISCAL_YEAR AS TW_FISCAL_YEAR,
        T.FISCAL_WEEK_OF_YEAR AS TW_FISCAL_WEEK_OF_YEAR,
        T.FISCAL_YEAR_WEEK AS TW_FISCAL_YEAR_WEEK,
        T.FISCAL_YEAR_PERIOD AS TW_FISCAL_YEAR_PERIOD,
        T.FISCAL_WEEK_END_DATE AS TW_FISCAL_WEEK_END_DATE,
        Y.FISCAL_WEEK_START_DATE::DATE AS LW_FISCAL_WEEK_START_DATE,
        Y.FISCAL_WEEK_OF_YEAR AS LW_FISCAL_WEEK_OF_YEAR,
        Y.FISCAL_YEAR AS LW_FISCAL_YEAR,
        Y.FISCAL_YEAR_WEEK AS LW_FISCAL_YEAR_WEEK,
        Y.FISCAL_WEEK_END_DATE::DATE AS LW_FISCAL_WEEK_END_DATE
    FROM AAP_PRCNG_CATALYST_PROD_DB.PUBLIC.VW_DIM_DATE AS T
        INNER JOIN AAP_PRCNG_CATALYST_PROD_DB.PUBLIC.VW_DIM_DATE AS Y
            ON DATEADD(DAY,-7,T.FISCAL_WEEK_START_DATE) = Y.FISCAL_WEEK_START_DATE
    WHERE 
         TW_FISCAL_YEAR_WEEK IN (202434)
    ),


    
PCAL AS (
    SELECT DISTINCT
        C1.FISCAL_YEAR_PERIOD,
        C.TW_FISCAL_YEAR_PERIOD AS FISCAL_PERIOD,
        DATE(C2.FISCAL_PERIOD_START_DATE) AS ROLLING_BGN_FISCAL_DATE,
        DATE(DATEADD(DAY,-1,C1.FISCAL_PERIOD_START_DATE)) AS ROLLING_END_FISCAL_DATE,
        C.TW_FISCAL_YEAR_WEEK
    FROM (SELECT DISTINCT TW_FISCAL_YEAR_PERIOD, TW_FISCAL_WEEK_START_DATE, TW_FISCAL_YEAR_WEEK FROM CAL) AS C
        INNER JOIN AAP_PRCNG_CATALYST_PROD_DB.PUBLIC.VW_DIM_DATE AS C1
            ON C.TW_FISCAL_YEAR_PERIOD = C1.FISCAL_YEAR_PERIOD
        INNER JOIN AAP_PRCNG_CATALYST_PROD_DB.PUBLIC.VW_DIM_DATE AS C2
            ON C1.FISCAL_YEAR_PERIOD = C2.FISCAL_YEAR_PERIOD + 100
),

MASTER_INTERCHANGE AS(
    SELECT DISTINCT 
        SKU
    FROM AAP_PRCNG_CATALYST_PROD_DB.PUBLIC.VW_COMPETITOR_PRICING_MASTER_INTERCHANGE
),


DIY_UNITS AS (
    SELECT
        A.SKU, 
        SUM(ZEROIFNULL(QUANTITY)) AS AAP_QUANTITY,
        PCAL.FISCAL_YEAR_PERIOD,
        PCAL.TW_FISCAL_YEAR_WEEK
    FROM AAP_PRCNG_CATALYST_PROD_DB.PUBLIC.VW_DIFM_DIY_TRANSACTIONS AS S
    INNER JOIN PCAL 
        ON S.ORDER_DT BETWEEN PCAL.ROLLING_BGN_FISCAL_DATE AND PCAL.ROLLING_END_FISCAL_DATE
    RIGHT JOIN MASTER_INTERCHANGE AS A 
        ON TRY_CAST(SKU_CD AS INTEGER) = A.SKU
    WHERE 
        CHANNEL_NM = 'DIY' 
        AND TRY_TO_NUMBER(SKU_CD) IS NOT NULL
    GROUP BY 
        A.SKU, 
        PCAL.FISCAL_YEAR_PERIOD,
        PCAL.TW_FISCAL_YEAR_WEEK
),

RETAIL_PRICE AS (
    SELECT 
        SKU,
        PRICE AS AAP_ITEM_PRICE,
        TW_FISCAL_YEAR_WEEK,
        TW_FISCAL_WEEK_START_DATE,
        TW_FISCAL_WEEK_END_DATE,
        EFFDATE,
        RN
    FROM(
        SELECT 
            A.SKU,
            PRICE, 
            CAL.TW_FISCAL_YEAR_WEEK,
            CAL.TW_FISCAL_WEEK_END_DATE,
            CAL.TW_FISCAL_WEEK_START_DATE,
            EFFDATE,
            ROW_NUMBER() OVER(PARTITION BY A.SKU, CAL.TW_FISCAL_YEAR_WEEK ORDER BY EFFDATE DESC) AS RN
        FROM AAP_PRCNG_CATALYST_PROD_DB.PUBLIC.VW_RETAIL_PRICE AS R 
            RIGHT JOIN MASTER_INTERCHANGE AS A 
                ON A.SKU = TRY_CAST(R.SKU AS INTEGER)
            INNER JOIN CAL 
                ON DATE(CAL.TW_FISCAL_WEEK_END_DATE) >= EFFDATE
        )
    WHERE RN = 1
    ),

PROMO_PRICE AS(
    SELECT 
        SKU,
        TW_FISCAL_YEAR_WEEK,
        PRICE AS AAP_PROMO_PRICE
    FROM( 
        SELECT
            A.SKU,
            CAL.TW_FISCAL_YEAR_WEEK,
            MEDIAN(PRICE) AS PRICE,
            ROW_NUMBER() OVER(PARTITION BY A.SKU, CAL.TW_FISCAL_YEAR_WEEK ORDER BY START_DATE DESC) AS RN
        FROM AAP_PRCNG_ANLYTCS_PROD_DB.PUBLIC.VW_PROMOTIONAL_PRICE_HISTORY AS Z 
            RIGHT JOIN MASTER_INTERCHANGE AS A 
                ON A.SKU = TRY_CAST(Z.SKU AS INTEGER)
            INNER JOIN CAL 
                ON CAL.TW_FISCAL_WEEK_START_DATE >= START_DATE
        GROUP BY 
            A.SKU,
            TW_FISCAL_YEAR_WEEK,
            START_DATE
        )
    WHERE RN = 1 
),

COMP_DATA_TW AS (
   SELECT
        A.SKU,
        CAL.TW_FISCAL_YEAR_WEEK,
        'AUTOZONE' AS RETAILER,
        MEDIAN(A.REGULAR_PRICE) AS TW_COMP_PRICE,
        MEDIAN(A.SALE_PRICE) AS TW_COMP_SALE_PRICE,
        B.AAP_QUANTITY,
        C.AAP_ITEM_PRICE,
        F.AAP_PROMO_PRICE,
        COALESCE(F.AAP_PROMO_PRICE, C.AAP_ITEM_PRICE) AS AAP_EFF_PRICE,
        COALESCE(TW_COMP_SALE_PRICE, TW_COMP_PRICE) AS COMP_EFF_PRICE,
        IFF(AAP_ITEM_PRICE > 0, COMP_EFF_PRICE*AAP_QUANTITY,0) AS COMP_EFF_SALES_EST,
        IFF(AAP_ITEM_PRICE > 0, TW_COMP_PRICE*AAP_QUANTITY, 0) AS COMP_SALES_EST,
        IFF(TW_COMP_PRICE >0, AAP_ITEM_PRICE*AAP_QUANTITY,0) AS AAP_SALES_EST,
        IFF(TW_COMP_PRICE >0, AAP_EFF_PRICE*AAP_QUANTITY,0) AS AAP_EFF_SALES_EST
    FROM AAP_PRCNG_CATALYST_PROD_DB.PUBLIC.VW_CPI_CLEANED_AUTOZONE AS A
        INNER JOIN CAL 
            ON A.FISCAL_WEEK_OF_YEAR = CAL.TW_FISCAL_WEEK_OF_YEAR 
            AND A.FISCAL_YEAR = CAL.TW_FISCAL_YEAR
        LEFT JOIN DIY_UNITS AS B
            ON CAL.TW_FISCAL_YEAR_WEEK = B.TW_FISCAL_YEAR_WEEK
            AND A.SKU = B.SKU 
        LEFT JOIN RETAIL_PRICE AS C 
            ON CAL.TW_FISCAL_YEAR_WEEK = C.TW_FISCAL_YEAR_WEEK 
            AND A.SKU = C.SKU 
        LEFT JOIN PROMO_PRICE AS F 
            ON CAL.TW_FISCAL_YEAR_WEEK = F.TW_FISCAL_YEAR_WEEK
            AND A.SKU = F.SKU
    GROUP BY 
        A.SKU,
        CAL.TW_FISCAL_YEAR_WEEK,
        A.RETAILER,
        B.AAP_QUANTITY,
        C.AAP_ITEM_PRICE,
        B.AAP_QUANTITY,
        F.AAP_PROMO_PRICE
        

UNION ALL 

   SELECT
        A.SKU,
        CAL.TW_FISCAL_YEAR_WEEK,
        'OREILLY' AS RETAILER,
        MEDIAN(A.REGULAR_PRICE) AS TW_COMP_PRICE,
        MEDIAN(A.SALE_PRICE) AS TW_COMP_SALE_PRICE,
        B.AAP_QUANTITY,
        C.AAP_ITEM_PRICE,
        F.AAP_PROMO_PRICE,
        COALESCE(F.AAP_PROMO_PRICE, C.AAP_ITEM_PRICE) AS AAP_EFF_PRICE,
        COALESCE(TW_COMP_SALE_PRICE, TW_COMP_PRICE) AS COMP_EFF_PRICE,
        IFF(AAP_ITEM_PRICE > 0, COMP_EFF_PRICE*AAP_QUANTITY,0) AS COMP_EFF_SALES_EST,
        IFF(AAP_ITEM_PRICE > 0, TW_COMP_PRICE*AAP_QUANTITY, 0) AS COMP_SALES_EST,
        IFF(TW_COMP_PRICE >0, AAP_ITEM_PRICE*AAP_QUANTITY,0) AS AAP_SALES_EST,
        IFF(TW_COMP_PRICE >0, AAP_EFF_PRICE*AAP_QUANTITY,0) AS AAP_EFF_SALES_EST
    FROM AAP_PRCNG_CATALYST_PROD_DB.PUBLIC.VW_CPI_CLEANED_OREILLY AS A
        INNER JOIN CAL 
            ON A.FISCAL_WEEK_OF_YEAR = CAL.TW_FISCAL_WEEK_OF_YEAR 
            AND A.FISCAL_YEAR = CAL.TW_FISCAL_YEAR
        LEFT JOIN DIY_UNITS AS B
            ON CAL.TW_FISCAL_YEAR_WEEK = B.TW_FISCAL_YEAR_WEEK
            AND A.SKU = B.SKU 
        LEFT JOIN RETAIL_PRICE AS C 
            ON CAL.TW_FISCAL_YEAR_WEEK = C.TW_FISCAL_YEAR_WEEK 
            AND A.SKU = C.SKU 
        LEFT JOIN PROMO_PRICE AS F 
            ON CAL.TW_FISCAL_YEAR_WEEK = F.TW_FISCAL_YEAR_WEEK
            AND A.SKU = F.SKU
    GROUP BY 
        A.SKU,
        CAL.TW_FISCAL_YEAR_WEEK,
        A.RETAILER,
        B.AAP_QUANTITY,
        C.AAP_ITEM_PRICE,
        B.AAP_QUANTITY,
        F.AAP_PROMO_PRICE

UNION ALL 

   SELECT
        A.SKU,
        CAL.TW_FISCAL_YEAR_WEEK,
        'NAPA' AS RETAILER,
        MEDIAN(A.REGULAR_PRICE) AS TW_COMP_PRICE,
        MEDIAN(A.SALE_PRICE) AS TW_COMP_SALE_PRICE,
        B.AAP_QUANTITY,
        C.AAP_ITEM_PRICE,
        F.AAP_PROMO_PRICE,
        COALESCE(F.AAP_PROMO_PRICE, C.AAP_ITEM_PRICE) AS AAP_EFF_PRICE,
        COALESCE(TW_COMP_SALE_PRICE, TW_COMP_PRICE) AS COMP_EFF_PRICE,
        IFF(AAP_ITEM_PRICE > 0, COMP_EFF_PRICE*AAP_QUANTITY,0) AS COMP_EFF_SALES_EST,
        IFF(AAP_ITEM_PRICE > 0, TW_COMP_PRICE*AAP_QUANTITY, 0) AS COMP_SALES_EST,
        IFF(TW_COMP_PRICE >0, AAP_ITEM_PRICE*AAP_QUANTITY,0) AS AAP_SALES_EST,
        IFF(TW_COMP_PRICE >0, AAP_EFF_PRICE*AAP_QUANTITY,0) AS AAP_EFF_SALES_EST
    FROM AAP_PRCNG_CATALYST_PROD_DB.PUBLIC.VW_CPI_CLEANED_NAPA AS A
        INNER JOIN CAL 
            ON A.FISCAL_WEEK_OF_YEAR = CAL.TW_FISCAL_WEEK_OF_YEAR 
            AND A.FISCAL_YEAR = CAL.TW_FISCAL_YEAR
        LEFT JOIN DIY_UNITS AS B
            ON CAL.TW_FISCAL_YEAR_WEEK = B.TW_FISCAL_YEAR_WEEK
            AND A.SKU = B.SKU 
        LEFT JOIN RETAIL_PRICE AS C 
            ON CAL.TW_FISCAL_YEAR_WEEK = C.TW_FISCAL_YEAR_WEEK 
            AND A.SKU = C.SKU 
        LEFT JOIN PROMO_PRICE AS F 
            ON CAL.TW_FISCAL_YEAR_WEEK = F.TW_FISCAL_YEAR_WEEK
            AND A.SKU = F.SKU
    GROUP BY 
        A.SKU,
        CAL.TW_FISCAL_YEAR_WEEK,
        A.RETAILER,
        B.AAP_QUANTITY,
        C.AAP_ITEM_PRICE,
        B.AAP_QUANTITY,
        F.AAP_PROMO_PRICE
),


COMP_DATA_LW AS (
    SELECT
        A.SKU,
        CAL.TW_FISCAL_YEAR_WEEK,
        'AUTOZONE' AS RETAILER,
        MEDIAN(A.REGULAR_PRICE) AS TW_COMP_PRICE,
        MEDIAN(A.SALE_PRICE) AS TW_COMP_SALE_PRICE,
        B.AAP_QUANTITY,
        C.AAP_ITEM_PRICE,
        F.AAP_PROMO_PRICE,
        COALESCE(F.AAP_PROMO_PRICE, C.AAP_ITEM_PRICE) AS AAP_EFF_PRICE,
        COALESCE(TW_COMP_SALE_PRICE, TW_COMP_PRICE) AS COMP_EFF_PRICE,
        IFF(AAP_ITEM_PRICE > 0, COMP_EFF_PRICE*AAP_QUANTITY,0) AS COMP_EFF_SALES_EST,
        IFF(AAP_ITEM_PRICE > 0, TW_COMP_PRICE*AAP_QUANTITY, 0) AS COMP_SALES_EST,
        IFF(TW_COMP_PRICE >0, AAP_ITEM_PRICE*AAP_QUANTITY,0) AS AAP_SALES_EST,
        IFF(TW_COMP_PRICE >0, AAP_EFF_PRICE*AAP_QUANTITY,0) AS AAP_EFF_SALES_EST
    FROM AAP_PRCNG_CATALYST_PROD_DB.PUBLIC.VW_CPI_CLEANED_AUTOZONE AS A
        INNER JOIN CAL 
            ON A.FISCAL_WEEK_OF_YEAR = CAL.LW_FISCAL_WEEK_OF_YEAR 
            AND A.FISCAL_YEAR = CAL.LW_FISCAL_YEAR
        LEFT JOIN DIY_UNITS AS B
            ON CAL.LW_FISCAL_YEAR_WEEK = B.TW_FISCAL_YEAR_WEEK
            AND A.SKU = B.SKU 
        LEFT JOIN RETAIL_PRICE AS C 
            ON CAL.LW_FISCAL_YEAR_WEEK = C.TW_FISCAL_YEAR_WEEK 
            AND A.SKU = C.SKU 
        LEFT JOIN PROMO_PRICE AS F 
            ON CAL.LW_FISCAL_YEAR_WEEK = F.TW_FISCAL_YEAR_WEEK
            AND A.SKU = F.SKU
    GROUP BY 
        A.SKU,
        CAL.TW_FISCAL_YEAR_WEEK,
        A.RETAILER,
        C.AAP_ITEM_PRICE,
        B.AAP_QUANTITY,
        F.AAP_PROMO_PRICE

UNION ALL 

    SELECT
        A.SKU,
        CAL.TW_FISCAL_YEAR_WEEK,
        'OREILLY' AS RETAILER,
        MEDIAN(A.REGULAR_PRICE) AS TW_COMP_PRICE,
        MEDIAN(A.SALE_PRICE) AS TW_COMP_SALE_PRICE,
        B.AAP_QUANTITY,
        C.AAP_ITEM_PRICE,
        F.AAP_PROMO_PRICE,
        COALESCE(F.AAP_PROMO_PRICE, C.AAP_ITEM_PRICE) AS AAP_EFF_PRICE,
        COALESCE(TW_COMP_SALE_PRICE, TW_COMP_PRICE) AS COMP_EFF_PRICE,
        IFF(AAP_ITEM_PRICE > 0, COMP_EFF_PRICE*AAP_QUANTITY,0) AS COMP_EFF_SALES_EST,
        IFF(AAP_ITEM_PRICE > 0, TW_COMP_PRICE*AAP_QUANTITY, 0) AS COMP_SALES_EST,
        IFF(TW_COMP_PRICE >0, AAP_ITEM_PRICE*AAP_QUANTITY,0) AS AAP_SALES_EST,
        IFF(TW_COMP_PRICE >0, AAP_EFF_PRICE*AAP_QUANTITY,0) AS AAP_EFF_SALES_EST
    FROM AAP_PRCNG_CATALYST_PROD_DB.PUBLIC.VW_CPI_CLEANED_OREILLY AS A
        INNER JOIN CAL 
            ON A.FISCAL_WEEK_OF_YEAR = CAL.LW_FISCAL_WEEK_OF_YEAR 
            AND A.FISCAL_YEAR = CAL.LW_FISCAL_YEAR
        LEFT JOIN DIY_UNITS AS B
            ON CAL.LW_FISCAL_YEAR_WEEK = B.TW_FISCAL_YEAR_WEEK
            AND A.SKU = B.SKU 
        LEFT JOIN RETAIL_PRICE AS C 
            ON CAL.LW_FISCAL_YEAR_WEEK = C.TW_FISCAL_YEAR_WEEK 
            AND A.SKU = C.SKU 
        LEFT JOIN PROMO_PRICE AS F 
            ON CAL.LW_FISCAL_YEAR_WEEK = F.TW_FISCAL_YEAR_WEEK
            AND A.SKU = F.SKU
    GROUP BY 
        A.SKU,
        CAL.TW_FISCAL_YEAR_WEEK,
        A.RETAILER,
        C.AAP_ITEM_PRICE,
        B.AAP_QUANTITY,
        F.AAP_PROMO_PRICE

UNION ALL 
    SELECT
        A.SKU,
        CAL.TW_FISCAL_YEAR_WEEK,
        'NAPA' AS RETAILER,
        MEDIAN(A.REGULAR_PRICE) AS TW_COMP_PRICE,
        MEDIAN(A.SALE_PRICE) AS TW_COMP_SALE_PRICE,
        B.AAP_QUANTITY,
        C.AAP_ITEM_PRICE,
        F.AAP_PROMO_PRICE,
        COALESCE(F.AAP_PROMO_PRICE, C.AAP_ITEM_PRICE) AS AAP_EFF_PRICE,
        COALESCE(TW_COMP_SALE_PRICE, TW_COMP_PRICE) AS COMP_EFF_PRICE,
        IFF(AAP_ITEM_PRICE > 0, COMP_EFF_PRICE*AAP_QUANTITY,0) AS COMP_EFF_SALES_EST,
        IFF(AAP_ITEM_PRICE > 0, TW_COMP_PRICE*AAP_QUANTITY, 0) AS COMP_SALES_EST,
        IFF(TW_COMP_PRICE >0, AAP_ITEM_PRICE*AAP_QUANTITY,0) AS AAP_SALES_EST,
        IFF(TW_COMP_PRICE >0, AAP_EFF_PRICE*AAP_QUANTITY,0) AS AAP_EFF_SALES_EST
    FROM AAP_PRCNG_CATALYST_PROD_DB.PUBLIC.VW_CPI_CLEANED_NAPA AS A
        INNER JOIN CAL 
            ON A.FISCAL_WEEK_OF_YEAR = CAL.LW_FISCAL_WEEK_OF_YEAR 
            AND A.FISCAL_YEAR = CAL.LW_FISCAL_YEAR
        LEFT JOIN DIY_UNITS AS B
            ON CAL.LW_FISCAL_YEAR_WEEK = B.TW_FISCAL_YEAR_WEEK
            AND A.SKU = B.SKU 
        LEFT JOIN RETAIL_PRICE AS C 
            ON CAL.LW_FISCAL_YEAR_WEEK = C.TW_FISCAL_YEAR_WEEK 
            AND A.SKU = C.SKU 
        LEFT JOIN PROMO_PRICE AS F 
            ON CAL.LW_FISCAL_YEAR_WEEK = F.TW_FISCAL_YEAR_WEEK
            AND A.SKU = F.SKU
    GROUP BY 
        A.SKU,
        CAL.TW_FISCAL_YEAR_WEEK,
        A.RETAILER,
        C.AAP_ITEM_PRICE,
        B.AAP_QUANTITY,
        F.AAP_PROMO_PRICE
)

    SELECT 
        CAL.TW_FISCAL_YEAR_WEEK AS FISCAL_WEEK,
        CAL.LW_FISCAL_YEAR_WEEK,
        CAL.TW_FISCAL_WEEK_START_DATE AS FISCAL_WEEK_START_DATE,
        A.SKU,
        A.RETAILER AS COMP_NAME,
        A.TW_COMP_PRICE AS COMP_ITEM_PRICE,
        A.TW_COMP_SALE_PRICE AS COMP_SALE_PRICE,
        B.TW_COMP_PRICE AS LW_COMP_ITEM_PRICE,
        A.AAP_ITEM_PRICE AS AAP_ITEM_PRICE,
        B.AAP_ITEM_PRICE AS AAP_ITEM_PRICE_LW,
        A.AAP_EFF_PRICE AS AAP_EFF_PRICE,
        SUM(A.AAP_EFF_SALES_EST) AS AAP_EFF_SALES_EST,
        COALESCE(COMP_SALE_PRICE, COMP_ITEM_PRICE) AS COMP_EFF_PRICE,
        IFF(A.AAP_ITEM_PRICE>0, A.COMP_EFF_PRICE*A.AAP_QUANTITY,0) AS COMP_EFF_SALES_EST,
        IFF((A.TW_COMP_PRICE) - (B.TW_COMP_PRICE) <> 0, 1, 0) AS PRICE_CHANGE,
        IFF((A.TW_COMP_PRICE) - (B.TW_COMP_PRICE) >  0, 1, 0) AS PRICE_UPSHIFT,
        IFF((A.TW_COMP_PRICE) - (B.TW_COMP_PRICE) <  0, 1, 0) AS PRICE_DOWNSHIFT,
        IFF((A.AAP_ITEM_PRICE - B.AAP_ITEM_PRICE) <> 0, 1, 0) AS AAP_PRICE_CHANGE,
        IFF((A.AAP_ITEM_PRICE - B.AAP_ITEM_PRICE) > 0, 1, 0) AS AAP_PRICE_UPSHIFT,
        IFF((A.AAP_ITEM_PRICE - B.AAP_ITEM_PRICE) < 0, 1, 0) AS AAP_PRICE_DOWNSHIFT,
        SUM(A.AAP_QUANTITY) AS AAP_QUANTITY,
        SUM(B.AAP_QUANTITY) AS AAP_QUANTITY_LW,
        SUM(A.COMP_SALES_EST) AS COMP_SALES_EST,
        SUM(A.AAP_SALES_EST) AS AAP_SALES_EST,
        DIV0(A.AAP_ITEM_PRICE,A.TW_COMP_PRICE) AS CPI,
        P.MERCHANDISE_GROUP_CODE,
        P.MERCHANDISE_SUBCLASS_CODE
    FROM CAL
        INNER JOIN COMP_DATA_TW AS A 
            ON A.TW_FISCAL_YEAR_WEEK = CAL.TW_FISCAL_YEAR_WEEK
        LEFT JOIN COMP_DATA_LW AS B 
            ON A.SKU = B.SKU
            AND A.RETAILER = B.RETAILER
            AND A.TW_FISCAL_YEAR_WEEK = B.TW_FISCAL_YEAR_WEEK
        INNER JOIN PEDW.PUBLIC.CR_DIM_PRODUCT_W_FIN_CGY_V AS P ON A.SKU = P.SKU

    GROUP BY 
        CAL.TW_FISCAL_YEAR_WEEK,
        CAL.LW_FISCAL_YEAR_WEEK,
        CAL.TW_FISCAL_WEEK_START_DATE,
        A.SKU,
        A.RETAILER,
        A.TW_COMP_PRICE,
        A.TW_COMP_SALE_PRICE,
        A.COMP_EFF_PRICE,
        A.AAP_QUANTITY,
        B.TW_COMP_PRICE,
        A.AAP_ITEM_PRICE,
        B.AAP_ITEM_PRICE,
        A.AAP_EFF_PRICE,
        B.AAP_EFF_PRICE,
        P.MERCHANDISE_GROUP_CODE,
        P.MERCHANDISE_SUBCLASS_CODE")







#Read in inventory data + data cleansing for excel file. Purpose of this excel file is to get elasticity info based on subclass. Here I did manipulation to join on sub class description.
elasticity_subclass <- read_excel("elasticitiessubclassupdate.xlsx")

elasticity_subclass$BPG <- trimws(elasticity_subclass$BPG)

elasticity_subclass <- elasticity_subclass %>% rename(SUBCLASS = `Color Selection (Hierarchy)`)

elasticity_subclass$SUBCLASS <- trimws(elasticity_subclass$SUBCLASS)
#Concatenate bpg and subclass names in excel file to later join on subclass

elasticity_subclass$BPG_Sub <- str_c(elasticity_subclass$BPG, ' ', elasticity_subclass$SUBCLASS)

#read in class level data for elasticity

elasticity_class <- read_excel("elasticitiesclass.xlsx")

elasticity_class$BPG <- trimws(elasticity_class$BPG)

elasticity_class <- elasticity_class %>% rename(CLASS = `Plot Detail Selection (Hierarchy)`)

elasticity_class$CLASS <- trimws(elasticity_class$CLASS)
#Concatenate bpg and class names in excel file to later join on subclass

elasticity_class$BPG_Class <- str_c(elasticity_class$BPG, ' ', elasticity_class$CLASS)

elasticity_class<- elasticity_class %>% rename(CLASS_ELASTICITY= 'Elasticity')


#read in department level data for elasticity

elasticity_department <- read_excel("elasticitiesdepartment.xlsx")

elasticity_department$BPG <- trimws(elasticity_department$BPG)

elasticity_department <- elasticity_department %>% rename(DEPARTMENT = `Plot Detail Selection (Hierarchy)`)

elasticity_department$DEPARTMENT <- trimws(elasticity_department$DEPARTMENT)

#Concatenate bpg and department names in excel file to later join on subclass

elasticity_department$BPG_Dep <- str_c(elasticity_department$BPG, ' ', elasticity_department$DEPARTMENT)

elasticity_department<- elasticity_department %>% rename(DEPARTMENT_ELASTICITY = 'Elasticity')


#join DC inventory table onto Store inventory table by Sku id.
kvi_inventory <- kvi_inventory_period_dc %>% left_join(unique(kvi_inventory_period_store), by = c("SKU_NUMBER" = "SKU_NUMBER"))

#For inventory and sku_data table, change sku id to character variable to join on other dataframe/table
kvi_inventory <- kvi_inventory %>% mutate(SKU_NUMBER = as.character(SKU_NUMBER))

kvi_sku_data <- kvi_sku_data %>% mutate(SKU_NUMBER = as.character(SKU_NUMBER))

kvi_sku_report <- kvi_sku_report %>% mutate(SKU_NUMBER = as.character(SKU_NUMBER))

kvi_sku_report <- kvi_sku_report %>% mutate(store_sum = rowSums(across(c(COUNT_STORE_ASSORTED,COUNT_HUB_ASSORTED,COUNT_SUPER_HUB_ASSORTED))))

#Joining inventory dataframe with transaction and sku_data table together



join_data_first <- kvi_transaction %>% left_join(unique(kvi_inventory), by = c("SKU_CD"="SKU_NUMBER"))


join_data_first <- join_data_first %>% left_join(unique(kvi_sku_data), by = c("SKU_CD" = "SKU_NUMBER"))

join_data_first <- join_data_first %>% left_join(unique(kvi_sku_report), by = c("SKU_CD" = "SKU_NUMBER"))

#Manipulating BPG and Subclass, class and department variables to prepare to join with excel file

join_data_first$BPG_DESC <- toupper(join_data_first$GROUP_DESCRIPTION)

join_data_first$BPG_DESC <- trimws(join_data_first$GROUP_DESCRIPTION)

join_data_first$SUB_DESC <- toupper(join_data_first$SUBCLASS_DESCRIPTION)

join_data_first$SUB_DESC <- trimws(join_data_first$SUBCLASS_DESCRIPTION)

join_data_first$BPG_SubClass <- str_c(join_data_first$GROUP_DESCRIPTION, ' ', join_data_first$SUBCLASS_DESCRIPTION)

join_data_first$BPG_Classcomb<- str_c(join_data_first$GROUP_DESCRIPTION, ' ', join_data_first$CLASS_DESCRIPTION)

join_data_first$BPG_Depcomb <- str_c(join_data_first$GROUP_DESCRIPTION, ' ', join_data_first$DEPARTMENT_DESCRIPTION)


#Joining excel file based on subclass,class and department level

join_data_elasticity <- join_data_first %>% left_join(elasticity_subclass, by = c("BPG_SubClass" = "BPG_Sub"))

join_data_elasticity<- join_data_elasticity %>% left_join(elasticity_class, by = c("BPG_Classcomb" = "BPG_Class"))

join_data_elasticity<- join_data_elasticity %>% left_join(elasticity_department, by = c("BPG_Depcomb" = "BPG_Dep"))

join_data_elasticity <- transform(join_data_elasticity, new_elasticity = ifelse(is.na(Elasticity),CLASS_ELASTICITY,Elasticity)) 

join_data_elasticity <- transform(join_data_elasticity, final_elasticity = ifelse(is.na(new_elasticity),DEPARTMENT_ELASTICITY,new_elasticity))


#Grab needed variables to prepare the final dataset for use for rest of code.  

new_data_elasticity <- join_data_elasticity %>% dplyr::select(SKU_CD,SKU_NM,DESCRIPTION,DEPARTMENT_DESCRIPTION,DEPARTMENT_NUM,GROUP_DESCRIPTION,GROUP_NUM,DIVISION_NUM,DIVISION_DESCRIPTION,CLASS_NUM,CLASS_DESCRIPTION,SUBCLASS_NUM,SUBCLASS_DESCRIPTION,BRAND_DESCRIPTION,INVOICE_COUNT_PRODUCT,SALES,UNITS,GM,GM_RATE,final_elasticity,COMMITTED_INVENTORY,
                                                              PHYSICAL_INVENTORY,UNIT_COST,COGS,store_sum)



#92711 skus before filtering


#Filters out NA values for Unit_Cost and Sales info being 0.

new_data_elasticity <- new_data_elasticity %>% filter(!is.na(UNIT_COST) & SALES != 0 & UNITS != 0) 






#Creates 2 new variables(These variables get inventory data in dollars) for use to calculate GMROI

new_data_elasticity <- new_data_elasticity %>% mutate(COMMITTED_COST_INVENTORY = COMMITTED_INVENTORY * UNIT_COST, PHYSICAL_COST_INVENTORY = PHYSICAL_INVENTORY * UNIT_COST)



#Filtering out unneeded BPGs + unassigned Subclasses + warranty info

new_data_elasticity <- new_data_elasticity %>% filter(!GROUP_DESCRIPTION %in% c('ELECTRICAL','NUTS/BOLTS/MISC HARDWARE','REFERENCE'))

new_data_elasticity <- new_data_elasticity %>% filter(!SUBCLASS_DESCRIPTION %in% c('UNASSIGNED'))


new_data_elasticity <- new_data_elasticity %>% filter(!grepl('WARRANTY',CLASS_DESCRIPTION))

#Replace NA/missing values for inventory + inventory cost 

new_data_elasticity$COMMITTED_INVENTORY[is.na(new_data_elasticity$COMMITTED_INVENTORY)] <- 0

new_data_elasticity$COMMITTED_COST_INVENTORY[is.na(new_data_elasticity$COMMITTED_COST_INVENTORY)] <- 0

new_data_elasticity$PHYSICAL_INVENTORY[is.na(new_data_elasticity$PHYSICAL_INVENTORY)] <- 0

new_data_elasticity$PHYSICAL_COST_INVENTORY[is.na(new_data_elasticity$PHYSICAL_COST_INVENTORY)] <- 0

new_data_elasticity$store_sum[is.na(new_data_elasticity$store_sum)] <- 0

#This dataframe below is now cleansed and includes the inventory_cost variables.

new_data_elasticity <- new_data_elasticity %>% dplyr::select(SKU_CD,SKU_NM,DESCRIPTION,GROUP_DESCRIPTION,DEPARTMENT_DESCRIPTION,DEPARTMENT_NUM,GROUP_NUM,DIVISION_NUM,DIVISION_DESCRIPTION,CLASS_NUM,CLASS_DESCRIPTION,SUBCLASS_NUM,SUBCLASS_DESCRIPTION,BRAND_DESCRIPTION,INVOICE_COUNT_PRODUCT,SALES,UNITS,GM,GM_RATE,final_elasticity,PHYSICAL_INVENTORY,COMMITTED_INVENTORY,COMMITTED_COST_INVENTORY,
                                                             COGS,PHYSICAL_COST_INVENTORY,UNIT_COST,store_sum)





#impute model with mean by Group number to replace missing elasticity values

imputed_mean_model <- new_data_elasticity %>% group_by(GROUP_NUM) %>% mutate_at("final_elasticity", function(x) replace(x, is.na(x), mean(x,na.rm=TRUE)))



#Code below calculates the average inventory and inventory turn. Will be used to calculate GMROI.


imputed_mean_model <- imputed_mean_model %>% mutate(average_inventory = (COMMITTED_COST_INVENTORY + PHYSICAL_COST_INVENTORY)/3)

#imputed_mean_model$inventory_turn[is.na(imputed_mean_model$inventory_turn)] <- 0


#Establish GMROI formula. For now reference line 229 as formula to use




x_kvi_impute <- imputed_mean_model  %>%
  mutate(weeklygmroi = abs((GM) / average_inventory))


#new gmroi : Annualized GM$(gross margin for the year/ 13 period) / avg inventory()


#COGS = units * cost


##Psuedo code 
#average inventory = (Ending of physical inventory for period 11 + period 12 ending physical inventory) / 2 + (p11 dc + p12 Dc)/2  + (p12 store + p12 store) / 2)

#GM rate / (1-GM Rate)  * Turns (COGS / Average Inventory)

#gmroi = GM % / (1-GM%) * Turns

#annual GMROI

#Create 3 new variables + accounting for NAs(replace NA values with GMROI, Units and salesunits * Elasticity values)

kvi_output <- x_kvi_impute %>%
  mutate(
    salesunits = UNITS,
    units_x_elas = ifelse(!is.na(salesunits) & !is.na(final_elasticity), salesunits * final_elasticity, NA)
  )





#Replace other missing variables with NA for replacement.These NA values will be replaced later in the code


kvi_output[sapply(kvi_output, is.infinite)] <- NA
kvi_output[sapply(kvi_output, is.nan)] <- NA







#Establish weights.
sales_weight <- 0.22
invoicecountproduct_weight <- 0.20
salesunits_weight <- 0.20
weeklygmroi_weight <- 0.21
elasticity_weight <- 0.08
store_weight <- 0.09



#establish pgp averages with imputed model. This dataframe establishes sum of sales variable, average of sales, invoice count product, units, gmroi, elasticity and the weighted elasticity.
x_pgp_averages_impute_mean <- kvi_output  %>%
  group_by(GROUP_NUM) %>%
  summarise(
    avg_sales = round(mean(SALES, na.rm = TRUE),2),
    avg_invctprd = mean(INVOICE_COUNT_PRODUCT, na.rm = TRUE),
    avg_salesunits = mean(salesunits, na.rm = TRUE),
    avg_wklygmroi = mean(weeklygmroi, na.rm = TRUE),
    avg_elasticity = mean(final_elasticity, na.rm = TRUE),
    weighted_elasticity = (sum(units_x_elas,na.rm = TRUE) / sum(salesunits,na.rm = TRUE)),
    avg_store_sum = round(mean(store_sum, na.rm = TRUE),2)
  ) %>% 
  arrange(GROUP_NUM)











#Join kvi_output frame with the pgp averages table.

competitive_score <- kvi_output %>%
  left_join(x_pgp_averages_impute_mean , by = "GROUP_NUM") %>%
  arrange(SKU_CD)




#This dataframe will replace NA values with averages.

competitive_score <- competitive_score %>%
  mutate(
    salesunits = ifelse(is.na(salesunits), avg_salesunits, salesunits),
    weeklygmroi = ifelse(is.na(weeklygmroi), 0, weeklygmroi),
    Elasticity = ifelse(is.na(final_elasticity), avg_elasticity, final_elasticity),
    store_sum = ifelse(is.na(store_sum),avg_store_sum,store_sum))

#Establish a new elasticity classification variable to account for ranking skus with high elasticity but low financials higher than other skus. 

competitive_score <- competitive_score %>% mutate(elasticity_classification = ifelse(abs(Elasticity) >= 0 & abs(Elasticity) < 0.5, 1, ifelse((abs(Elasticity) >= 0.5 & abs(Elasticity) <= 1), 2
                                                                                                                                             , ifelse(abs(Elasticity) > 1 & abs(Elasticity) <= 1.5,3,4))))


#establishing max values for sales, invoice count product, units, gmroi and elasticity

x_pgp_max <- competitive_score %>%
  group_by(GROUP_NUM) %>%
  summarise(
    max_sales = max(SALES, na.rm =TRUE),
    max_invctprd = max(INVOICE_COUNT_PRODUCT, na.rm = TRUE),
    max_salesunits = max(salesunits, na.rm = TRUE),
    max_weeklygmroi = max(weeklygmroi, na.rm = TRUE),
    min_elasticity = min(Elasticity, na.rm = TRUE),
    max_elasticity_classification = max(elasticity_classification, na.rm = TRUE),
    max_store = max(store_sum, na.rm = TRUE)
  ) %>%
  arrange(GROUP_NUM)





##Create x_competitive_score_2 df. This joins the x_pgp_max dataframe with competitive score dataframe.
x_competitive_score_2 <- competitive_score %>%
  left_join(x_pgp_max, by = "GROUP_NUM") %>%
  arrange(SKU_CD)



#Establish itemrankscore variable which will rank the SKUs.

x_competitive_score_2 <- x_competitive_score_2 %>%
  mutate(
    sales_index = SALES / max_sales,
    invoicecount_index = INVOICE_COUNT_PRODUCT / max_invctprd,
    units_index = salesunits / max_salesunits,
    gmroi_index = weeklygmroi / max_weeklygmroi,
    elasticity_index = elasticity_classification / max_elasticity_classification,
    store_index = ifelse(GROUP_DESCRIPTION == 'ENGINES AND TRANSMISSIONS',0,store_sum / max_store),
    itemrankscore = (
      sales_index * sales_weight +
        invoicecount_index * invoicecountproduct_weight +
        units_index * salesunits_weight +
        gmroi_index * weeklygmroi_weight +
        elasticity_index * elasticity_weight + store_index * store_weight
    )
  )



#Sort by descending itemrankscore

x_competitive_score_2 <- x_competitive_score_2 %>%
  arrange(GROUP_NUM, desc(itemrankscore))





## Percentile count variable created for SKUs.

competitive_score <- x_competitive_score_2  %>% arrange(GROUP_NUM, desc(itemrankscore)) %>% group_by(GROUP_NUM) %>% mutate(percentilecount = cumsum(c(1,diff(itemrankscore)!= 0))) %>% ungroup()




#creates variable which is max percentile count by each product

x_pgp_count <- competitive_score %>%
  group_by(GROUP_NUM) %>%
  summarise(
    pgpcount = max(percentilecount)
  )



#joins competitive score and x_pgp_count

x_pgp_count_join <- left_join(competitive_score, x_pgp_count, by = "GROUP_NUM") %>%
  arrange(SKU_CD)




#calculates the percentile ranking for each product
competitive_score<- x_pgp_count_join %>%
  mutate(
    item_percentile = ifelse(percentilecount > 0, 1 - percentilecount / pgpcount, NA)
  )





#DIY role designations


x_role_join <- competitive_score %>% mutate(role = ifelse(GROUP_DESCRIPTION %in% c('BATTERY ACCESSORIES','FLUID MANAGEMENT ACCESSORIES','MOTOR OIL','PERFORMANCE & FUNCTIONAL CHEMICALS') | (GROUP_DESCRIPTION %in% c('BULK CHEMICALS - ANTIFREEZE AND WASHER SOLVENT') & DEPARTMENT_DESCRIPTION %in% c('ANTIFREEZE')),'Traffic', 
                                                          ifelse(GROUP_DESCRIPTION %in% c('AIR FRESHENERS','AIR TOOLS & ACCESSORIES', 'CONSUMABLES','ELECTRONICS','JACKS AND LIFTS','LOANER TOOLS','PROTECTIVE GEAR','SEALANTS, ADHESIVES AND COMPOUNDS','GENERAL MERCH AND BATTERIES','WINTER & SUMMER SEASONAL')| (GROUP_DESCRIPTION %in% c('BULK CHEMICALS - ANTIFREEZE AND WASHER SOLVENT') & DEPARTMENT_DESCRIPTION %in% c('WINDSHIELD WASH')),'Convenience',
                                                                 ifelse(GROUP_DESCRIPTION %in% c('AIR FILTERS','APPEARANCE ACCESSORIES','APPEARANCE CHEMICALS','BATTERIES','BEARINGS, SEALS, HUB ASSEMBLIES','CHASSIS PARTS','HAND & SPECIALTY TOOLS','LIGHTING','OIL FILTERS','RIDE CONTROL','STARTERS & ALTERNATORS','WIPERS','IGNITION/EMISSION - EMISSION SENSORS & VALVES
','IGNITION/EMISSION - IGNITION COMPONENTS','IGNITION/EMISSION - ELECTRICAL COMPONENTS','IGNITION/EMISSION -SPARK PLUGS') | (GROUP_DESCRIPTION %in% c('IGNITION/EMISSION - AIR INJECTION & O2 SENSORS') & DEPARTMENT_DESCRIPTION %in% c('OXYGEN SENSORS'))| (GROUP_DESCRIPTION %in% c('BRAKES - DRUMS AND ROTORS') & CLASS_DESCRIPTION %in% c('ROTORS'))
                                                                        | (GROUP_DESCRIPTION %in% c('BRAKES - FRICTION, PADS & SHOES') & DEPARTMENT_DESCRIPTION %in% c('BRAKE PADS','BRAKES - FRICTION, PADS & SHOES - NGF')),'Destination','Core'))))

#assign role percentiles

final_competitive_score <- x_role_join %>% mutate(item_classification = ifelse(role == 'Destination' & item_percentile <= .50,'Tail',
                                                                               ifelse(role == 'Destination' & item_percentile > .50 & item_percentile < .80,'Core',ifelse(role == 'Destination' & item_percentile >= 0.80,'Head',
                                                                                                                                                                          ifelse(role == 'Core' & item_percentile <= .65,'Tail',
                                                                                                                                                                                 ifelse(role == 'Core' & item_percentile > .65 & item_percentile < .90,'Core',ifelse(role == 'Core' & item_percentile >= .90, 'Head',ifelse(role == 'Convenience' & item_percentile <= .70,'Tail', 
                                                                                                                                                                                                                                                                                                                            ifelse(role == 'Convenience' & item_percentile > .70 & item_percentile < .95, 'Core', ifelse(role =='Convenience' & item_percentile >= .95, 'Head', 
                                                                                                                                                                                                                                                                                                                                                                                                                         ifelse(role == 'Traffic' & item_percentile <= .60, 'Tail', ifelse(role == 'Traffic' & item_percentile > .60 & item_percentile < .85, 'Core', 
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           ifelse(role == 'Traffic' & item_percentile >= 0.85, 'Head','Nothing')))))))))))))

#Mutate lifecycle sku number as character variable

kvi_lifecycle <- kvi_lifecycle %>% mutate(SKU_NUMBER = as.character(SKU_NUMBER))







#Joining lifecycle data
final_join <- final_competitive_score %>% left_join(unique(kvi_lifecycle), by = c("SKU_CD" = "SKU_NUMBER"))





##final_join <- final_join %>% dplyr::select(-WEEK_NUM)

final_join <- arrange(final_join, percentilecount)

final_join$GROUP_DESCRIPTION <- chartr("-"," ",final_join$GROUP_DESCRIPTION)

final_join$GROUP_DESCRIPTION <- chartr("/"," ",final_join$GROUP_DESCRIPTION)

final_join <- final_join %>% rename(SKU = SKU_CD)

#filter out unit cost less than 1.00 and late lifecycle stages 
final_join <- final_join %>% filter(!(UNIT_COST< 1.00 & SKU_EPOCH %in% c('DISPOSITION','BEYOND_DISPOSITION','TO_BE_DISCONTINUED','DISCONTINUED','FDO_POST_PEAK')& item_classification %in% c('Head')))



#CPI join. Do this last 

kvi_CPI <- kvi_CPI %>% mutate(SKU= as.character(SKU))

kvi_CPI <- kvi_CPI %>% dplyr::select(SKU,COMP_NAME,CPI)


final_join <- final_join %>% left_join(unique(kvi_CPI), by = c("SKU" = "SKU"))




#Here is where I check for distinct values in Description for BPGS. Can do this for class,department and subclass

final_join_check <- final_join %>% distinct(GROUP_DESCRIPTION,DEPARTMENT_DESCRIPTION,CLASS_DESCRIPTION)



#View table of percentage + count breakdown of roles and lifecycle




final_join_summary <- final_join %>% group_by(GROUP_DESCRIPTION,role,item_classification) %>% summarise(SALES = sum(SALES), observation_count = n())

final_join_summary <- final_join_summary %>% group_by(GROUP_DESCRIPTION) %>% mutate(total_revenue = sum(SALES))

final_join_summary <- final_join_summary %>% group_by(GROUP_DESCRIPTION) %>% mutate(total_count= sum(observation_count))

final_join_summary <- final_join_summary %>% mutate(percentage_revenue = round((SALES / total_revenue)*100,2), observation_percentage = round((observation_count / total_count)*100,2))


#View table of percentage + count breakdown of roles and lifecycle


final_join_summary_item_importance <- final_join %>% group_by(GROUP_DESCRIPTION,role,item_classification,SKU_EPOCH,SKU_EPOCH_SHARED) %>% summarise(SALES = sum(SALES), observation_count = n())

final_join_summary_item_importance <- final_join_summary_item_importance %>% group_by(GROUP_DESCRIPTION) %>% mutate(total_revenue = sum(SALES))

final_join_summary_item_importance <- final_join_summary_item_importance %>% group_by(GROUP_DESCRIPTION) %>% mutate(total_count= sum(observation_count))

final_join_summary_item_importance <- final_join_summary_item_importance %>% mutate(percentage_revenue = round((SALES / total_revenue)*100,2), observation_percentage = round((observation_count / total_count)*100,2))




#View table of percentage + count breakdown of roles and lifecycle by sku


final_join_summary_item_importance_sku <- final_join %>% group_by(SKU,GROUP_DESCRIPTION,role,item_classification,SKU_EPOCH,SKU_EPOCH_SHARED) %>% summarise(SALES = sum(SALES), observation_count = n())

final_join_summary_item_importance_sku <- final_join_summary_item_importance_sku %>% group_by(GROUP_DESCRIPTION) %>% mutate(total_revenue = sum(SALES))

final_join_summary_item_importance_sku <- final_join_summary_item_importance_sku %>% group_by(GROUP_DESCRIPTION) %>% mutate(total_count= sum(observation_count))

final_join_summary_item_importance_sku <- final_join_summary_item_importance_sku %>% mutate(percentage_revenue = round((SALES / total_revenue)*100,2), observation_percentage = round((observation_count / total_count)*100,2))

#final_join_item_importance_sku <- final_join_summary_item_importance_sku %>% mutate(item_importance_ranking = ifelse(SALES >= (0.95 * total_revenue),'Super_Kvi',ifelse(SALES > (0.85 * total_revenue) & SALES < 0.95 * total_revenue,'KVI',ifelse(SALES > 0.60 * total_revenue & SALES < 0.85 * total_revenue,'non_kvi_1','non_kvi_2'))))


#final_join_summary_CPI

final_join_summary_item_importance_sku_cpi <- final_join %>% group_by(SKU,GROUP_DESCRIPTION,role,item_classification,SKU_EPOCH,SKU_EPOCH_SHARED,CPI,COMP_NAME) %>% summarise(SALES = sum(SALES), observation_count = n())

final_join_summary_item_importance_sku_cpi <- final_join_summary_item_importance_sku_cpi %>% group_by(GROUP_DESCRIPTION) %>% mutate(total_revenue = sum(SALES))

final_join_summary_item_importance_sku_cpi  <- final_join_summary_item_importance_sku_cpi  %>% group_by(GROUP_DESCRIPTION) %>% mutate(total_count= sum(observation_count))

final_join_summary_item_importance_sku_cpi  <-final_join_summary_item_importance_sku_cpi  %>% mutate(percentage_revenue = round((SALES / total_revenue)*100,2), observation_percentage = round((observation_count / total_count)*100,2))




#Code to write out all BPGs info to one file. Two for csv and excel files.

setwd("S:/Retail Pricing/Richard Xiao/KVI/KVI_masterfiles")

write.csv(final_join,"KVI_DIY_masterfile.csv")

write_xlsx(final_join,"KVI_DIY_masterfile.xlsx")

#Use this bottom line to create master file after joining with CPI query

write_xlsx(final_join,"KVI_DIY_CPI_masterfile.xlsx")

#Writes out output to excel files.This is for raw data

setwd("C:/Users/Richard.Xiao/OneDrive - Advance Auto Parts/Documents/Excel_Files/KVI_BPG_DIY_excel_output")

customFun = function(DF) {
  write.xlsx(DF,paste0("DIY_Data_BPG_",unique(DF$GROUP_DESCRIPTION),".xlsx"))
  return(DF)
}

final_join %>%
  group_by(GROUP_DESCRIPTION) %>% do(customFun(.))



#Writes out output to excel files.This is for percentages.


setwd("C:/Users/Richard.Xiao/OneDrive - Advance Auto Parts/Documents/Excel_Files/KVI_BPG_DIY_summary_breakdown")

customFun = function(DF) {
  write.xlsx(DF,paste0("DIY_Summary_BPG_",unique(DF$GROUP_DESCRIPTION),".xlsx"))
  return(DF)
}

final_join_summary %>%
  group_by(GROUP_DESCRIPTION) %>% do(customFun(.))

write.csv(final_join_summary,"KVI_DIY_summary_masterfile.csv")

#summary by item importance

setwd("C:/Users/Richard.Xiao/OneDrive - Advance Auto Parts/Documents/Excel_Files/KVI_item_importance_summary_DIY")

customFun = function(DF) {
  write.xlsx(DF,paste0("KVI_item_importance_Summary_BPG_DIY",unique(DF$GROUP_DESCRIPTION),".xlsx"))
  return(DF)
}

final_join_summary_item_importance %>%
  group_by(GROUP_DESCRIPTION) %>% do(customFun(.))




#Break out summary by sku 

setwd("C:/Users/Richard.Xiao/OneDrive - Advance Auto Parts/Documents/Excel_Files/KVI_item_importance_summary_sku_DIY")

customFun = function(DF) {
  write.xlsx(DF,paste0("KVI_item_importance_Summary_sku_BPG_DIY",unique(DF$GROUP_DESCRIPTION),".xlsx"))
  return(DF)
}

final_join_summary_item_importance_sku %>%
  group_by(GROUP_DESCRIPTION) %>% do(customFun(.))



#Break out summary by sku and CPI

setwd("C:/Users/Richard.Xiao/OneDrive - Advance Auto Parts/Documents/Excel_Files/KVI_sku_DIY_CPI_summary_table")

customFun = function(DF) {
  write.xlsx(DF,paste0("KVI_item_importance_Summary_sku_cpi_BPG_DIY",unique(DF$GROUP_DESCRIPTION),".xlsx"))
  return(DF)
}

final_join_summary_item_importance_sku_cpi %>%
  group_by(GROUP_DESCRIPTION) %>% do(customFun(.))






kvi with clustering



library(mice)
library(ibmdbR)
library(miceadds)
library(DBI)
library(dbplyr)
library(odbc)
library(readxl)
library(sqldf)
library(rJava)
library(plyr)
library(dplyr)
library(readxl)
library(xlsxjars)
library(MASS)
library(dplyr)
library(shiny)
library(stringr)
library(scales) 
library(sqldf)
require(data.table)
library(Rcpp)
library(readr)
library(splitstackshape)
library(dplyr)
library(tidyr)
library(tidyverse)
library(fs)
library(data.table)
library(RMariaDB)
library(splitstackshape)
library(lubridate)
library(openxlsx)
library(readxl)
library(openxlsx)
library(stringr)
library(writexl)
library(Hmisc)
library(imputeTS)
library(VIM)
library(class)
library(foreach)
library(doParallel)
library(kknn)
library(stats)
library(imputeTS)
library(scales)
library(pivottabler)

detach("package:rlang", unload = TRUE)


#Set up working directory + connections. Replace line 54 with your working directory.
setwd("C:/Users/Richard.Xiao/OneDrive - Advance Auto Parts/Documents/Excel_Files")


myconn <- DBI::dbConnect(odbc::odbc(), "SNOWFLAKE", uid="AAP_PRCNG_ANLYTCS_PROD_USER", pwd='p2JiJ@e1i86GXD6g',role="AAP_PRCNG_ANLYTCS_PROD_REPORTING_ROLE",warehouse='PRCNG_ANLYTCS_WH', schema = 'analysis')


myconn2 <- DBI::dbConnect(odbc::odbc(), "SNOWFLAKE", uid="AAP_PRCNG_ANLYTCS_PROD_USER", pwd='p2JiJ@e1i86GXD6g',role="AAP_FINERP_EPM_PROD_REPORTING_ROLE",warehouse='FINERP_EPM_WH', schema = 'analysis')



##Start of KVI queries

##Connection 1 for transaction/financial data

kvi_transaction <- DBI::dbGetQuery(myconn,"select distinct SKU_CD,SKU_NM,DESCRIPTION,DEPARTMENT_DESCRIPTION,DEPARTMENT_NUM,GROUP_DESCRIPTION,GROUP_NUM,DIVISION_NUM,DIVISION_DESCRIPTION,CLASS_NUM,CLASS_DESCRIPTION,SUBCLASS_NUM,SUBCLASS_DESCRIPTION,BRAND_DESCRIPTION,sum(COGS_AMT) as NEW_COGS_AMT,sum(PROFIT_AMT) as GM,sum(REVENUE) as SALES,sum(QUANTITY) as UNITS,count(distinct(ORDER_ID)) as invoice_count_product,GM/NULLIFZERO(SALES) as GM_Rate, UNITS * NEW_COGS_AMT as COGS
from AAP_PRCNG_ANLYTCS_PROD_DB.PUBLIC.vw_p_pricing_transactions as sls
left join AAP_PRCNG_ANLYTCS_PROD_DB.PUBLIC.vw_dim_date as dte
    on DATE(dte.FULL_DATE) = sls.ORDER_DT
left join AAP_PRCNG_ANLYTCS_PROD_DB.PUBLIC.vw_p_dim_product as prod
    on prod.SKU = sls.SKU_CD
left join AAP_PRCNG_ANLYTCS_PROD_DB.PUBLIC.vw_sku as skuss
    on skuss.SKU_ID = sls.SKU_CD
where  FISCAL_PERIOD_IN_YEAR in (6,7,8) and FISCAL_YEAR = '2024' and BRAND_DESCRIPTION not in ('WRLPC', 'CORE') and GROUP_DESCRIPTION not in ('ADVANCE CARE SERVICES','COMMERCIAL JACKS/LIFTS','EXTERNAL SUPPLY','NON-SKU MERCHANDISE','OTHER/CORES','SERVICE','STORE SUPPLIES & FIXTURES','UNDEFINED') and CHANNEL_NM = 'DIY'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14")






##Connections 2-3 for store and dc inventory data


kvi_inventory_period_dc <- DBI::dbGetQuery(myconn2,"select SKU_NUMBER,sum(dc.ON_HAND_COUNT) as committed_inventory
from AAP_DAT_PROD_DB.DAT_BV.dc_inventory_history as dc
where dc.FISCAL_YEAR = 2024 and dc.fiscal_period in (6,7,8)
group by 1")


kvi_inventory_period_store <- DBI::dbGetQuery(myconn2,"select SKU_NUMBER,sum(store.ON_HAND_COUNT) as physical_inventory
from AAP_DAT_PROD_DB.DAT_BV.store_inventory_history as store
where store.FISCAL_YEAR = 2024 and store.fiscal_period in (6,7,8)
group by 1")





#Connection 4 for lifecycle data

kvi_lifecycle <- DBI::dbGetQuery(myconn2,"select AAP_SKU_NUMBER as SKU_NUMBER, SKU_EPOCH,SKU_EPOCH_SHARED
from AAP_DAT_PROD_DB.AVAN.VW_SKU_EPOCH_LATEST")

#Connection 5 for sku_data. This is needed to calculate GMROI
kvi_sku_data <- DBI::dbGetQuery(myconn2,"select SKU_NUMBER, UNIT_COST
from AAP_DAT_PROD_DB.DAT_BV.sku_data
where DISCONTINUED_FLG = 'N'")

#Read in store data 

kvi_sku_report <- DBI::dbGetQuery(myconn2,"select AAP_SKU_NUMBER as SKU_NUMBER, COUNT_STORE_ASSORTED,COUNT_HUB_ASSORTED,COUNT_SUPER_HUB_ASSORTED
from aap_dat_prod_db.avan.vw_sku_reporting_base")

#Read in CPI information

kvi_CPI <- DBI::dbGetQuery(myconn,"WITH CAL AS (
SELECT DISTINCT 
        T.FISCAL_WEEK_START_DATE::DATE AS TW_FISCAL_WEEK_START_DATE,
        T.FISCAL_YEAR AS TW_FISCAL_YEAR,
        T.FISCAL_WEEK_OF_YEAR AS TW_FISCAL_WEEK_OF_YEAR,
        T.FISCAL_YEAR_WEEK AS TW_FISCAL_YEAR_WEEK,
        T.FISCAL_YEAR_PERIOD AS TW_FISCAL_YEAR_PERIOD,
        T.FISCAL_WEEK_END_DATE AS TW_FISCAL_WEEK_END_DATE,
        Y.FISCAL_WEEK_START_DATE::DATE AS LW_FISCAL_WEEK_START_DATE,
        Y.FISCAL_WEEK_OF_YEAR AS LW_FISCAL_WEEK_OF_YEAR,
        Y.FISCAL_YEAR AS LW_FISCAL_YEAR,
        Y.FISCAL_YEAR_WEEK AS LW_FISCAL_YEAR_WEEK,
        Y.FISCAL_WEEK_END_DATE::DATE AS LW_FISCAL_WEEK_END_DATE
    FROM AAP_PRCNG_CATALYST_PROD_DB.PUBLIC.VW_DIM_DATE AS T
        INNER JOIN AAP_PRCNG_CATALYST_PROD_DB.PUBLIC.VW_DIM_DATE AS Y
            ON DATEADD(DAY,-7,T.FISCAL_WEEK_START_DATE) = Y.FISCAL_WEEK_START_DATE
    WHERE 
         TW_FISCAL_YEAR_WEEK IN (202434)
    ),


    
PCAL AS (
    SELECT DISTINCT
        C1.FISCAL_YEAR_PERIOD,
        C.TW_FISCAL_YEAR_PERIOD AS FISCAL_PERIOD,
        DATE(C2.FISCAL_PERIOD_START_DATE) AS ROLLING_BGN_FISCAL_DATE,
        DATE(DATEADD(DAY,-1,C1.FISCAL_PERIOD_START_DATE)) AS ROLLING_END_FISCAL_DATE,
        C.TW_FISCAL_YEAR_WEEK
    FROM (SELECT DISTINCT TW_FISCAL_YEAR_PERIOD, TW_FISCAL_WEEK_START_DATE, TW_FISCAL_YEAR_WEEK FROM CAL) AS C
        INNER JOIN AAP_PRCNG_CATALYST_PROD_DB.PUBLIC.VW_DIM_DATE AS C1
            ON C.TW_FISCAL_YEAR_PERIOD = C1.FISCAL_YEAR_PERIOD
        INNER JOIN AAP_PRCNG_CATALYST_PROD_DB.PUBLIC.VW_DIM_DATE AS C2
            ON C1.FISCAL_YEAR_PERIOD = C2.FISCAL_YEAR_PERIOD + 100
),

MASTER_INTERCHANGE AS(
    SELECT DISTINCT 
        SKU
    FROM AAP_PRCNG_CATALYST_PROD_DB.PUBLIC.VW_COMPETITOR_PRICING_MASTER_INTERCHANGE
),


DIY_UNITS AS (
    SELECT
        A.SKU, 
        SUM(ZEROIFNULL(QUANTITY)) AS AAP_QUANTITY,
        PCAL.FISCAL_YEAR_PERIOD,
        PCAL.TW_FISCAL_YEAR_WEEK
    FROM AAP_PRCNG_CATALYST_PROD_DB.PUBLIC.VW_DIFM_DIY_TRANSACTIONS AS S
    INNER JOIN PCAL 
        ON S.ORDER_DT BETWEEN PCAL.ROLLING_BGN_FISCAL_DATE AND PCAL.ROLLING_END_FISCAL_DATE
    RIGHT JOIN MASTER_INTERCHANGE AS A 
        ON TRY_CAST(SKU_CD AS INTEGER) = A.SKU
    WHERE 
        CHANNEL_NM = 'DIY' 
        AND TRY_TO_NUMBER(SKU_CD) IS NOT NULL
    GROUP BY 
        A.SKU, 
        PCAL.FISCAL_YEAR_PERIOD,
        PCAL.TW_FISCAL_YEAR_WEEK
),

RETAIL_PRICE AS (
    SELECT 
        SKU,
        PRICE AS AAP_ITEM_PRICE,
        TW_FISCAL_YEAR_WEEK,
        TW_FISCAL_WEEK_START_DATE,
        TW_FISCAL_WEEK_END_DATE,
        EFFDATE,
        RN
    FROM(
        SELECT 
            A.SKU,
            PRICE, 
            CAL.TW_FISCAL_YEAR_WEEK,
            CAL.TW_FISCAL_WEEK_END_DATE,
            CAL.TW_FISCAL_WEEK_START_DATE,
            EFFDATE,
            ROW_NUMBER() OVER(PARTITION BY A.SKU, CAL.TW_FISCAL_YEAR_WEEK ORDER BY EFFDATE DESC) AS RN
        FROM AAP_PRCNG_CATALYST_PROD_DB.PUBLIC.VW_RETAIL_PRICE AS R 
            RIGHT JOIN MASTER_INTERCHANGE AS A 
                ON A.SKU = TRY_CAST(R.SKU AS INTEGER)
            INNER JOIN CAL 
                ON DATE(CAL.TW_FISCAL_WEEK_END_DATE) >= EFFDATE
        )
    WHERE RN = 1
    ),

PROMO_PRICE AS(
    SELECT 
        SKU,
        TW_FISCAL_YEAR_WEEK,
        PRICE AS AAP_PROMO_PRICE
    FROM( 
        SELECT
            A.SKU,
            CAL.TW_FISCAL_YEAR_WEEK,
            MEDIAN(PRICE) AS PRICE,
            ROW_NUMBER() OVER(PARTITION BY A.SKU, CAL.TW_FISCAL_YEAR_WEEK ORDER BY START_DATE DESC) AS RN
        FROM AAP_PRCNG_ANLYTCS_PROD_DB.PUBLIC.VW_PROMOTIONAL_PRICE_HISTORY AS Z 
            RIGHT JOIN MASTER_INTERCHANGE AS A 
                ON A.SKU = TRY_CAST(Z.SKU AS INTEGER)
            INNER JOIN CAL 
                ON CAL.TW_FISCAL_WEEK_START_DATE >= START_DATE
        GROUP BY 
            A.SKU,
            TW_FISCAL_YEAR_WEEK,
            START_DATE
        )
    WHERE RN = 1 
),

COMP_DATA_TW AS (
   SELECT
        A.SKU,
        CAL.TW_FISCAL_YEAR_WEEK,
        'AUTOZONE' AS RETAILER,
        MEDIAN(A.REGULAR_PRICE) AS TW_COMP_PRICE,
        MEDIAN(A.SALE_PRICE) AS TW_COMP_SALE_PRICE,
        B.AAP_QUANTITY,
        C.AAP_ITEM_PRICE,
        F.AAP_PROMO_PRICE,
        COALESCE(F.AAP_PROMO_PRICE, C.AAP_ITEM_PRICE) AS AAP_EFF_PRICE,
        COALESCE(TW_COMP_SALE_PRICE, TW_COMP_PRICE) AS COMP_EFF_PRICE,
        IFF(AAP_ITEM_PRICE > 0, COMP_EFF_PRICE*AAP_QUANTITY,0) AS COMP_EFF_SALES_EST,
        IFF(AAP_ITEM_PRICE > 0, TW_COMP_PRICE*AAP_QUANTITY, 0) AS COMP_SALES_EST,
        IFF(TW_COMP_PRICE >0, AAP_ITEM_PRICE*AAP_QUANTITY,0) AS AAP_SALES_EST,
        IFF(TW_COMP_PRICE >0, AAP_EFF_PRICE*AAP_QUANTITY,0) AS AAP_EFF_SALES_EST
    FROM AAP_PRCNG_CATALYST_PROD_DB.PUBLIC.VW_CPI_CLEANED_AUTOZONE AS A
        INNER JOIN CAL 
            ON A.FISCAL_WEEK_OF_YEAR = CAL.TW_FISCAL_WEEK_OF_YEAR 
            AND A.FISCAL_YEAR = CAL.TW_FISCAL_YEAR
        LEFT JOIN DIY_UNITS AS B
            ON CAL.TW_FISCAL_YEAR_WEEK = B.TW_FISCAL_YEAR_WEEK
            AND A.SKU = B.SKU 
        LEFT JOIN RETAIL_PRICE AS C 
            ON CAL.TW_FISCAL_YEAR_WEEK = C.TW_FISCAL_YEAR_WEEK 
            AND A.SKU = C.SKU 
        LEFT JOIN PROMO_PRICE AS F 
            ON CAL.TW_FISCAL_YEAR_WEEK = F.TW_FISCAL_YEAR_WEEK
            AND A.SKU = F.SKU
    GROUP BY 
        A.SKU,
        CAL.TW_FISCAL_YEAR_WEEK,
        A.RETAILER,
        B.AAP_QUANTITY,
        C.AAP_ITEM_PRICE,
        B.AAP_QUANTITY,
        F.AAP_PROMO_PRICE
        

UNION ALL 

   SELECT
        A.SKU,
        CAL.TW_FISCAL_YEAR_WEEK,
        'OREILLY' AS RETAILER,
        MEDIAN(A.REGULAR_PRICE) AS TW_COMP_PRICE,
        MEDIAN(A.SALE_PRICE) AS TW_COMP_SALE_PRICE,
        B.AAP_QUANTITY,
        C.AAP_ITEM_PRICE,
        F.AAP_PROMO_PRICE,
        COALESCE(F.AAP_PROMO_PRICE, C.AAP_ITEM_PRICE) AS AAP_EFF_PRICE,
        COALESCE(TW_COMP_SALE_PRICE, TW_COMP_PRICE) AS COMP_EFF_PRICE,
        IFF(AAP_ITEM_PRICE > 0, COMP_EFF_PRICE*AAP_QUANTITY,0) AS COMP_EFF_SALES_EST,
        IFF(AAP_ITEM_PRICE > 0, TW_COMP_PRICE*AAP_QUANTITY, 0) AS COMP_SALES_EST,
        IFF(TW_COMP_PRICE >0, AAP_ITEM_PRICE*AAP_QUANTITY,0) AS AAP_SALES_EST,
        IFF(TW_COMP_PRICE >0, AAP_EFF_PRICE*AAP_QUANTITY,0) AS AAP_EFF_SALES_EST
    FROM AAP_PRCNG_CATALYST_PROD_DB.PUBLIC.VW_CPI_CLEANED_OREILLY AS A
        INNER JOIN CAL 
            ON A.FISCAL_WEEK_OF_YEAR = CAL.TW_FISCAL_WEEK_OF_YEAR 
            AND A.FISCAL_YEAR = CAL.TW_FISCAL_YEAR
        LEFT JOIN DIY_UNITS AS B
            ON CAL.TW_FISCAL_YEAR_WEEK = B.TW_FISCAL_YEAR_WEEK
            AND A.SKU = B.SKU 
        LEFT JOIN RETAIL_PRICE AS C 
            ON CAL.TW_FISCAL_YEAR_WEEK = C.TW_FISCAL_YEAR_WEEK 
            AND A.SKU = C.SKU 
        LEFT JOIN PROMO_PRICE AS F 
            ON CAL.TW_FISCAL_YEAR_WEEK = F.TW_FISCAL_YEAR_WEEK
            AND A.SKU = F.SKU
    GROUP BY 
        A.SKU,
        CAL.TW_FISCAL_YEAR_WEEK,
        A.RETAILER,
        B.AAP_QUANTITY,
        C.AAP_ITEM_PRICE,
        B.AAP_QUANTITY,
        F.AAP_PROMO_PRICE

UNION ALL 

   SELECT
        A.SKU,
        CAL.TW_FISCAL_YEAR_WEEK,
        'NAPA' AS RETAILER,
        MEDIAN(A.REGULAR_PRICE) AS TW_COMP_PRICE,
        MEDIAN(A.SALE_PRICE) AS TW_COMP_SALE_PRICE,
        B.AAP_QUANTITY,
        C.AAP_ITEM_PRICE,
        F.AAP_PROMO_PRICE,
        COALESCE(F.AAP_PROMO_PRICE, C.AAP_ITEM_PRICE) AS AAP_EFF_PRICE,
        COALESCE(TW_COMP_SALE_PRICE, TW_COMP_PRICE) AS COMP_EFF_PRICE,
        IFF(AAP_ITEM_PRICE > 0, COMP_EFF_PRICE*AAP_QUANTITY,0) AS COMP_EFF_SALES_EST,
        IFF(AAP_ITEM_PRICE > 0, TW_COMP_PRICE*AAP_QUANTITY, 0) AS COMP_SALES_EST,
        IFF(TW_COMP_PRICE >0, AAP_ITEM_PRICE*AAP_QUANTITY,0) AS AAP_SALES_EST,
        IFF(TW_COMP_PRICE >0, AAP_EFF_PRICE*AAP_QUANTITY,0) AS AAP_EFF_SALES_EST
    FROM AAP_PRCNG_CATALYST_PROD_DB.PUBLIC.VW_CPI_CLEANED_NAPA AS A
        INNER JOIN CAL 
            ON A.FISCAL_WEEK_OF_YEAR = CAL.TW_FISCAL_WEEK_OF_YEAR 
            AND A.FISCAL_YEAR = CAL.TW_FISCAL_YEAR
        LEFT JOIN DIY_UNITS AS B
            ON CAL.TW_FISCAL_YEAR_WEEK = B.TW_FISCAL_YEAR_WEEK
            AND A.SKU = B.SKU 
        LEFT JOIN RETAIL_PRICE AS C 
            ON CAL.TW_FISCAL_YEAR_WEEK = C.TW_FISCAL_YEAR_WEEK 
            AND A.SKU = C.SKU 
        LEFT JOIN PROMO_PRICE AS F 
            ON CAL.TW_FISCAL_YEAR_WEEK = F.TW_FISCAL_YEAR_WEEK
            AND A.SKU = F.SKU
    GROUP BY 
        A.SKU,
        CAL.TW_FISCAL_YEAR_WEEK,
        A.RETAILER,
        B.AAP_QUANTITY,
        C.AAP_ITEM_PRICE,
        B.AAP_QUANTITY,
        F.AAP_PROMO_PRICE
),


COMP_DATA_LW AS (
    SELECT
        A.SKU,
        CAL.TW_FISCAL_YEAR_WEEK,
        'AUTOZONE' AS RETAILER,
        MEDIAN(A.REGULAR_PRICE) AS TW_COMP_PRICE,
        MEDIAN(A.SALE_PRICE) AS TW_COMP_SALE_PRICE,
        B.AAP_QUANTITY,
        C.AAP_ITEM_PRICE,
        F.AAP_PROMO_PRICE,
        COALESCE(F.AAP_PROMO_PRICE, C.AAP_ITEM_PRICE) AS AAP_EFF_PRICE,
        COALESCE(TW_COMP_SALE_PRICE, TW_COMP_PRICE) AS COMP_EFF_PRICE,
        IFF(AAP_ITEM_PRICE > 0, COMP_EFF_PRICE*AAP_QUANTITY,0) AS COMP_EFF_SALES_EST,
        IFF(AAP_ITEM_PRICE > 0, TW_COMP_PRICE*AAP_QUANTITY, 0) AS COMP_SALES_EST,
        IFF(TW_COMP_PRICE >0, AAP_ITEM_PRICE*AAP_QUANTITY,0) AS AAP_SALES_EST,
        IFF(TW_COMP_PRICE >0, AAP_EFF_PRICE*AAP_QUANTITY,0) AS AAP_EFF_SALES_EST
    FROM AAP_PRCNG_CATALYST_PROD_DB.PUBLIC.VW_CPI_CLEANED_AUTOZONE AS A
        INNER JOIN CAL 
            ON A.FISCAL_WEEK_OF_YEAR = CAL.LW_FISCAL_WEEK_OF_YEAR 
            AND A.FISCAL_YEAR = CAL.LW_FISCAL_YEAR
        LEFT JOIN DIY_UNITS AS B
            ON CAL.LW_FISCAL_YEAR_WEEK = B.TW_FISCAL_YEAR_WEEK
            AND A.SKU = B.SKU 
        LEFT JOIN RETAIL_PRICE AS C 
            ON CAL.LW_FISCAL_YEAR_WEEK = C.TW_FISCAL_YEAR_WEEK 
            AND A.SKU = C.SKU 
        LEFT JOIN PROMO_PRICE AS F 
            ON CAL.LW_FISCAL_YEAR_WEEK = F.TW_FISCAL_YEAR_WEEK
            AND A.SKU = F.SKU
    GROUP BY 
        A.SKU,
        CAL.TW_FISCAL_YEAR_WEEK,
        A.RETAILER,
        C.AAP_ITEM_PRICE,
        B.AAP_QUANTITY,
        F.AAP_PROMO_PRICE

UNION ALL 

    SELECT
        A.SKU,
        CAL.TW_FISCAL_YEAR_WEEK,
        'OREILLY' AS RETAILER,
        MEDIAN(A.REGULAR_PRICE) AS TW_COMP_PRICE,
        MEDIAN(A.SALE_PRICE) AS TW_COMP_SALE_PRICE,
        B.AAP_QUANTITY,
        C.AAP_ITEM_PRICE,
        F.AAP_PROMO_PRICE,
        COALESCE(F.AAP_PROMO_PRICE, C.AAP_ITEM_PRICE) AS AAP_EFF_PRICE,
        COALESCE(TW_COMP_SALE_PRICE, TW_COMP_PRICE) AS COMP_EFF_PRICE,
        IFF(AAP_ITEM_PRICE > 0, COMP_EFF_PRICE*AAP_QUANTITY,0) AS COMP_EFF_SALES_EST,
        IFF(AAP_ITEM_PRICE > 0, TW_COMP_PRICE*AAP_QUANTITY, 0) AS COMP_SALES_EST,
        IFF(TW_COMP_PRICE >0, AAP_ITEM_PRICE*AAP_QUANTITY,0) AS AAP_SALES_EST,
        IFF(TW_COMP_PRICE >0, AAP_EFF_PRICE*AAP_QUANTITY,0) AS AAP_EFF_SALES_EST
    FROM AAP_PRCNG_CATALYST_PROD_DB.PUBLIC.VW_CPI_CLEANED_OREILLY AS A
        INNER JOIN CAL 
            ON A.FISCAL_WEEK_OF_YEAR = CAL.LW_FISCAL_WEEK_OF_YEAR 
            AND A.FISCAL_YEAR = CAL.LW_FISCAL_YEAR
        LEFT JOIN DIY_UNITS AS B
            ON CAL.LW_FISCAL_YEAR_WEEK = B.TW_FISCAL_YEAR_WEEK
            AND A.SKU = B.SKU 
        LEFT JOIN RETAIL_PRICE AS C 
            ON CAL.LW_FISCAL_YEAR_WEEK = C.TW_FISCAL_YEAR_WEEK 
            AND A.SKU = C.SKU 
        LEFT JOIN PROMO_PRICE AS F 
            ON CAL.LW_FISCAL_YEAR_WEEK = F.TW_FISCAL_YEAR_WEEK
            AND A.SKU = F.SKU
    GROUP BY 
        A.SKU,
        CAL.TW_FISCAL_YEAR_WEEK,
        A.RETAILER,
        C.AAP_ITEM_PRICE,
        B.AAP_QUANTITY,
        F.AAP_PROMO_PRICE

UNION ALL 
    SELECT
        A.SKU,
        CAL.TW_FISCAL_YEAR_WEEK,
        'NAPA' AS RETAILER,
        MEDIAN(A.REGULAR_PRICE) AS TW_COMP_PRICE,
        MEDIAN(A.SALE_PRICE) AS TW_COMP_SALE_PRICE,
        B.AAP_QUANTITY,
        C.AAP_ITEM_PRICE,
        F.AAP_PROMO_PRICE,
        COALESCE(F.AAP_PROMO_PRICE, C.AAP_ITEM_PRICE) AS AAP_EFF_PRICE,
        COALESCE(TW_COMP_SALE_PRICE, TW_COMP_PRICE) AS COMP_EFF_PRICE,
        IFF(AAP_ITEM_PRICE > 0, COMP_EFF_PRICE*AAP_QUANTITY,0) AS COMP_EFF_SALES_EST,
        IFF(AAP_ITEM_PRICE > 0, TW_COMP_PRICE*AAP_QUANTITY, 0) AS COMP_SALES_EST,
        IFF(TW_COMP_PRICE >0, AAP_ITEM_PRICE*AAP_QUANTITY,0) AS AAP_SALES_EST,
        IFF(TW_COMP_PRICE >0, AAP_EFF_PRICE*AAP_QUANTITY,0) AS AAP_EFF_SALES_EST
    FROM AAP_PRCNG_CATALYST_PROD_DB.PUBLIC.VW_CPI_CLEANED_NAPA AS A
        INNER JOIN CAL 
            ON A.FISCAL_WEEK_OF_YEAR = CAL.LW_FISCAL_WEEK_OF_YEAR 
            AND A.FISCAL_YEAR = CAL.LW_FISCAL_YEAR
        LEFT JOIN DIY_UNITS AS B
            ON CAL.LW_FISCAL_YEAR_WEEK = B.TW_FISCAL_YEAR_WEEK
            AND A.SKU = B.SKU 
        LEFT JOIN RETAIL_PRICE AS C 
            ON CAL.LW_FISCAL_YEAR_WEEK = C.TW_FISCAL_YEAR_WEEK 
            AND A.SKU = C.SKU 
        LEFT JOIN PROMO_PRICE AS F 
            ON CAL.LW_FISCAL_YEAR_WEEK = F.TW_FISCAL_YEAR_WEEK
            AND A.SKU = F.SKU
    GROUP BY 
        A.SKU,
        CAL.TW_FISCAL_YEAR_WEEK,
        A.RETAILER,
        C.AAP_ITEM_PRICE,
        B.AAP_QUANTITY,
        F.AAP_PROMO_PRICE
)

    SELECT 
        CAL.TW_FISCAL_YEAR_WEEK AS FISCAL_WEEK,
        CAL.LW_FISCAL_YEAR_WEEK,
        CAL.TW_FISCAL_WEEK_START_DATE AS FISCAL_WEEK_START_DATE,
        A.SKU,
        A.RETAILER AS COMP_NAME,
        A.TW_COMP_PRICE AS COMP_ITEM_PRICE,
        A.TW_COMP_SALE_PRICE AS COMP_SALE_PRICE,
        B.TW_COMP_PRICE AS LW_COMP_ITEM_PRICE,
        A.AAP_ITEM_PRICE AS AAP_ITEM_PRICE,
        B.AAP_ITEM_PRICE AS AAP_ITEM_PRICE_LW,
        A.AAP_EFF_PRICE AS AAP_EFF_PRICE,
        SUM(A.AAP_EFF_SALES_EST) AS AAP_EFF_SALES_EST,
        COALESCE(COMP_SALE_PRICE, COMP_ITEM_PRICE) AS COMP_EFF_PRICE,
        IFF(A.AAP_ITEM_PRICE>0, A.COMP_EFF_PRICE*A.AAP_QUANTITY,0) AS COMP_EFF_SALES_EST,
        IFF((A.TW_COMP_PRICE) - (B.TW_COMP_PRICE) <> 0, 1, 0) AS PRICE_CHANGE,
        IFF((A.TW_COMP_PRICE) - (B.TW_COMP_PRICE) >  0, 1, 0) AS PRICE_UPSHIFT,
        IFF((A.TW_COMP_PRICE) - (B.TW_COMP_PRICE) <  0, 1, 0) AS PRICE_DOWNSHIFT,
        IFF((A.AAP_ITEM_PRICE - B.AAP_ITEM_PRICE) <> 0, 1, 0) AS AAP_PRICE_CHANGE,
        IFF((A.AAP_ITEM_PRICE - B.AAP_ITEM_PRICE) > 0, 1, 0) AS AAP_PRICE_UPSHIFT,
        IFF((A.AAP_ITEM_PRICE - B.AAP_ITEM_PRICE) < 0, 1, 0) AS AAP_PRICE_DOWNSHIFT,
        SUM(A.AAP_QUANTITY) AS AAP_QUANTITY,
        SUM(B.AAP_QUANTITY) AS AAP_QUANTITY_LW,
        SUM(A.COMP_SALES_EST) AS COMP_SALES_EST,
        SUM(A.AAP_SALES_EST) AS AAP_SALES_EST,
        DIV0(A.AAP_ITEM_PRICE,A.TW_COMP_PRICE) AS CPI,
        P.MERCHANDISE_GROUP_CODE,
        P.MERCHANDISE_SUBCLASS_CODE
    FROM CAL
        INNER JOIN COMP_DATA_TW AS A 
            ON A.TW_FISCAL_YEAR_WEEK = CAL.TW_FISCAL_YEAR_WEEK
        LEFT JOIN COMP_DATA_LW AS B 
            ON A.SKU = B.SKU
            AND A.RETAILER = B.RETAILER
            AND A.TW_FISCAL_YEAR_WEEK = B.TW_FISCAL_YEAR_WEEK
        INNER JOIN PEDW.PUBLIC.CR_DIM_PRODUCT_W_FIN_CGY_V AS P ON A.SKU = P.SKU

    GROUP BY 
        CAL.TW_FISCAL_YEAR_WEEK,
        CAL.LW_FISCAL_YEAR_WEEK,
        CAL.TW_FISCAL_WEEK_START_DATE,
        A.SKU,
        A.RETAILER,
        A.TW_COMP_PRICE,
        A.TW_COMP_SALE_PRICE,
        A.COMP_EFF_PRICE,
        A.AAP_QUANTITY,
        B.TW_COMP_PRICE,
        A.AAP_ITEM_PRICE,
        B.AAP_ITEM_PRICE,
        A.AAP_EFF_PRICE,
        B.AAP_EFF_PRICE,
        P.MERCHANDISE_GROUP_CODE,
        P.MERCHANDISE_SUBCLASS_CODE")







#Read in inventory data + data cleansing for excel file. Purpose of this excel file is to get elasticity info based on subclass. Here I did manipulation to join on sub class description.
elasticity_subclass <- read_excel("elasticitiessubclassupdate.xlsx")

elasticity_subclass$BPG <- trimws(elasticity_subclass$BPG)

elasticity_subclass <- elasticity_subclass %>% rename(SUBCLASS = `Color Selection (Hierarchy)`)

elasticity_subclass$SUBCLASS <- trimws(elasticity_subclass$SUBCLASS)
#Concatenate bpg and subclass names in excel file to later join on subclass

elasticity_subclass$BPG_Sub <- str_c(elasticity_subclass$BPG, ' ', elasticity_subclass$SUBCLASS)

#read in class level data for elasticity

elasticity_class <- read_excel("elasticitiesclass.xlsx")

elasticity_class$BPG <- trimws(elasticity_class$BPG)

elasticity_class <- elasticity_class %>% rename(CLASS = `Plot Detail Selection (Hierarchy)`)

elasticity_class$CLASS <- trimws(elasticity_class$CLASS)
#Concatenate bpg and class names in excel file to later join on subclass

elasticity_class$BPG_Class <- str_c(elasticity_class$BPG, ' ', elasticity_class$CLASS)

elasticity_class<- elasticity_class %>% rename(CLASS_ELASTICITY= 'Elasticity')


#read in department level data for elasticity

elasticity_department <- read_excel("elasticitiesdepartment.xlsx")

elasticity_department$BPG <- trimws(elasticity_department$BPG)

elasticity_department <- elasticity_department %>% rename(DEPARTMENT = `Plot Detail Selection (Hierarchy)`)

elasticity_department$DEPARTMENT <- trimws(elasticity_department$DEPARTMENT)

#Concatenate bpg and department names in excel file to later join on subclass

elasticity_department$BPG_Dep <- str_c(elasticity_department$BPG, ' ', elasticity_department$DEPARTMENT)

elasticity_department<- elasticity_department %>% rename(DEPARTMENT_ELASTICITY = 'Elasticity')


#join DC inventory table onto Store inventory table by Sku id.
kvi_inventory <- kvi_inventory_period_dc %>% left_join(unique(kvi_inventory_period_store), by = c("SKU_NUMBER" = "SKU_NUMBER"))

#For inventory and sku_data table, change sku id to character variable to join on other dataframe/table
kvi_inventory <- kvi_inventory %>% mutate(SKU_NUMBER = as.character(SKU_NUMBER))

kvi_sku_data <- kvi_sku_data %>% mutate(SKU_NUMBER = as.character(SKU_NUMBER))

kvi_sku_report <- kvi_sku_report %>% mutate(SKU_NUMBER = as.character(SKU_NUMBER))

kvi_sku_report <- kvi_sku_report %>% mutate(store_sum = rowSums(across(c(COUNT_STORE_ASSORTED,COUNT_HUB_ASSORTED,COUNT_SUPER_HUB_ASSORTED))))

#Joining inventory dataframe with transaction and sku_data table together



join_data_first <- kvi_transaction %>% left_join(unique(kvi_inventory), by = c("SKU_CD"="SKU_NUMBER"))


join_data_first <- join_data_first %>% left_join(unique(kvi_sku_data), by = c("SKU_CD" = "SKU_NUMBER"))

join_data_first <- join_data_first %>% left_join(unique(kvi_sku_report), by = c("SKU_CD" = "SKU_NUMBER"))

#Manipulating BPG and Subclass, class and department variables to prepare to join with excel file

join_data_first$BPG_DESC <- toupper(join_data_first$GROUP_DESCRIPTION)

join_data_first$BPG_DESC <- trimws(join_data_first$GROUP_DESCRIPTION)

join_data_first$SUB_DESC <- toupper(join_data_first$SUBCLASS_DESCRIPTION)

join_data_first$SUB_DESC <- trimws(join_data_first$SUBCLASS_DESCRIPTION)

join_data_first$BPG_SubClass <- str_c(join_data_first$GROUP_DESCRIPTION, ' ', join_data_first$SUBCLASS_DESCRIPTION)

join_data_first$BPG_Classcomb<- str_c(join_data_first$GROUP_DESCRIPTION, ' ', join_data_first$CLASS_DESCRIPTION)

join_data_first$BPG_Depcomb <- str_c(join_data_first$GROUP_DESCRIPTION, ' ', join_data_first$DEPARTMENT_DESCRIPTION)


#Joining excel file based on subclass,class and department level

join_data_elasticity <- join_data_first %>% left_join(elasticity_subclass, by = c("BPG_SubClass" = "BPG_Sub"))

join_data_elasticity<- join_data_elasticity %>% left_join(elasticity_class, by = c("BPG_Classcomb" = "BPG_Class"))

join_data_elasticity<- join_data_elasticity %>% left_join(elasticity_department, by = c("BPG_Depcomb" = "BPG_Dep"))

join_data_elasticity <- transform(join_data_elasticity, new_elasticity = ifelse(is.na(Elasticity),CLASS_ELASTICITY,Elasticity)) 

join_data_elasticity <- transform(join_data_elasticity, final_elasticity = ifelse(is.na(new_elasticity),DEPARTMENT_ELASTICITY,new_elasticity))


#Grab needed variables to prepare the final dataset for use for rest of code.  

new_data_elasticity <- join_data_elasticity %>% dplyr::select(SKU_CD,SKU_NM,DESCRIPTION,DEPARTMENT_DESCRIPTION,DEPARTMENT_NUM,GROUP_DESCRIPTION,GROUP_NUM,DIVISION_NUM,DIVISION_DESCRIPTION,CLASS_NUM,CLASS_DESCRIPTION,SUBCLASS_NUM,SUBCLASS_DESCRIPTION,BRAND_DESCRIPTION,INVOICE_COUNT_PRODUCT,SALES,UNITS,GM,GM_RATE,final_elasticity,COMMITTED_INVENTORY,
                                                              PHYSICAL_INVENTORY,UNIT_COST,COGS,store_sum)



#92711 skus before filtering


#Filters out NA values for Unit_Cost and Sales info being 0.

new_data_elasticity <- new_data_elasticity %>% filter(!is.na(UNIT_COST) & SALES != 0 & UNITS != 0) 






#Creates 2 new variables(These variables get inventory data in dollars) for use to calculate GMROI

new_data_elasticity <- new_data_elasticity %>% mutate(COMMITTED_COST_INVENTORY = COMMITTED_INVENTORY * UNIT_COST, PHYSICAL_COST_INVENTORY = PHYSICAL_INVENTORY * UNIT_COST)



#Filtering out unneeded BPGs + unassigned Subclasses + warranty info

new_data_elasticity <- new_data_elasticity %>% filter(!GROUP_DESCRIPTION %in% c('ELECTRICAL','NUTS/BOLTS/MISC HARDWARE','REFERENCE'))

new_data_elasticity <- new_data_elasticity %>% filter(!SUBCLASS_DESCRIPTION %in% c('UNASSIGNED'))


new_data_elasticity <- new_data_elasticity %>% filter(!grepl('WARRANTY',CLASS_DESCRIPTION))

#Replace NA/missing values for inventory + inventory cost 

new_data_elasticity$COMMITTED_INVENTORY[is.na(new_data_elasticity$COMMITTED_INVENTORY)] <- 0

new_data_elasticity$COMMITTED_COST_INVENTORY[is.na(new_data_elasticity$COMMITTED_COST_INVENTORY)] <- 0

new_data_elasticity$PHYSICAL_INVENTORY[is.na(new_data_elasticity$PHYSICAL_INVENTORY)] <- 0

new_data_elasticity$PHYSICAL_COST_INVENTORY[is.na(new_data_elasticity$PHYSICAL_COST_INVENTORY)] <- 0

new_data_elasticity$store_sum[is.na(new_data_elasticity$store_sum)] <- 0

#This dataframe below is now cleansed and includes the inventory_cost variables.

new_data_elasticity <- new_data_elasticity %>% dplyr::select(SKU_CD,SKU_NM,DESCRIPTION,GROUP_DESCRIPTION,DEPARTMENT_DESCRIPTION,DEPARTMENT_NUM,GROUP_NUM,DIVISION_NUM,DIVISION_DESCRIPTION,CLASS_NUM,CLASS_DESCRIPTION,SUBCLASS_NUM,SUBCLASS_DESCRIPTION,BRAND_DESCRIPTION,INVOICE_COUNT_PRODUCT,SALES,UNITS,GM,GM_RATE,final_elasticity,PHYSICAL_INVENTORY,COMMITTED_INVENTORY,COMMITTED_COST_INVENTORY,
                                                             COGS,PHYSICAL_COST_INVENTORY,UNIT_COST,store_sum)





#impute model with mean by Group number to replace missing elasticity values

imputed_mean_model <- new_data_elasticity %>% group_by(GROUP_NUM) %>% mutate_at("final_elasticity", function(x) replace(x, is.na(x), mean(x,na.rm=TRUE)))



#Code below calculates the average inventory and inventory turn. Will be used to calculate GMROI.


imputed_mean_model <- imputed_mean_model %>% mutate(average_inventory = (COMMITTED_COST_INVENTORY + PHYSICAL_COST_INVENTORY)/3)

#imputed_mean_model$inventory_turn[is.na(imputed_mean_model$inventory_turn)] <- 0


#Establish GMROI formula. For now reference line 229 as formula to use




x_kvi_impute <- imputed_mean_model  %>%
  mutate(weeklygmroi = abs((GM) / average_inventory))


#new gmroi : Annualized GM$(gross margin for the year/ 13 period) / avg inventory()


#COGS = units * cost


##Psuedo code 
#average inventory = (Ending of physical inventory for period 11 + period 12 ending physical inventory) / 2 + (p11 dc + p12 Dc)/2  + (p12 store + p12 store) / 2)

#GM rate / (1-GM Rate)  * Turns (COGS / Average Inventory)

#gmroi = GM % / (1-GM%) * Turns

#annual GMROI

#Create 3 new variables + accounting for NAs(replace NA values with GMROI, Units and salesunits * Elasticity values)

kvi_output <- x_kvi_impute %>%
  mutate(
    salesunits = UNITS,
    units_x_elas = ifelse(!is.na(salesunits) & !is.na(final_elasticity), salesunits * final_elasticity, NA)
  )





#Replace other missing variables with NA for replacement.These NA values will be replaced later in the code


kvi_output[sapply(kvi_output, is.infinite)] <- NA
kvi_output[sapply(kvi_output, is.nan)] <- NA







#Establish weights.
sales_weight <- 0.22
invoicecountproduct_weight <- 0.20
salesunits_weight <- 0.20
weeklygmroi_weight <- 0.21
elasticity_weight <- 0.08
store_weight <- 0.09



#establish pgp averages with imputed model. This dataframe establishes sum of sales variable, average of sales, invoice count product, units, gmroi, elasticity and the weighted elasticity.
x_pgp_averages_impute_mean <- kvi_output  %>%
  group_by(GROUP_NUM) %>%
  summarise(
    avg_sales = round(mean(SALES, na.rm = TRUE),2),
    avg_invctprd = mean(INVOICE_COUNT_PRODUCT, na.rm = TRUE),
    avg_salesunits = mean(salesunits, na.rm = TRUE),
    avg_wklygmroi = mean(weeklygmroi, na.rm = TRUE),
    avg_elasticity = mean(final_elasticity, na.rm = TRUE),
    weighted_elasticity = (sum(units_x_elas,na.rm = TRUE) / sum(salesunits,na.rm = TRUE)),
    avg_store_sum = round(mean(store_sum, na.rm = TRUE),2)
  ) %>% 
  arrange(GROUP_NUM)











#Join kvi_output frame with the pgp averages table.

competitive_score <- kvi_output %>%
  left_join(x_pgp_averages_impute_mean , by = "GROUP_NUM") %>%
  arrange(SKU_CD)




#This dataframe will replace NA values with averages.

competitive_score <- competitive_score %>%
  mutate(
    salesunits = ifelse(is.na(salesunits), avg_salesunits, salesunits),
    weeklygmroi = ifelse(is.na(weeklygmroi), 0, weeklygmroi),
    Elasticity = ifelse(is.na(final_elasticity), avg_elasticity, final_elasticity),
    store_sum = ifelse(is.na(store_sum),avg_store_sum,store_sum))

#Establish a new elasticity classification variable to account for ranking skus with high elasticity but low financials higher than other skus. 

competitive_score <- competitive_score %>% mutate(elasticity_classification = ifelse(abs(Elasticity) >= 0 & abs(Elasticity) < 0.5, 1, ifelse((abs(Elasticity) >= 0.5 & abs(Elasticity) <= 1), 2
                                                                                                                                             , ifelse(abs(Elasticity) > 1 & abs(Elasticity) <= 1.5,3,4))))


#establishing max values for sales, invoice count product, units, gmroi and elasticity

x_pgp_max <- competitive_score %>%
  group_by(GROUP_NUM) %>%
  summarise(
    max_sales = max(SALES, na.rm =TRUE),
    max_invctprd = max(INVOICE_COUNT_PRODUCT, na.rm = TRUE),
    max_salesunits = max(salesunits, na.rm = TRUE),
    max_weeklygmroi = max(weeklygmroi, na.rm = TRUE),
    min_elasticity = min(Elasticity, na.rm = TRUE),
    max_elasticity_classification = max(elasticity_classification, na.rm = TRUE),
    max_store = max(store_sum, na.rm = TRUE)
  ) %>%
  arrange(GROUP_NUM)





##Create x_competitive_score_2 df. This joins the x_pgp_max dataframe with competitive score dataframe.
x_competitive_score_2 <- competitive_score %>%
  left_join(x_pgp_max, by = "GROUP_NUM") %>%
  arrange(SKU_CD)



#Establish itemrankscore variable which will rank the SKUs.

x_competitive_score_2 <- x_competitive_score_2 %>%
  mutate(
    sales_index = SALES / max_sales,
    invoicecount_index = INVOICE_COUNT_PRODUCT / max_invctprd,
    units_index = salesunits / max_salesunits,
    gmroi_index = weeklygmroi / max_weeklygmroi,
    elasticity_index = elasticity_classification / max_elasticity_classification,
    store_index = ifelse(GROUP_DESCRIPTION == 'ENGINES AND TRANSMISSIONS',0,store_sum / max_store),
    itemrankscore = (
      sales_index * sales_weight +
        invoicecount_index * invoicecountproduct_weight +
        units_index * salesunits_weight +
        gmroi_index * weeklygmroi_weight +
        elasticity_index * elasticity_weight + store_index * store_weight
    )
  )



#Sort by descending itemrankscore

x_competitive_score_2 <- x_competitive_score_2 %>%
  arrange(GROUP_NUM, desc(itemrankscore))





## Percentile count variable created for SKUs.

competitive_score <- x_competitive_score_2  %>% arrange(GROUP_NUM, desc(itemrankscore)) %>% group_by(GROUP_NUM) %>% mutate(percentilecount = cumsum(c(1,diff(itemrankscore)!= 0))) %>% ungroup()




#creates variable which is max percentile count by each product

x_pgp_count <- competitive_score %>%
  group_by(GROUP_NUM) %>%
  summarise(
    pgpcount = max(percentilecount)
  )



#joins competitive score and x_pgp_count

x_pgp_count_join <- left_join(competitive_score, x_pgp_count, by = "GROUP_NUM") %>%
  arrange(SKU_CD)




#calculates the percentile ranking for each product
competitive_score<- x_pgp_count_join %>%
  mutate(
    item_percentile = ifelse(percentilecount > 0, 1 - percentilecount / pgpcount, NA)
  )





#DIY role designations


x_role_join <- competitive_score %>% mutate(role = ifelse(GROUP_DESCRIPTION %in% c('BATTERY ACCESSORIES','FLUID MANAGEMENT ACCESSORIES','MOTOR OIL','PERFORMANCE & FUNCTIONAL CHEMICALS') | (GROUP_DESCRIPTION %in% c('BULK CHEMICALS - ANTIFREEZE AND WASHER SOLVENT') & DEPARTMENT_DESCRIPTION %in% c('ANTIFREEZE')),'Traffic', 
                                                          ifelse(GROUP_DESCRIPTION %in% c('AIR FRESHENERS','AIR TOOLS & ACCESSORIES', 'CONSUMABLES','ELECTRONICS','JACKS AND LIFTS','LOANER TOOLS','PROTECTIVE GEAR','SEALANTS, ADHESIVES AND COMPOUNDS','GENERAL MERCH AND BATTERIES','WINTER & SUMMER SEASONAL')| (GROUP_DESCRIPTION %in% c('BULK CHEMICALS - ANTIFREEZE AND WASHER SOLVENT') & DEPARTMENT_DESCRIPTION %in% c('WINDSHIELD WASH')),'Convenience',
                                                                 ifelse(GROUP_DESCRIPTION %in% c('AIR FILTERS','APPEARANCE ACCESSORIES','APPEARANCE CHEMICALS','BATTERIES','BEARINGS, SEALS, HUB ASSEMBLIES','CHASSIS PARTS','HAND & SPECIALTY TOOLS','LIGHTING','OIL FILTERS','RIDE CONTROL','STARTERS & ALTERNATORS','WIPERS','IGNITION/EMISSION - EMISSION SENSORS & VALVES
','IGNITION/EMISSION - IGNITION COMPONENTS','IGNITION/EMISSION - ELECTRICAL COMPONENTS','IGNITION/EMISSION -SPARK PLUGS') | (GROUP_DESCRIPTION %in% c('IGNITION/EMISSION - AIR INJECTION & O2 SENSORS') & DEPARTMENT_DESCRIPTION %in% c('OXYGEN SENSORS'))| (GROUP_DESCRIPTION %in% c('BRAKES - DRUMS AND ROTORS') & CLASS_DESCRIPTION %in% c('ROTORS'))
                                                                        | (GROUP_DESCRIPTION %in% c('BRAKES - FRICTION, PADS & SHOES') & DEPARTMENT_DESCRIPTION %in% c('BRAKE PADS','BRAKES - FRICTION, PADS & SHOES - NGF')),'Destination','Core'))))

#assign role percentiles

final_competitive_score <- x_role_join %>% mutate(item_classification = ifelse(role == 'Destination' & item_percentile <= .50,'Tail',
                                                                               ifelse(role == 'Destination' & item_percentile > .50 & item_percentile < .80,'Core',ifelse(role == 'Destination' & item_percentile >= 0.80,'Head',
                                                                                                                                                                          ifelse(role == 'Core' & item_percentile <= .65,'Tail',
                                                                                                                                                                                 ifelse(role == 'Core' & item_percentile > .65 & item_percentile < .90,'Core',ifelse(role == 'Core' & item_percentile >= .90, 'Head',ifelse(role == 'Convenience' & item_percentile <= .70,'Tail', 
                                                                                                                                                                                                                                                                                                                            ifelse(role == 'Convenience' & item_percentile > .70 & item_percentile < .95, 'Core', ifelse(role =='Convenience' & item_percentile >= .95, 'Head', 
                                                                                                                                                                                                                                                                                                                                                                                                                         ifelse(role == 'Traffic' & item_percentile <= .60, 'Tail', ifelse(role == 'Traffic' & item_percentile > .60 & item_percentile < .85, 'Core', 
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           ifelse(role == 'Traffic' & item_percentile >= 0.85, 'Head','Nothing')))))))))))))

#Mutate lifecycle sku number as character variable

kvi_lifecycle <- kvi_lifecycle %>% mutate(SKU_NUMBER = as.character(SKU_NUMBER))







#Joining lifecycle data
final_join <- final_competitive_score %>% left_join(unique(kvi_lifecycle), by = c("SKU_CD" = "SKU_NUMBER"))





##final_join <- final_join %>% dplyr::select(-WEEK_NUM)

final_join <- arrange(final_join, percentilecount)

final_join$GROUP_DESCRIPTION <- chartr("-"," ",final_join$GROUP_DESCRIPTION)

final_join$GROUP_DESCRIPTION <- chartr("/"," ",final_join$GROUP_DESCRIPTION)

final_join <- final_join %>% rename(SKU = SKU_CD)

#filter out unit cost less than 1.00 and late lifecycle stages 
final_join <- final_join %>% filter(!(UNIT_COST< 1.00 & SKU_EPOCH %in% c('DISPOSITION','BEYOND_DISPOSITION','TO_BE_DISCONTINUED','DISCONTINUED','FDO_POST_PEAK')& item_classification %in% c('Head')))



#CPI join. Do this last 

kvi_CPI <- kvi_CPI %>% mutate(SKU= as.character(SKU))

kvi_CPI <- kvi_CPI %>% dplyr::select(SKU,COMP_NAME,CPI)


final_join <- final_join %>% left_join(unique(kvi_CPI), by = c("SKU" = "SKU"))




#Here is where I check for distinct values in Description for BPGS. Can do this for class,department and subclass

final_join_check <- final_join %>% distinct(GROUP_DESCRIPTION,DEPARTMENT_DESCRIPTION,CLASS_DESCRIPTION)



#View table of percentage + count breakdown of roles and lifecycle




final_join_summary <- final_join %>% group_by(GROUP_DESCRIPTION,role,item_classification) %>% summarise(SALES = sum(SALES), observation_count = n())

final_join_summary <- final_join_summary %>% group_by(GROUP_DESCRIPTION) %>% mutate(total_revenue = sum(SALES))

final_join_summary <- final_join_summary %>% group_by(GROUP_DESCRIPTION) %>% mutate(total_count= sum(observation_count))

final_join_summary <- final_join_summary %>% mutate(percentage_revenue = round((SALES / total_revenue)*100,2), observation_percentage = round((observation_count / total_count)*100,2))


#View table of percentage + count breakdown of roles and lifecycle


final_join_summary_item_importance <- final_join %>% group_by(GROUP_DESCRIPTION,role,item_classification,SKU_EPOCH,SKU_EPOCH_SHARED) %>% summarise(SALES = sum(SALES), observation_count = n())

final_join_summary_item_importance <- final_join_summary_item_importance %>% group_by(GROUP_DESCRIPTION) %>% mutate(total_revenue = sum(SALES))

final_join_summary_item_importance <- final_join_summary_item_importance %>% group_by(GROUP_DESCRIPTION) %>% mutate(total_count= sum(observation_count))

final_join_summary_item_importance <- final_join_summary_item_importance %>% mutate(percentage_revenue = round((SALES / total_revenue)*100,2), observation_percentage = round((observation_count / total_count)*100,2))




#View table of percentage + count breakdown of roles and lifecycle by sku


final_join_summary_item_importance_sku <- final_join %>% group_by(SKU,GROUP_DESCRIPTION,role,item_classification,SKU_EPOCH,SKU_EPOCH_SHARED) %>% summarise(SALES = sum(SALES), observation_count = n())

final_join_summary_item_importance_sku <- final_join_summary_item_importance_sku %>% group_by(GROUP_DESCRIPTION) %>% mutate(total_revenue = sum(SALES))

final_join_summary_item_importance_sku <- final_join_summary_item_importance_sku %>% group_by(GROUP_DESCRIPTION) %>% mutate(total_count= sum(observation_count))

final_join_summary_item_importance_sku <- final_join_summary_item_importance_sku %>% mutate(percentage_revenue = round((SALES / total_revenue)*100,2), observation_percentage = round((observation_count / total_count)*100,2))

#final_join_item_importance_sku <- final_join_summary_item_importance_sku %>% mutate(item_importance_ranking = ifelse(SALES >= (0.95 * total_revenue),'Super_Kvi',ifelse(SALES > (0.85 * total_revenue) & SALES < 0.95 * total_revenue,'KVI',ifelse(SALES > 0.60 * total_revenue & SALES < 0.85 * total_revenue,'non_kvi_1','non_kvi_2'))))


#final_join_summary_CPI

final_join_summary_item_importance_sku_cpi <- final_join %>% group_by(SKU,GROUP_DESCRIPTION,role,item_classification,SKU_EPOCH,SKU_EPOCH_SHARED,CPI,COMP_NAME) %>% summarise(SALES = sum(SALES), observation_count = n())

final_join_summary_item_importance_sku_cpi <- final_join_summary_item_importance_sku_cpi %>% group_by(GROUP_DESCRIPTION) %>% mutate(total_revenue = sum(SALES))

final_join_summary_item_importance_sku_cpi  <- final_join_summary_item_importance_sku_cpi  %>% group_by(GROUP_DESCRIPTION) %>% mutate(total_count= sum(observation_count))

final_join_summary_item_importance_sku_cpi  <-final_join_summary_item_importance_sku_cpi  %>% mutate(percentage_revenue = round((SALES / total_revenue)*100,2), observation_percentage = round((observation_count / total_count)*100,2))




#Code to write out all BPGs info to one file. Two for csv and excel files.

setwd("S:/Retail Pricing/Richard Xiao/KVI/KVI_masterfiles")

write.csv(final_join,"KVI_DIY_masterfile.csv")

write_xlsx(final_join,"KVI_DIY_masterfile.xlsx")

#Use this bottom line to create master file after joining with CPI query

write_xlsx(final_join,"KVI_DIY_CPI_masterfile.xlsx")

#Writes out output to excel files.This is for raw data

setwd("C:/Users/Richard.Xiao/OneDrive - Advance Auto Parts/Documents/Excel_Files/KVI_BPG_DIY_excel_output")

customFun = function(DF) {
  write.xlsx(DF,paste0("DIY_Data_BPG_",unique(DF$GROUP_DESCRIPTION),".xlsx"))
  return(DF)
}

final_join %>%
  group_by(GROUP_DESCRIPTION) %>% do(customFun(.))



#Writes out output to excel files.This is for percentages.


setwd("C:/Users/Richard.Xiao/OneDrive - Advance Auto Parts/Documents/Excel_Files/KVI_BPG_DIY_summary_breakdown")

customFun = function(DF) {
  write.xlsx(DF,paste0("DIY_Summary_BPG_",unique(DF$GROUP_DESCRIPTION),".xlsx"))
  return(DF)
}

final_join_summary %>%
  group_by(GROUP_DESCRIPTION) %>% do(customFun(.))

write.csv(final_join_summary,"KVI_DIY_summary_masterfile.csv")

#summary by item importance

setwd("C:/Users/Richard.Xiao/OneDrive - Advance Auto Parts/Documents/Excel_Files/KVI_item_importance_summary_DIY")

customFun = function(DF) {
  write.xlsx(DF,paste0("KVI_item_importance_Summary_BPG_DIY",unique(DF$GROUP_DESCRIPTION),".xlsx"))
  return(DF)
}

final_join_summary_item_importance %>%
  group_by(GROUP_DESCRIPTION) %>% do(customFun(.))




#Break out summary by sku 

setwd("C:/Users/Richard.Xiao/OneDrive - Advance Auto Parts/Documents/Excel_Files/KVI_item_importance_summary_sku_DIY")

customFun = function(DF) {
  write.xlsx(DF,paste0("KVI_item_importance_Summary_sku_BPG_DIY",unique(DF$GROUP_DESCRIPTION),".xlsx"))
  return(DF)
}

final_join_summary_item_importance_sku %>%
  group_by(GROUP_DESCRIPTION) %>% do(customFun(.))



#Break out summary by sku and CPI

setwd("C:/Users/Richard.Xiao/OneDrive - Advance Auto Parts/Documents/Excel_Files/KVI_sku_DIY_CPI_summary_table")

customFun = function(DF) {
  write.xlsx(DF,paste0("KVI_item_importance_Summary_sku_cpi_BPG_DIY",unique(DF$GROUP_DESCRIPTION),".xlsx"))
  return(DF)
}

final_join_summary_item_importance_sku_cpi %>%
  group_by(GROUP_DESCRIPTION) %>% do(customFun(.))




kvi knn


#Packages
install.packages("psych")
devtools::install_version('DMwR', '0.4.1')
#load in required packages
library(mice)
library(ibmdbR)
library(miceadds)
library(DBI)
library(dbplyr)
library(odbc)
library(readxl)
library(sqldf)
library(rJava)
library(plyr)
library(dplyr)
library(readxl)
library(xlsxjars)
library(MASS)
library(dplyr)
library(shiny)
library(stringr)
library(scales) 
library(sqldf)
require(data.table)
library(Rcpp)
library(readr)
library(splitstackshape)
library(dplyr)
library(tidyr)
library(tidyverse)
library(fs)
library(data.table)
library(RMariaDB)
library(splitstackshape)
library(lubridate)
library(openxlsx)
library(readxl)
library(openxlsx)
library(stringr)
library(writexl)
library(Hmisc)
library(imputeTS)
library(VIM)
library(class)
library(foreach)
library(doParallel)
library(kknn)
library(stats)
library(imputeTS)
library(scales)
library(pivottabler)
library(e1071) 
library(caTools) 
library(factoextra)
library(cluster)
library(NbClust)
library(EnsCat)
library(klAR)
library(broom)
library(psych)

detach("package:rlang", unload = TRUE)


#Set up working directory + connections. Replace line 57 with your working directory.
setwd("C:/Users/Richard.Xiao/OneDrive - Advance Auto Parts/Documents/Excel_Files")


myconn <- DBI::dbConnect(odbc::odbc(), "SNOWFLAKE", uid="AAP_PRCNG_ANLYTCS_PROD_USER", pwd='p2JiJ@e1i86GXD6g',role="AAP_PRCNG_ANLYTCS_PROD_REPORTING_ROLE",warehouse='PRCNG_ANLYTCS_WH', schema = 'analysis')


myconn2 <- DBI::dbConnect(odbc::odbc(), "SNOWFLAKE", uid="AAP_PRCNG_ANLYTCS_PROD_USER", pwd='p2JiJ@e1i86GXD6g',role="AAP_FINERP_EPM_PROD_REPORTING_ROLE",warehouse='FINERP_EPM_WH', schema = 'analysis')



##Start of KVI queries

##Connection 1 for transaction/financial data

kvi_transaction <- DBI::dbGetQuery(myconn,"select distinct SKU_CD,SKU_NM,DESCRIPTION,DEPARTMENT_DESCRIPTION,DEPARTMENT_NUM,GROUP_DESCRIPTION,GROUP_NUM,DIVISION_NUM,DIVISION_DESCRIPTION,CLASS_NUM,CLASS_DESCRIPTION,SUBCLASS_NUM,SUBCLASS_DESCRIPTION,BRAND_DESCRIPTION,sum(COGS_AMT) as NEW_COGS_AMT,sum(PROFIT_AMT) as GM,sum(REVENUE) as SALES,sum(QUANTITY) as UNITS,count(distinct(ORDER_ID)) as invoice_count_product,GM/NULLIFZERO(SALES) as GM_Rate, UNITS * NEW_COGS_AMT as COGS
from AAP_PRCNG_ANLYTCS_PROD_DB.PUBLIC.vw_p_pricing_transactions as sls
left join AAP_PRCNG_ANLYTCS_PROD_DB.PUBLIC.vw_dim_date as dte
    on DATE(dte.FULL_DATE) = sls.ORDER_DT
left join AAP_PRCNG_ANLYTCS_PROD_DB.PUBLIC.vw_p_dim_product as prod
    on prod.SKU = sls.SKU_CD
left join AAP_PRCNG_ANLYTCS_PROD_DB.PUBLIC.vw_sku as skuss
    on skuss.SKU_ID = sls.SKU_CD
where  FISCAL_PERIOD_IN_YEAR in (7,8,9) and FISCAL_YEAR = '2024' and BRAND_DESCRIPTION not in ('WRLPC', 'CORE') and GROUP_DESCRIPTION not in ('ADVANCE CARE SERVICES','COMMERCIAL JACKS/LIFTS','EXTERNAL SUPPLY','NON-SKU MERCHANDISE','OTHER/CORES','SERVICE','STORE SUPPLIES & FIXTURES','UNDEFINED') and CHANNEL_NM = 'DIY'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14")











#Connection  for lifecycle data

kvi_lifecycle <- DBI::dbGetQuery(myconn2,"select AAP_SKU_NUMBER as SKU_NUMBER, SKU_EPOCH,SKU_EPOCH_SHARED
from AAP_DAT_PROD_DB.AVAN.VW_SKU_EPOCH_LATEST")


#Read in store data 

kvi_sku_report <- DBI::dbGetQuery(myconn2,"select AAP_SKU_NUMBER as SKU_NUMBER, COUNT_STORE_ASSORTED,COUNT_HUB_ASSORTED,COUNT_SUPER_HUB_ASSORTED
from aap_dat_prod_db.avan.vw_sku_reporting_base")





#Read in subclass data + data cleansing for excel file. Purpose of this excel file is to get elasticity info based on subclass. Here I did manipulation to join on sub class description.
elasticity_subclass <- read_excel("elasticitiessubclassupdate.xlsx")

elasticity_subclass$BPG <- trimws(elasticity_subclass$BPG)

elasticity_subclass <- elasticity_subclass %>% rename(SUBCLASS = `Color Selection (Hierarchy)`)

elasticity_subclass$SUBCLASS <- trimws(elasticity_subclass$SUBCLASS)
#Concatenate bpg and subclass names in excel file to later join on subclass

elasticity_subclass$BPG_Sub <- str_c(elasticity_subclass$BPG, ' ', elasticity_subclass$SUBCLASS)

#read in class level data for elasticity

elasticity_class <- read_excel("elasticitiesclass.xlsx")

elasticity_class$BPG <- trimws(elasticity_class$BPG)

elasticity_class <- elasticity_class %>% rename(CLASS = `Plot Detail Selection (Hierarchy)`)

elasticity_class$CLASS <- trimws(elasticity_class$CLASS)
#Concatenate bpg and class names in excel file to later join on subclass

elasticity_class$BPG_Class <- str_c(elasticity_class$BPG, ' ', elasticity_class$CLASS)

elasticity_class<- elasticity_class %>% rename(CLASS_ELASTICITY= 'Elasticity')


#read in department level data for elasticity

elasticity_department <- read_excel("elasticitiesdepartment.xlsx")

elasticity_department$BPG <- trimws(elasticity_department$BPG)

elasticity_department <- elasticity_department %>% rename(DEPARTMENT = `Plot Detail Selection (Hierarchy)`)

elasticity_department$DEPARTMENT <- trimws(elasticity_department$DEPARTMENT)



#Concatenate bpg and department names in excel file to later join on subclass

elasticity_department$BPG_Dep <- str_c(elasticity_department$BPG, ' ', elasticity_department$DEPARTMENT)

elasticity_department<- elasticity_department %>% rename(DEPARTMENT_ELASTICITY = 'Elasticity')

#Read in group level elasticity

elasticity_group <- read_excel("elasticity_group.xlsx")

elasticity_group$BPG <- trimws(elasticity_group$BPG)

elasticity_group <- elasticity_group %>% rename(GROUP = `Plot Detail Selection (Hierarchy)`)

elasticity_group$GROUP <- trimws(elasticity_group$GROUP)



#Concatenate bpg and department names in excel file to later join on subclass

elasticity_group$BPG_GROUP <- str_c(elasticity_group$BPG, ' ', elasticity_group$GROUP)

elasticity_group<- elasticity_group %>% rename(GROUP_ELASTICITY = 'Elasticity')



#For sku_data table, change sku id to character variable to join on other dataframe/table



kvi_sku_report <- kvi_sku_report %>% mutate(SKU_NUMBER = as.character(SKU_NUMBER))

kvi_sku_report <- kvi_sku_report %>% mutate(store_sum = rowSums(across(c(COUNT_STORE_ASSORTED,COUNT_HUB_ASSORTED,COUNT_SUPER_HUB_ASSORTED))))

#Joining transaction dataframe with sku_data table together





join_data_first <- kvi_transaction %>% left_join(unique(kvi_sku_report), by = c("SKU_CD" = "SKU_NUMBER"))


#Manipulating BPG and Subclass, class and department variables to prepare to join with excel file

join_data_first$BPG_DESC <- toupper(join_data_first$GROUP_DESCRIPTION)

join_data_first$BPG_DESC <- trimws(join_data_first$GROUP_DESCRIPTION)

join_data_first$SUB_DESC <- toupper(join_data_first$SUBCLASS_DESCRIPTION)

join_data_first$SUB_DESC <- trimws(join_data_first$SUBCLASS_DESCRIPTION)

join_data_first$BPG_SubClass <- str_c(join_data_first$GROUP_DESCRIPTION, ' ', join_data_first$SUBCLASS_DESCRIPTION)

join_data_first$BPG_Classcomb<- str_c(join_data_first$GROUP_DESCRIPTION, ' ', join_data_first$CLASS_DESCRIPTION)

join_data_first$BPG_Depcomb <- str_c(join_data_first$GROUP_DESCRIPTION, ' ', join_data_first$DEPARTMENT_DESCRIPTION)

join_data_first$BPG_Groupcomb <- str_c(join_data_first$GROUP_DESCRIPTION, ' ', join_data_first$GROUP_DESCRIPTION)

#Joining excel file based on subclass,class,group and department level

join_data_elasticity <- join_data_first %>% left_join(elasticity_subclass, by = c("BPG_SubClass" = "BPG_Sub"))

join_data_elasticity<- join_data_elasticity %>% left_join(elasticity_class, by = c("BPG_Classcomb" = "BPG_Class"))

join_data_elasticity<- join_data_elasticity %>% left_join(elasticity_department, by = c("BPG_Depcomb" = "BPG_Dep"))

join_data_elasticity<- join_data_elasticity %>% left_join(elasticity_group, by = c("BPG_Groupcomb" = "BPG_GROUP"))

join_data_elasticity <- transform(join_data_elasticity, new_elasticity = ifelse(is.na(Elasticity),CLASS_ELASTICITY,Elasticity)) 

join_data_elasticity <- transform(join_data_elasticity, final_elasticity = ifelse(is.na(new_elasticity),DEPARTMENT_ELASTICITY,new_elasticity))

join_data_elasticity <- transform(join_data_elasticity, final_elasticity = ifelse(is.na(new_elasticity),GROUP_ELASTICITY,new_elasticity))




#Grab needed variables to prepare the final dataset to use for the rest of code.  

new_data_elasticity <- join_data_elasticity %>% dplyr::select(SKU_CD,SKU_NM,DESCRIPTION,DEPARTMENT_DESCRIPTION,DEPARTMENT_NUM,GROUP_DESCRIPTION,GROUP_NUM,DIVISION_NUM,DIVISION_DESCRIPTION,CLASS_NUM,CLASS_DESCRIPTION,SUBCLASS_NUM,SUBCLASS_DESCRIPTION,BRAND_DESCRIPTION,INVOICE_COUNT_PRODUCT,SALES,UNITS,GM,GM_RATE,final_elasticity
                                                              ,store_sum)

x_min_elasticity <- new_data_elasticity %>% summarise(global_min_elasticity = min(final_elasticity, na.rm = TRUE))

new_data_elasticity <- cbind(new_data_elasticity,x_min_elasticity) 


#Filters out Sales and units info being 0.

new_data_elasticity <- new_data_elasticity %>% filter(SALES != 0 & UNITS != 0) 








#Filtering out unneeded BPGs + unassigned Subclasses + warranty info

new_data_elasticity <- new_data_elasticity %>% filter(!GROUP_DESCRIPTION %in% c('ELECTRICAL','NUTS/BOLTS/MISC HARDWARE','REFERENCE'))

new_data_elasticity <- new_data_elasticity %>% filter(!SUBCLASS_DESCRIPTION %in% c('UNASSIGNED'))


new_data_elasticity <- new_data_elasticity %>% filter(!grepl('WARRANTY',CLASS_DESCRIPTION))


#This dataframe below is now cleansed and includes the inventory_cost variables.

new_data_elasticity <- new_data_elasticity %>% dplyr::select(SKU_CD,SKU_NM,DESCRIPTION,GROUP_DESCRIPTION,DEPARTMENT_DESCRIPTION,DEPARTMENT_NUM,GROUP_NUM,DIVISION_NUM,DIVISION_DESCRIPTION,CLASS_NUM,CLASS_DESCRIPTION,SUBCLASS_NUM,SUBCLASS_DESCRIPTION,BRAND_DESCRIPTION,INVOICE_COUNT_PRODUCT,SALES,UNITS,GM,GM_RATE,final_elasticity,store_sum,global_min_elasticity)





#impute model with mean by Group number to replace missing elasticity values

imputed_mean_model <- new_data_elasticity %>% group_by(GROUP_NUM) %>% mutate_at("final_elasticity", function(x) replace(x, is.na(x), mean(x,na.rm=TRUE)))






x_kvi_impute <- imputed_mean_model




kvi_output <- x_kvi_impute %>%
  mutate(
    salesunits = UNITS,
    units_x_elas = ifelse(!is.na(salesunits) & !is.na(final_elasticity), salesunits * final_elasticity, NA)
  )





#Replace other missing variables with NA for replacement.These NA values will be replaced later in the code


kvi_output[sapply(kvi_output, is.infinite)] <- NA
kvi_output[sapply(kvi_output, is.nan)] <- NA













#establish pgp averages with imputed model. This dataframe establishes sum of sales variable, average of sales, invoice count product, units, gmroi, elasticity and the weighted elasticity.
x_pgp_averages_impute_mean <- kvi_output  %>%
  group_by(GROUP_NUM) %>%
  summarise(
    avg_sales = round(mean(SALES, na.rm = TRUE),2),
    avg_invctprd = mean(INVOICE_COUNT_PRODUCT, na.rm = TRUE),
    avg_salesunits = mean(salesunits, na.rm = TRUE),
    avg_gm = mean(GM, na.rm = TRUE),
    avg_gm_rate = mean(GM_RATE, na.rm = TRUE),
    avg_elasticity = mean(final_elasticity, na.rm = TRUE),
    weighted_elasticity = (sum(units_x_elas,na.rm = TRUE) / sum(salesunits,na.rm = TRUE)),
    avg_store_sum = round(mean(store_sum, na.rm = TRUE),2)
  ) %>% 
  arrange(GROUP_NUM)











#Join kvi_output frame with the pgp averages table.

competitive_score <- kvi_output %>%
  left_join(x_pgp_averages_impute_mean , by = "GROUP_NUM") %>%
  arrange(SKU_CD)




#This dataframe will replace NA values with averages.

competitive_score <- competitive_score %>%
  mutate(
    salesunits = ifelse(is.na(salesunits), avg_salesunits, salesunits),
    GM = ifelse(is.na(GM), 0, GM),
    GM_RATE = ifelse(is.na(GM_RATE), 0, GM_RATE),
    Elasticity = ifelse(is.na(final_elasticity), avg_elasticity, final_elasticity),
    store_sum = ifelse(is.na(store_sum),avg_store_sum,store_sum))



competitive_score <- competitive_score %>% dplyr::select(SKU_CD,SKU_NM,DESCRIPTION,GROUP_DESCRIPTION,DEPARTMENT_DESCRIPTION,DEPARTMENT_NUM,GROUP_NUM,DIVISION_NUM,DIVISION_DESCRIPTION,CLASS_NUM,CLASS_DESCRIPTION,SUBCLASS_NUM,SUBCLASS_DESCRIPTION,BRAND_DESCRIPTION,INVOICE_COUNT_PRODUCT,SALES,UNITS,GM,GM_RATE,Elasticity,store_sum)

competitive_score <- competitive_score %>% mutate(CLASS_NUM = as.character(CLASS_NUM))
#start for class cluster analysis just for units and sales 

competitive_score_test <- competitive_score %>% filter(GROUP_DESCRIPTION== 'BATTERIES')

competitive_score_class <- competitive_score_test %>% filter(CLASS_DESCRIPTION == 'BEST (GOLD)')

competitive_score_class <- competitive_score_class %>% ungroup()

competitive_score_units_sales_class <- competitive_score_class %>% dplyr::select(UNITS,SALES)

competitive_score_sales <- na.omit(competitive_score_test)

fviz_nbclust(competitive_score_units_sales_class, kmeans, method = "wss")

km_units_sales_class<- kmeans(competitive_score_units_sales_class %>% scale(), centers = 3,nstart = 20)

km_units_sales_class

fviz_cluster(km_units_sales_class, data = competitive_score_units_sales_class %>% scale(),geom = "point",ellipse.type = "convex")


competitive_score_class$cluster_id <- factor(km_units_GM$cluster,labels = c("Head","Core","Tail"))


ggplot(competitive_score_class,aes(UNITS,SALES, color = cluster_id)) + geom_point(alpha = 0.25) +
  xlab("Units") +
  ylab("Sales")

summary(competitive_score_class)


#start for class cluster analysis just for units and GM$ 

competitive_score_units_GM <- competitive_score_class %>% dplyr::select(UNITS,GM)

fviz_nbclust(competitive_score_units_GM, kmeans, method = "wss")

km_units_GM<- kmeans(competitive_score_units_GM %>% scale(), centers = 3,nstart = 20)

km_units_GM

fviz_cluster(km_units_GM, data = competitive_score_units_GM %>% scale(),geom = "point",ellipse.type = "convex")


competitive_score_class$cluster_id <- factor(km_units_GM$cluster,labels = c("Head","Core","Tail"))


ggplot(competitive_score_class,aes(UNITS,GM, color = cluster_id)) + geom_point(alpha = 0.25) +
  xlab("Units") +
  ylab("GM")


summary_stats <- competitive_score_class %>%
  group_by(cluster_id) %>%
  summarise(
    avg_Units = round(mean(UNITS, na.rm = TRUE),2),
    avg_GM = round(mean(GM, na.rm = TRUE),2),
    max_Units = max(UNITS, na.rm = TRUE),
    max_gm = max(GM, na.rm = TRUE),
  ) 




#BPG level

#start for class cluster analysis just for units and elasticity 

competitive_score_test <- competitive_score %>% filter(GROUP_DESCRIPTION== 'DORMAN - INNOVATION')


competitive_score_test<- competitive_score_test %>% ungroup()

competitive_score_units_sales <- competitive_score_test %>% dplyr::select(GM_RATE,SALES)


fviz_nbclust(competitive_score_units_sales, kmeans, method = "wss")

km_units_sales<- kmeans(competitive_score_units_sales %>% scale(), centers = 3,nstart = 20)

km_units_sales

fviz_cluster(km_units_sales, data = competitive_score_units_sales %>% scale(),geom = "point",ellipse.type = "convex")


competitive_score_test$cluster_id <- factor(km_units_sales$cluster)


#competitive_score_test$cluster_id <- factor(km_units_sales$cluster,labels = c("Head","Core","Tail"))


ggplot(competitive_score_test,aes(Elasticity,UNITS, color = cluster_id)) + geom_point(alpha = 0.25) +
  xlab("Elasticity") +
  ylab("Units")

summary_stats <- competitive_score_test %>%
  group_by(cluster_id) %>%
  summarise(
    avg_Units = round(mean(UNITS, na.rm = TRUE),2),
    avg_Sales = round(mean(SALES, na.rm = TRUE),2),
    max_Units = max(UNITS, na.rm = TRUE),
    max_Sales = max(SALES, na.rm = TRUE),
  ) 


setwd("S:/Retail Pricing/Richard Xiao/KVI/KVI_masterfiles")



write_xlsx(competitive_score_test,"KVI_DormanInnovation_clustering_unitswithelasticity_analysis.xlsx")



#dimension reduction with PCA

competitive_score_test_pca <- competitive_score_test %>% dplyr::select(INVOICE_COUNT_PRODUCT,SALES,UNITS,GM,GM_RATE,Elasticity,store_sum)

competitive_score_test_pca <- scale(competitive_score_test_pca)

pca_result <- princomp(competitive_score_test_pca)

summary(pca_result)

pca_result

pca_result$loadings[,1:2]

fviz_eig(pca_result, addlabels = TRUE)

fviz_pca_var(pca_result, col.var = "black")

fviz_cos2(pca_result, choice = "var", axes = 1:2)

#

#use k = 3 as optimal 



#kmodes

competitive_score_sales_class <- competitive_score_test %>% dplyr::select(CLASS_DESCRIPTION,SALES)

kmodes_sales <- kmodes(competitive_score_sales_class,3,iter.max=7,weighted = FALSE,fast=TRUE) 

#don't use this knn
competitive_score <- competitive_score %>% as.data.frame()
competitive_score_sales <- competitive_score %>% as.data.frame()

competitive_score_sales <- competitive_score_sales %>% dplyr::select(GROUP_NUM,SALES)



km_sales <- kmeans(competitive_score_sales %>% dplyr::select(-CLASS_NUM) %>% scale(), centers = 3)



split <- sample.split(competitive_score_sales, SplitRatio = 0.8)
train_cl <- subset(competitive_score_sales, split == "TRUE")
test_cl <- subset(competitive_score_sales, split == "FALSE")

train_scale <- scale(train_cl[,1:21])


