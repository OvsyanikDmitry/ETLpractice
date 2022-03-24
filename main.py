#!/usr/bin/python3

import jaydebeapi
import pandas as pd
import glob
import datetime
import os


w_passports = glob.glob('/home/de2tm/DOVS/data/passport_blacklist*.xlsx')
w_terminals = glob.glob('/home/de2tm/DOVS/data/terminals*.xlsx')
w_transactions = glob.glob('/home/de2tm/DOVS/data/transactions*.csv')

df_terminals = pd.read_excel(w_terminals[0],index_col=None,sep=',')

df_transactions = pd.read_csv(w_transactions[0],sep=';', decimal=",")
df_transactions['transaction_date'] = df_transactions['transaction_date'].astype(str)

df_passports = pd.read_excel(w_passports[0],index_col=None,sep=',')
df_passports['date'] = df_passports['date'].astype(str)



conn = jaydebeapi.connect(
'oracle.jdbc.driver.OracleDriver',
'jdbc:oracle:thin:de2tm/balinfundinson@de-oracle.chronosavant.ru:1521/deoracle',
['de2tm', 'balinfundinson'],
'ojdbc8.jar')

curs = conn.cursor()
conn.jconn.setAutoCommit(False)

#DELETE FROM STG
curs.execute("delete from DOVS_STG_PSSPRT_BL")
curs.execute("delete from DOVS_STG_TRANSACTIONS")
curs.execute("delete from DOVS_STG_TERMINALS")
curs.execute("delete from DOVS_STG_CARDS")
curs.execute("delete from DOVS_STG_ACCOUNTS")
curs.execute("delete from DOVS_STG_CLIENTS")

curs.execute("delete from DOVS_STG_DEL_CARDS")
curs.execute("delete from DOVS_STG_DEL_ACCOUNTS")
curs.execute("delete from DOVS_STG_DEL_CLIENTS")
curs.execute("delete from DOVS_STG_DEL_TRMN")


#грузим в stg измененные и новые данные
#DE2TM.DOVS_STG_TERMINALS
curs.executemany("""INSERT into DOVS_STG_TERMINALS
    (TERMINAL_ID,TERMINAL_TYPE,TERMINAL_CITY,TERMINAL_ADDRESS)
    values (?,?,?,?)""", df_terminals.values.tolist())

#DE2TM.DOVS_STG_TRANSACTIONS
curs.executemany("""INSERT into DOVS_STG_TRANSACTIONS (TRANS_ID,TRANS_DATE,AMT,
    CARD_NUM,OPER_TYPE,OPER_RESULT,TERMINAL)
    values
    (?,TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS'),?,?,?,?,?)""", df_transactions.values.tolist())

#DE2TM.DOVS_STG_PASSPORT_BLACKLIST

curs.executemany("""INSERT into DOVS_STG_PSSPRT_BL
                (ENTRY_DT,PASSPORT_NUM)
                VALUES(TO_DATE(?,'YYYY-MM-DD'), ?)""", df_passports.values.tolist())

curs.execute("SELECT last_update FROM DE2TM.DOVS_META_PSSPRT_BL")
passport_dt = curs.fetchall()
df_passports = df_passports.loc[df_passports['date'] > passport_dt[0][0]]


#DE2TM.DOVS_STG_ACCOUNTS
curs.execute("""INSERT INTO DOVS_STG_ACCOUNTS (ACCOUNT_NUM, VALID_TO, CLIENT, CREATE_DT, UPDATE_DT)
                    SELECT
                    ACCOUNT,
                    VALID_TO,
                    CLIENT,
                    CREATE_DT,
                    UPDATE_DT
                FROM BANK.ACCOUNTS
                WHERE COALESCE(CREATE_DT, UPDATE_DT) > (SELECT MAX(LAST_UPDATE) FROM DE2TM.DOVS_META_ACCOUNTS)""")


#DE2TM.DOVS_STG_CARDS
curs.execute("""INSERT INTO DE2TM.DOVS_STG_CARDS (CARD_NUM,ACCOUNT_NUM,CREATE_DT,UPDATE_DT)
                    SELECT
                    CARD_NUM,
                    ACCOUNT,
                    CREATE_DT,
                    UPDATE_DT
                    FROM BANK.CARDS
                    WHERE COALESCE(CREATE_DT, UPDATE_DT) >(SELECT MAX(LAST_UPDATE) FROM DE2TM.DOVS_META_CARDS)""")


#DE2TM.DOVS_STG_CLIENTS
curs.execute("""INSERT INTO DOVS_STG_CLIENTS (CLIENT_ID, LAST_NAME, FIRST_NAME, PATRONYMIC, DATE_OF_BIRTH,
                    PASSPORT_NUM, PASSPORT_VALID_TO, PHONE, CREATE_DT, UPDATE_DT)
                    SELECT
                    CLIENT_ID,
                    LAST_NAME,
                    FIRST_NAME,
                    PATRONYMIC,
                    DATE_OF_BIRTH,
                    PASSPORT_NUM,
                    PASSPORT_VALID_TO,
                    PHONE,
                    CREATE_DT,
                    UPDATE_DT
                        FROM BANK.CLIENTS
                        WHERE COALESCE(CREATE_DT, UPDATE_DT) >(SELECT MAX(LAST_UPDATE) FROM DE2TM.DOVS_META_CLIENTS)""")

#переменная даты
curs.execute("select trans_date from DOVS_STG_TRANSACTIONS")
currdate = curs.fetchone()[0]

curs.execute("SELECT TRANS_DATE - interval '1' day FROM DOVS_STG_TRANSACTIONS")
prev_date = curs.fetchone()[0]


# переносим в архив отработанные файлы
today_dt = datetime.datetime.strptime(currdate, '%Y-%m-%d %H:%M:%S')
format_today = datetime.datetime.strftime(today_dt, '%d%m%Y')

pbl_archive = '/home/de2tm/DOVS/archive/passport_blacklist_{}.xlsx.backup'.format(format_today)
terminals_archive = '/home/de2tm/DOVS/archive/terminals_{}.xlsx.backup'.format(format_today)
transactions_archive = '/home/de2tm/DOVS/archive/transactions_{}.csv.backup'.format(format_today)

os.replace(w_passports[0], pbl_archive)
os.replace(w_terminals[0], terminals_archive)
os.replace(w_transactions[0], transactions_archive)

#обработка делитов
# STG_DEL

curs.execute("""INSERT INTO DOVS_STG_DEL_CLIENTS
SELECT CLIENT_ID FROM BANK.CLIENTS
""")

curs.execute("""INSERT INTO DOVS_STG_DEL_ACCOUNTS
SELECT ACCOUNT FROM BANK.ACCOUNTS
""")

curs.execute("""INSERT INTO DOVS_STG_DEL_CARDS
SELECT CARD_NUM FROM BANK.CARDS
""")

#####################################
#обрабатываем и заливаем данные в DWH
#DOVS_DWH_FACT_PSSPRT_BL
curs.execute("""INSERT INTO DE2TM.DOVS_DWH_FACT_PSSPRT_BL(PASSPORT_NUM, ENTRY_DT)
                SELECT
                    PASSPORT_NUM,
                    ENTRY_DT
                FROM DOVS_STG_PSSPRT_BL""")
#DOVS_DWH_FACT_TRANSACTIONS
curs.execute("""INSERT INTO DE2TM.DOVS_DWH_FACT_TRANSACTIONS(TRANS_ID, TRANS_DATE, CARD_NUM, OPER_TYPE,AMT, OPER_RESULT, TERMINAL)
                    SELECT
                        TRANS_ID, TRANS_DATE, CARD_NUM, OPER_TYPE,
                        AMT, OPER_RESULT, TERMINAL
                    FROM DOVS_STG_TRANSACTIONS""")

#DOVS_DWH_DIM_TRMNS_HIST
curs.execute("""INSERT INTO DE2TM.DOVS_DWH_DIM_TRMNS_HIST (TERMINAL_ID,TERMINAL_TYPE,TERMINAL_CITY,
                TERMINAL_ADDRESS,EFFECTIVE_FROM_DT,EFFECTIVE_TO_DT,DELETED_FLG)
                SELECT st.TERMINAL_ID,st.TERMINAL_TYPE,st.TERMINAL_CITY,st.TERMINAL_ADDRESS,
                TO_DATE('{}', 'YYYY-MM-DD HH24:MI:SS') + INTERVAL '1' DAY,
                TO_DATE('5999-12-01', 'YYYY-MM-DD'), 0
                FROM DE2TM.DOVS_DWH_DIM_TRMNS_HIST dt
                FULL JOIN DOVS_STG_TERMINALS st
                ON dt.TERMINAL_ID = st.TERMINAL_ID
                WHERE dt.TERMINAL_ID IS NULL""".format(currdate))


#DE2TM.DOVS_DWH_DIM_CARDS_HIST
curs.execute("""insert into DE2TM.DOVS_DWH_DIM_CARDS_HIST(CARD_NUM, ACCOUNT_NUM, EFFECTIVE_FROM_DT)
                    select
                    CARD_NUM, ACCOUNT_NUM, coalesce(UPDATE_DT,CREATE_DT)
                    from DE2TM.DOVS_STG_CARDS""")


curs.execute("""merge into DE2TM.DOVS_DWH_DIM_CARDS_HIST dwh
                    using DE2TM.DOVS_STG_CARDS stg
                    on (dwh.CARD_NUM = stg.CARD_NUM
                    and dwh.EFFECTIVE_FROM_DT < coalesce(stg.UPDATE_DT,stg.CREATE_DT))
                    when matched then update set
                    dwh.EFFECTIVE_TO_DT = coalesce(stg.UPDATE_DT,stg.CREATE_DT) - 1
                    where dwh.EFFECTIVE_TO_DT = TO_DATE('5999-12-31', 'YYYY-MM-DD')""")

#DOVS_DWH_DIM_ACC_HIST
curs.execute("""INSERT INTO DE2TM.DOVS_DWH_DIM_ACC_HIST( ACCOUNT_NUM, VALID_TO, CLIENT, EFFECTIVE_FROM_DT)
            SELECT
                ACCOUNT_NUM,
                VALID_TO,
                CLIENT,
                coalesce(UPDATE_DT,CREATE_DT)
                FROM DE2TM.DOVS_STG_ACCOUNTS""")

curs.execute("""merge into DE2TM.DOVS_DWH_DIM_ACC_HIST dwh
                    using DE2TM.DOVS_STG_ACCOUNTS stg
                    on (dwh.ACCOUNT_NUM = stg.ACCOUNT_NUM
                    and dwh.EFFECTIVE_FROM_DT < coalesce(stg.UPDATE_DT,stg.CREATE_DT))
                    when matched then update set
                    dwh.EFFECTIVE_TO_DT = coalesce(stg.UPDATE_DT,stg.CREATE_DT) - 1
                    where dwh.EFFECTIVE_TO_DT = TO_DATE('5999-12-31', 'YYYY-MM-DD')""")

#DE2TM.DOVS_DWH_DIM_CLNT_HIST
curs.execute("""insert into DE2TM.DOVS_DWH_DIM_CLNT_HIST(CLIENT_ID, LAST_NAME,
                FIRST_NAME,PATRONYMIC,DATE_OF_BIRTH, PASSPORT_NUM, PASSPORT_VALID_TO, PHONE, EFFECTIVE_FROM_DT)
                select
                    CLIENT_ID, LAST_NAME, FIRST_NAME, PATRONYMIC,
                    DATE_OF_BIRTH, PASSPORT_NUM, PASSPORT_VALID_TO, PHONE, coalesce(UPDATE_DT,CREATE_DT)
                    from DE2TM.DOVS_STG_CLIENTS""")

curs.execute("""merge into DE2TM.DOVS_DWH_DIM_CLNT_HIST dwh
                    using DE2TM.DOVS_STG_CLIENTS stg
                    on (dwh.CLIENT_ID = stg.CLIENT_ID
                    and dwh.EFFECTIVE_FROM_DT < coalesce(stg.UPDATE_DT,stg.CREATE_DT))
                    when matched then update set
                    dwh.EFFECTIVE_TO_DT = coalesce(stg.UPDATE_DT,stg.CREATE_DT) - 1
                    where dwh.EFFECTIVE_TO_DT = TO_DATE('5999-12-31', 'YYYY-MM-DD')""")






# вставка новой версии удалений

curs.execute("""insert into DE2TM.DOVS_DWH_DIM_CARDS_HIST
            (CARD_NUM, ACCOUNT_NUM, EFFECTIVE_FROM_DT, DELETED_FLG)
            select
            dwh.CARD_NUM,
            dwh.ACCOUNT_NUM,
            TO_DATE ('{}', 'YYYY-MM-DD HH24-MI-SS'),
            1
            from DE2TM.DOVS_DWH_DIM_CARDS_HIST dwh
            left join
            DE2TM.DOVS_STG_DEL_CARDS stg
            on dwh.CARD_NUM = stg.CARD_NUM
            where stg.CARD_NUM is null
            and dwh.EFFECTIVE_TO_DT = TO_DATE('5999-12-31', 'YYYY-MM-DD')
            and dwh.DELETED_FLG = '0' """.format(currdate))

curs.execute("""insert into DE2TM.DOVS_DWH_DIM_TRMNS_HIST (TERMINAL_ID, TERMINAL_TYPE, TERMINAL_CITY,
                TERMINAL_ADDRESS, EFFECTIVE_FROM_DT,DELETED_FLG)
                select dwh.TERMINAL_ID,
                       dwh.TERMINAL_TYPE,
                       dwh.TERMINAL_CITY,
                       dwh.TERMINAL_ADDRESS,
                       TO_DATE ('{}', 'YYYY-MM-DD HH24-MI-SS'),
                       1
                from DE2TM.DOVS_DWH_DIM_TRMNS_HIST dwh
                full join DE2TM.DOVS_STG_DEL_TRMN stg
                on dwh.terminal_id = stg.terminal_id
                where stg.terminal_id is null
                and dwh.EFFECTIVE_TO_DT = TO_DATE('5999-12-31', 'YYYY-MM-DD')
                and dwh.DELETED_FLG = '0' """.format(currdate))

curs.execute("""insert into DE2TM.DOVS_DWH_DIM_ACC_HIST (ACCOUNT_NUM, VALID_TO, CLIENT,
            EFFECTIVE_FROM_DT, DELETED_FLG)
            select
                dwh.ACCOUNT_NUM,
                dwh.VALID_TO,
                dwh.CLIENT,
                TO_DATE ('{}', 'YYYY-MM-DD HH24-MI-SS'),
                1
            from DE2TM.DOVS_DWH_DIM_ACC_HIST dwh
            left join DE2TM.DOVS_STG_DEL_ACCOUNTS stg
            on dwh.ACCOUNT_NUM = stg.ACCOUNT_NUM
            where stg.ACCOUNT_NUM is null
            and dwh.EFFECTIVE_TO_DT = TO_DATE('5999-12-31', 'YYYY-MM-DD')
            and dwh.DELETED_FLG = '0' """.format(currdate))


curs.execute("""insert into DE2TM.DOVS_DWH_DIM_CLNT_HIST (CLIENT_ID, LAST_NAME, FIRST_NAME, PATRONYMIC,
            DATE_OF_BIRTH, PASSPORT_NUM, PASSPORT_VALID_TO,
            PHONE, EFFECTIVE_FROM_DT, DELETED_FLG)
            select
                dwh.CLIENT_ID,
                dwh.LAST_NAME,
                dwh.FIRST_NAME,
                dwh.PATRONYMIC,
                dwh.DATE_OF_BIRTH,
                dwh.PASSPORT_NUM,
                dwh.PASSPORT_VALID_TO,
                dwh.PHONE,
                TO_DATE ('{}', 'YYYY-MM-DD HH24-MI-SS'),
                1
                from DE2TM.DOVS_DWH_DIM_CLNT_HIST dwh
                left join DOVS_STG_DEL_CLIENTS stg
                on dwh.CLIENT_ID = stg.CLIENT_ID
                where stg.CLIENT_ID is null
                and dwh.EFFECTIVE_TO_DT = TO_DATE('5999-12-31', 'YYYY-MM-DD')
                and dwh.DELETED_FLG = '0' """.format(currdate))




# закрытие старой версии удалений

curs.execute("""update DE2TM.DOVS_DWH_DIM_CARDS_HIST
            set EFFECTIVE_TO_DT = TO_DATE ('{}', 'YYYY-MM-DD HH24-MI-SS')
            where CARD_NUM in (
                        select
                        dwh.CARD_NUM
                        from DE2TM.DOVS_DWH_DIM_CARDS_HIST dwh
                        left join DE2TM.DOVS_STG_DEL_CARDS stg
                        on dwh.CARD_NUM = stg.CARD_NUM
                        where
                        stg.CARD_NUM is null
                        and dwh.EFFECTIVE_TO_DT = TO_DATE('5999-12-31', 'YYYY-MM-DD')
                        and dwh.DELETED_FLG = '0' )""".format(prev_date))


curs.execute("""update DE2TM.DOVS_DWH_DIM_ACC_HIST
            set EFFECTIVE_TO_DT = TO_DATE ('{}', 'YYYY-MM-DD HH24-MI-SS')
            where ACCOUNT_NUM in (
                        select
                            dwh.ACCOUNT_NUM
                        from DE2TM.DOVS_DWH_DIM_ACC_HIST dwh
                        left join DE2TM.DOVS_STG_DEL_ACCOUNTS stg
                        on dwh.ACCOUNT_NUM = stg.ACCOUNT_NUM
                        where stg.ACCOUNT_NUM is null
                        and dwh.EFFECTIVE_TO_DT = TO_DATE('5999-12-31', 'YYYY-MM-DD')
                        and dwh.DELETED_FLG = '0')""".format(prev_date))


curs.execute("""update DE2TM.DOVS_DWH_DIM_CLNT_HIST
            set EFFECTIVE_TO_DT = TO_DATE ('{}', 'YYYY-MM-DD HH24-MI-SS')
            where CLIENT_ID in (
                            select
                            dwh.CLIENT_ID
                            from DE2TM.DOVS_DWH_DIM_CLNT_HIST dwh
                            left join DE2TM.DOVS_STG_CLIENTS stg
                            on dwh.CLIENT_ID = stg.CLIENT_ID
                            where stg.CLIENT_ID is null
                            and dwh.EFFECTIVE_TO_DT = TO_DATE('5999-12-31', 'YYYY-MM-DD')
                            and dwh.DELETED_FLG = '0')""".format(prev_date))


curs.execute("""update DE2TM.DOVS_DWH_DIM_TRMNS_HIST
            set EFFECTIVE_TO_DT = TO_DATE ('{}', 'YYYY-MM-DD HH24-MI-SS')
            where TERMINAL_ID in (
                        select dwh.TERMINAL_ID
                            from DE2TM.DOVS_DWH_DIM_TRMNS_HIST dwh
                            full join DE2TM.DOVS_STG_DEL_TRMN stg
                            on dwh.TERMINAL_ID = stg.TERMINAL_ID
                            where stg.TERMINAL_ID is null
                            and dwh.EFFECTIVE_TO_DT = TO_DATE('5999-12-31', 'YYYY-MM-DD')
                            and dwh.DELETED_FLG = '0')""".format(prev_date))
# обновление метадаты

curs.execute("""UPDATE DE2TM.DOVS_META_PSSPRT_BL
                SET
                LAST_UPDATE = (select max(ENTRY_DT)
                FROM DE2TM.DOVS_STG_PSSPRT_BL)
                WHERE (select max(ENTRY_DT)
                FROM DE2TM.DOVS_STG_PSSPRT_BL) is not NULL""")

curs.execute("""UPDATE DE2TM.DOVS_META_TRANSACTIONS
                SET
                LAST_UPDATE = (select max(TRANS_DATE)
                FROM DE2TM.DOVS_STG_TRANSACTIONS)
                WHERE
                (select max(TRANS_DATE)
                FROM DE2TM.DOVS_STG_TRANSACTIONS) is not NULL""")

curs.execute("""UPDATE DE2TM.DOVS_META_CARDS
                SET
                LAST_UPDATE = (SELECT MAX(COALESCE(UPDATE_DT, CREATE_DT))
                FROM DE2TM.DOVS_STG_CARDS)
                WHERE
                (SELECT MAX(COALESCE(UPDATE_DT, CREATE_DT))
                FROM DE2TM.DOVS_STG_CARDS) IS NOT NULL""")

curs.execute("""UPDATE DE2TM.DOVS_META_ACCOUNTS
                SET
                LAST_UPDATE = (SELECT MAX(COALESCE(UPDATE_DT, CREATE_DT)) FROM DE2TM.DOVS_STG_ACCOUNTS)
                WHERE (SELECT MAX(COALESCE(UPDATE_DT, CREATE_DT))
                FROM DE2TM.DOVS_STG_ACCOUNTS) IS NOT NULL""")

curs.execute("""UPDATE DE2TM.DOVS_META_CLIENTS
                SET
                LAST_UPDATE = (SELECT MAX(COALESCE(UPDATE_DT, CREATE_DT)) FROM DE2TM.DOVS_STG_CLIENTS)
                WHERE (SELECT MAX(COALESCE(UPDATE_DT, CREATE_DT))
                FROM DE2TM.DOVS_STG_CLIENTS) IS NOT NULL""")

conn.commit()

#report

curs.execute("""INSERT INTO DE2TM.DOVS_REP_FRAUD (event_dt, passport, fio, phone, event_type, report_dt)
WITH EVENT AS
	(
	SELECT
		trans_date,
		passport_num,
		last_name||' '||first_name||' '||patronymic,
		phone,
		CASE
			WHEN psbl_passport_num IS NOT NULL
			OR passport_valid_to < TO_DATE ('{}', 'YYYY-MM-DD HH24-MI-SS')
			THEN 'Совершение операции при просроченном или заблокированном паспорте'
            WHEN valid_to IS NULL
			OR valid_to < TO_DATE ('{}', 'YYYY-MM-DD HH24-MI-SS')
			THEN 'Совершение операции при недействующем договоре'
            WHEN next_terminal_city IS NOT NULL
			AND terminal_city!= next_terminal_city
			AND (next_trans_date-trans_date)<INTERVAL '1' HOUR
			THEN 'Совершение операций в разных городах в течение одного часа'
            WHEN oper_result='SUCCESS'
			         AND prev_oper_result='REJECT'
			         AND before_prev_result='REJECT'
				     AND prev_amt BETWEEN amt AND before_prev_amt
				     AND (trans_date-before_prev_date) < INTERVAL '20' MINUTE
			THEN 'Попытка подборка суммы'
		END EVENT_TIP,
		CAST(TO_TIMESTAMP(trans_date) as DATE)+INTERVAL '1' DAY
	FROM
		(

		SELECT
		trans.trans_id,trans.trans_date,trans.amt,trans.oper_result,
        trans.card_num,term.terminal_id,term.terminal_city,
        ac.account_num, ac.valid_to,cl.client_id, cl.last_name,cl.first_name,cl.patronymic,
        cl.passport_num,cl.passport_valid_to,cl.phone,
            pbl.entry_dt as psbl_entry_dt,pbl.passport_num as psbl_passport_num,
			LEAD (term.terminal_city) OVER (PARTITION BY cl.client_id ORDER BY trans.trans_date) AS next_terminal_city,
			LEAD (trans.trans_date) OVER (PARTITION BY cl.client_id ORDER BY trans.trans_date) AS next_trans_date,
			LAG (trans.trans_date,2) OVER (PARTITION BY cl.client_id ORDER BY trans.trans_date) AS before_prev_date,
			LAG (trans.oper_result) OVER (PARTITION BY cl.client_id ORDER BY trans.trans_date) AS prev_oper_result,
			LAG (trans.oper_result,2) OVER (PARTITION BY cl.client_id ORDER BY trans.trans_date) AS before_prev_result,
			LAG (trans.amt) OVER (PARTITION BY cl.client_id ORDER BY trans.trans_date) AS prev_amt,
			LAG (trans.amt,2) OVER (PARTITION BY cl.client_id ORDER BY trans.trans_date) AS before_prev_amt
		FROM  DE2TM.DOVS_DWH_FACT_TRANSACTIONS   trans
            LEFT JOIN DE2TM.DOVS_DWH_DIM_CARDS_HIST  ca
            ON trim(ca.card_num)=trim(trans.card_num)
            LEFT JOIN DE2TM.DOVS_DWH_DIM_TRMNS_HIST term
            ON term.terminal_id=trans.terminal
            LEFT JOIN DE2TM.DOVS_DWH_DIM_ACC_HIST  ac
            ON ca.account_num=ac.account_num
            LEFT JOIN DE2TM.DOVS_DWH_DIM_CLNT_HIST cl
            ON cl.client_id=ac.client
            LEFT JOIN DE2TM.DOVS_DWH_FACT_PSSPRT_BL pbl
            ON pbl.passport_num=cl.passport_num
		)
	)
SELECT * FROM EVENT WHERE EVENT_TIP IS NOT NULL

""".format(currdate, currdate))
#REPORT to xlsx
DOVS_REP_FRAUD=pd.read_sql("""SELECT * FROM DE2TM.DOVS_REP_FRAUD""",conn)

DOVS_REP_FRAUD.to_excel('/home/de2tm/DOVS/dovs_rep_fraud.xlsx',header=True,index=False)


conn.commit()
conn.close()
