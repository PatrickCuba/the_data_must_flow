/* =============================================================================
   DATA VAULT 2.0 QUALITY FRAMEWORK — Part 2: Attach DMFs to DV Tables
   All custom DMFs from 01_create_dq_schema_and_dmfs.sql.
   Expectations are all VALUE = 0 (zero errors = pass).
   Schedule: TRIGGER_ON_CHANGES — fires asynchronously after each DML commit.

   Recon DMFs (DMF_DV_HUB_RECON, DMF_DV_LNK_RECON, DMF_DV_SAT_RECON) are
   library-ready but not attached here — they attach to staging tables which
   are scoped separately.

   DROP FUNCTION syntax note:
     DROP FUNCTION <schema>.<name>(TABLE(col_type, ...));
     (NOT "DROP DATA METRIC FUNCTION")
   DROP attachment note:
     ALTER TABLE <t> DROP DATA METRIC FUNCTION <dmf> ON (<cols>);
     Column order must exactly match the original ADD statement.
============================================================================= */

USE ROLE IDENTIFIER('<% role %>');
USE WAREHOUSE IDENTIFIER('<% warehouse %>');

/* ═══════════════════════════════════════════════════════════════════════════ */
/* HUB_ACCOUNT                                                                 */
/* ═══════════════════════════════════════════════════════════════════════════ */

ALTER TABLE <% edw_database %>.SAL.HUB_ACCOUNT
    ADD DATA METRIC FUNCTION <% database %>.DQ.DV_DMF_HUB_SKEY_DUPE_err
        ON (DV_HASHKEY_HUB_ACCOUNT)
        EXPECTATION hub_account_skey_no_dupes (VALUE = 0);

ALTER TABLE <% edw_database %>.SAL.HUB_ACCOUNT
    ADD DATA METRIC FUNCTION <% database %>.DQ.DV_DMF_HUB_1BKEY_DUPE_err
        ON (DV_TENANT_ID, DV_COLLISIONCODE, ACCOUNT_ID)
        EXPECTATION hub_account_bkey_no_dupes (VALUE = 0);

ALTER TABLE <% edw_database %>.SAL.HUB_ACCOUNT
    SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';

/* ═══════════════════════════════════════════════════════════════════════════ */
/* HUB_PARTY                                                                   */
/* ═══════════════════════════════════════════════════════════════════════════ */

ALTER TABLE <% edw_database %>.SAL.HUB_PARTY
    ADD DATA METRIC FUNCTION <% database %>.DQ.DV_DMF_HUB_SKEY_DUPE_err
        ON (DV_HASHKEY_HUB_PARTY)
        EXPECTATION hub_party_skey_no_dupes (VALUE = 0);

ALTER TABLE <% edw_database %>.SAL.HUB_PARTY
    ADD DATA METRIC FUNCTION <% database %>.DQ.DV_DMF_HUB_1BKEY_DUPE_err
        ON (DV_TENANT_ID, DV_COLLISIONCODE, PARTY_ID)
        EXPECTATION hub_party_bkey_no_dupes (VALUE = 0);

ALTER TABLE <% edw_database %>.SAL.HUB_PARTY
    SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';

/* ═══════════════════════════════════════════════════════════════════════════ */
/* LNK_BV_CARD_ACCOUNT_ASSIGNMENT (5 hub keys)                                 */
/* ═══════════════════════════════════════════════════════════════════════════ */

ALTER TABLE <% edw_database %>.SAL.LNK_BV_CARD_ACCOUNT_ASSIGNMENT
    ADD DATA METRIC FUNCTION <% database %>.DQ.DV_DMF_LNK_SKEY_DUPE_err
        ON (DV_HASHKEY_LNK_BV_CARD_ACCOUNT_ASSIGNMENT)
        EXPECTATION lnk_bvcaa_skey_no_dupes (VALUE = 0);

ALTER TABLE <% edw_database %>.SAL.LNK_BV_CARD_ACCOUNT_ASSIGNMENT
    ADD DATA METRIC FUNCTION <% database %>.DQ.DV_DMF_LNK_5HKEY_DUPE_err
        ON (DV_HASHKEY_HUB_ACCOUNT_ACCOUNT_ID,
            DV_HASHKEY_HUB_ACCOUNT_BC_CONSOL_PRIMARY_ACCT,
            DV_HASHKEY_HUB_ACCOUNT_BC_TRANSFER_ACCOUNT_NO,
            DV_HASHKEY_HUB_ACCOUNT_CARD_ID,
            DV_HASHKEY_HUB_ACCOUNT_CONTROL_CARD_ID)
        EXPECTATION lnk_bvcaa_5hkey_no_dupes (VALUE = 0);

ALTER TABLE <% edw_database %>.SAL.LNK_BV_CARD_ACCOUNT_ASSIGNMENT
    ADD DATA METRIC FUNCTION <% database %>.DQ.DV_DMF_LNK_SKEY_ORPH_ERR
        ON (DV_HASHKEY_HUB_ACCOUNT_CARD_ID,
            TABLE(<% edw_database %>.SAL.HUB_ACCOUNT(DV_HASHKEY_HUB_ACCOUNT)))
        EXPECTATION lnk_bvcaa_card_id_orph (VALUE = 0);
ALTER TABLE <% edw_database %>.SAL.LNK_BV_CARD_ACCOUNT_ASSIGNMENT
    ADD DATA METRIC FUNCTION <% database %>.DQ.DV_DMF_LNK_SKEY_ORPH_ERR
        ON (DV_HASHKEY_HUB_ACCOUNT_BC_TRANSFER_ACCOUNT_NO,
            TABLE(<% edw_database %>.SAL.HUB_ACCOUNT(DV_HASHKEY_HUB_ACCOUNT)))
        EXPECTATION lnk_bvcaa_bc_transfer_orph (VALUE = 0);
ALTER TABLE <% edw_database %>.SAL.LNK_BV_CARD_ACCOUNT_ASSIGNMENT
    ADD DATA METRIC FUNCTION <% database %>.DQ.DV_DMF_LNK_SKEY_ORPH_ERR
        ON (DV_HASHKEY_HUB_ACCOUNT_BC_CONSOL_PRIMARY_ACCT,
            TABLE(<% edw_database %>.SAL.HUB_ACCOUNT(DV_HASHKEY_HUB_ACCOUNT)))
        EXPECTATION lnk_bvcaa_bc_consol_orph (VALUE = 0);
ALTER TABLE <% edw_database %>.SAL.LNK_BV_CARD_ACCOUNT_ASSIGNMENT
    ADD DATA METRIC FUNCTION <% database %>.DQ.DV_DMF_LNK_SKEY_ORPH_ERR
        ON (DV_HASHKEY_HUB_ACCOUNT_ACCOUNT_ID,
            TABLE(<% edw_database %>.SAL.HUB_ACCOUNT(DV_HASHKEY_HUB_ACCOUNT)))
        EXPECTATION lnk_bvcaa_account_id_orph (VALUE = 0);
ALTER TABLE <% edw_database %>.SAL.LNK_BV_CARD_ACCOUNT_ASSIGNMENT
    ADD DATA METRIC FUNCTION <% database %>.DQ.DV_DMF_LNK_SKEY_ORPH_ERR
        ON (DV_HASHKEY_HUB_ACCOUNT_CONTROL_CARD_ID,
            TABLE(<% edw_database %>.SAL.HUB_ACCOUNT(DV_HASHKEY_HUB_ACCOUNT)))
        EXPECTATION lnk_bvcaa_ctrl_card_orph (VALUE = 0);

ALTER TABLE <% edw_database %>.SAL.LNK_BV_CARD_ACCOUNT_ASSIGNMENT
    SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';

/* ═══════════════════════════════════════════════════════════════════════════ */
/* LNK_HY_RV_CARDS_CONTROL_CARD (2 hub keys)                                   */
/* ═══════════════════════════════════════════════════════════════════════════ */

ALTER TABLE <% edw_database %>.SAL.LNK_HY_RV_CARDS_CONTROL_CARD
    ADD DATA METRIC FUNCTION <% database %>.DQ.DV_DMF_LNK_SKEY_DUPE_err
        ON (DV_HASHKEY_LNK_HY_RV_CARDS_CONTROL_CARD)
        EXPECTATION lnk_hycc_skey_no_dupes (VALUE = 0);

ALTER TABLE <% edw_database %>.SAL.LNK_HY_RV_CARDS_CONTROL_CARD
    ADD DATA METRIC FUNCTION <% database %>.DQ.DV_DMF_LNK_2HKEY_DUPE_err
        ON (DV_HASHKEY_HUB_ACCOUNT_CARD, DV_HASHKEY_HUB_ACCOUNT_CONTROL_CARD)
        EXPECTATION lnk_hycc_2hkey_no_dupes (VALUE = 0);

ALTER TABLE <% edw_database %>.SAL.LNK_HY_RV_CARDS_CONTROL_CARD
    ADD DATA METRIC FUNCTION <% database %>.DQ.DV_DMF_LNK_SKEY_ORPH_ERR
        ON (DV_HASHKEY_HUB_ACCOUNT_CARD,
            TABLE(<% edw_database %>.SAL.HUB_ACCOUNT(DV_HASHKEY_HUB_ACCOUNT)))
        EXPECTATION lnk_hyrvcc_card_orph (VALUE = 0);
ALTER TABLE <% edw_database %>.SAL.LNK_HY_RV_CARDS_CONTROL_CARD
    ADD DATA METRIC FUNCTION <% database %>.DQ.DV_DMF_LNK_SKEY_ORPH_ERR
        ON (DV_HASHKEY_HUB_ACCOUNT_CONTROL_CARD,
            TABLE(<% edw_database %>.SAL.HUB_ACCOUNT(DV_HASHKEY_HUB_ACCOUNT)))
        EXPECTATION lnk_hyrvcc_ctrl_card_orph (VALUE = 0);

ALTER TABLE <% edw_database %>.SAL.LNK_HY_RV_CARDS_CONTROL_CARD
    SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';

/* ═══════════════════════════════════════════════════════════════════════════ */
/* LNK_HY_RV_ZOHO_EMPLOYEE_MANAGER (2 hub keys)                                */
/* ═══════════════════════════════════════════════════════════════════════════ */

ALTER TABLE <% edw_database %>.SAL.LNK_HY_RV_ZOHO_EMPLOYEE_MANAGER
    ADD DATA METRIC FUNCTION <% database %>.DQ.DV_DMF_LNK_SKEY_DUPE_err
        ON (DV_HASHKEY_LNK_HY_RV_ZOHO_EMPLOYEE_MANAGER)
        EXPECTATION lnk_hyzem_skey_no_dupes (VALUE = 0);

ALTER TABLE <% edw_database %>.SAL.LNK_HY_RV_ZOHO_EMPLOYEE_MANAGER
    ADD DATA METRIC FUNCTION <% database %>.DQ.DV_DMF_LNK_2HKEY_DUPE_err
        ON (DV_HASHKEY_HUB_PARTY_EMPLOYEE, DV_HASHKEY_HUB_PARTY_MANAGER)
        EXPECTATION lnk_hyzem_2hkey_no_dupes (VALUE = 0);

ALTER TABLE <% edw_database %>.SAL.LNK_HY_RV_ZOHO_EMPLOYEE_MANAGER
    ADD DATA METRIC FUNCTION <% database %>.DQ.DV_DMF_LNK_SKEY_ORPH_ERR
        ON (DV_HASHKEY_HUB_PARTY_EMPLOYEE,
            TABLE(<% edw_database %>.SAL.HUB_PARTY(DV_HASHKEY_HUB_PARTY)))
        EXPECTATION lnk_hyrvzem_employee_orph (VALUE = 0);
ALTER TABLE <% edw_database %>.SAL.LNK_HY_RV_ZOHO_EMPLOYEE_MANAGER
    ADD DATA METRIC FUNCTION <% database %>.DQ.DV_DMF_LNK_SKEY_ORPH_ERR
        ON (DV_HASHKEY_HUB_PARTY_MANAGER,
            TABLE(<% edw_database %>.SAL.HUB_PARTY(DV_HASHKEY_HUB_PARTY)))
        EXPECTATION lnk_hyrvzem_manager_orph (VALUE = 0);

ALTER TABLE <% edw_database %>.SAL.LNK_HY_RV_ZOHO_EMPLOYEE_MANAGER
    SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';

/* ═══════════════════════════════════════════════════════════════════════════ */
/* LNK_NH_RV_ZOHO_EMPLOYEE_ACCOUNT (2 hub keys → HUB_PARTY + HUB_ACCOUNT)     */
/* ═══════════════════════════════════════════════════════════════════════════ */

ALTER TABLE <% edw_database %>.SAL.LNK_NH_RV_ZOHO_EMPLOYEE_ACCOUNT
    ADD DATA METRIC FUNCTION <% database %>.DQ.DV_DMF_LNK_SKEY_ORPH_ERR
        ON (DV_HASHKEY_HUB_PARTY,
            TABLE(<% edw_database %>.SAL.HUB_PARTY(DV_HASHKEY_HUB_PARTY)))
        EXPECTATION lnk_nhrvzea_party_orph (VALUE = 0);
ALTER TABLE <% edw_database %>.SAL.LNK_NH_RV_ZOHO_EMPLOYEE_ACCOUNT
    ADD DATA METRIC FUNCTION <% database %>.DQ.DV_DMF_LNK_SKEY_ORPH_ERR
        ON (DV_HASHKEY_HUB_ACCOUNT,
            TABLE(<% edw_database %>.SAL.HUB_ACCOUNT(DV_HASHKEY_HUB_ACCOUNT)))
        EXPECTATION lnk_nhrvzea_account_orph (VALUE = 0);

ALTER TABLE <% edw_database %>.SAL.LNK_NH_RV_ZOHO_EMPLOYEE_ACCOUNT
    SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';

/* ═══════════════════════════════════════════════════════════════════════════ */
/* LNK_RV_CUSTOMER_ACCOUNT_PRODUCT (3 hub keys)                                */
/* ═══════════════════════════════════════════════════════════════════════════ */

ALTER TABLE <% edw_database %>.SAL.LNK_RV_CUSTOMER_ACCOUNT_PRODUCT
    ADD DATA METRIC FUNCTION <% database %>.DQ.DV_DMF_LNK_SKEY_DUPE_err
        ON (DV_HASHKEY_LNK_RV_CUSTOMER_ACCOUNT_PRODUCT)
        EXPECTATION lnk_cap_skey_no_dupes (VALUE = 0);

ALTER TABLE <% edw_database %>.SAL.LNK_RV_CUSTOMER_ACCOUNT_PRODUCT
    ADD DATA METRIC FUNCTION <% database %>.DQ.DV_DMF_LNK_3HKEY_DUPE_err
        ON (DV_HASHKEY_HUB_ACCOUNT_ACCOUNT,
            DV_HASHKEY_HUB_ACCOUNT_SECONDARY_ACCOUNT,
            DV_HASHKEY_HUB_PARTY)
        EXPECTATION lnk_cap_3hkey_no_dupes (VALUE = 0);

ALTER TABLE <% edw_database %>.SAL.LNK_RV_CUSTOMER_ACCOUNT_PRODUCT
    ADD DATA METRIC FUNCTION <% database %>.DQ.DV_DMF_LNK_SKEY_ORPH_ERR
        ON (DV_HASHKEY_HUB_PARTY,
            TABLE(<% edw_database %>.SAL.HUB_PARTY(DV_HASHKEY_HUB_PARTY)))
        EXPECTATION lnk_rvcap_party_orph (VALUE = 0);

ALTER TABLE <% edw_database %>.SAL.LNK_RV_CUSTOMER_ACCOUNT_PRODUCT
    ADD DATA METRIC FUNCTION <% database %>.DQ.DV_DMF_LNK_SKEY_ORPH_ERR
        ON (DV_HASHKEY_HUB_ACCOUNT_ACCOUNT,
            TABLE(<% edw_database %>.SAL.HUB_ACCOUNT(DV_HASHKEY_HUB_ACCOUNT)))
        EXPECTATION lnk_rvcap_account_orph (VALUE = 0);

ALTER TABLE <% edw_database %>.SAL.LNK_RV_CUSTOMER_ACCOUNT_PRODUCT
    ADD DATA METRIC FUNCTION <% database %>.DQ.DV_DMF_LNK_SKEY_ORPH_ERR
        ON (DV_HASHKEY_HUB_ACCOUNT_SECONDARY_ACCOUNT,
            TABLE(<% edw_database %>.SAL.HUB_ACCOUNT(DV_HASHKEY_HUB_ACCOUNT)))
        EXPECTATION lnk_rvcap_sec_account_orph (VALUE = 0);

ALTER TABLE <% edw_database %>.SAL.LNK_RV_CUSTOMER_ACCOUNT_PRODUCT
    SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';

/* ═══════════════════════════════════════════════════════════════════════════ */
/* LNK_SA_RV_CARDS_TRANSFER_CARDS (2 hub keys)                                 */
/* ═══════════════════════════════════════════════════════════════════════════ */

ALTER TABLE <% edw_database %>.SAL.LNK_SA_RV_CARDS_TRANSFER_CARDS
    ADD DATA METRIC FUNCTION <% database %>.DQ.DV_DMF_LNK_SKEY_DUPE_err
        ON (DV_HASHKEY_LNK_SA_RV_CARDS_TRANSFER_CARDS)
        EXPECTATION lnk_sactc_skey_no_dupes (VALUE = 0);

ALTER TABLE <% edw_database %>.SAL.LNK_SA_RV_CARDS_TRANSFER_CARDS
    ADD DATA METRIC FUNCTION <% database %>.DQ.DV_DMF_LNK_2HKEY_DUPE_err
        ON (DV_HASHKEY_HUB_ACCOUNT_CARD, DV_HASHKEY_HUB_ACCOUNT_TRANSFER_CARD)
        EXPECTATION lnk_sactc_2hkey_no_dupes (VALUE = 0);

ALTER TABLE <% edw_database %>.SAL.LNK_SA_RV_CARDS_TRANSFER_CARDS
    ADD DATA METRIC FUNCTION <% database %>.DQ.DV_DMF_LNK_SKEY_ORPH_ERR
        ON (DV_HASHKEY_HUB_ACCOUNT_CARD,
            TABLE(<% edw_database %>.SAL.HUB_ACCOUNT(DV_HASHKEY_HUB_ACCOUNT)))
        EXPECTATION lnk_sarvctc_card_orph (VALUE = 0);
ALTER TABLE <% edw_database %>.SAL.LNK_SA_RV_CARDS_TRANSFER_CARDS
    ADD DATA METRIC FUNCTION <% database %>.DQ.DV_DMF_LNK_SKEY_ORPH_ERR
        ON (DV_HASHKEY_HUB_ACCOUNT_TRANSFER_CARD,
            TABLE(<% edw_database %>.SAL.HUB_ACCOUNT(DV_HASHKEY_HUB_ACCOUNT)))
        EXPECTATION lnk_sarvctc_transfer_orph (VALUE = 0);

ALTER TABLE <% edw_database %>.SAL.LNK_SA_RV_CARDS_TRANSFER_CARDS
    SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';

/* ═══════════════════════════════════════════════════════════════════════════ */
/* LNK_SA_RV_MAP_MDM_ACCOUNT (2 hub keys)                                      */
/* ═══════════════════════════════════════════════════════════════════════════ */

ALTER TABLE <% edw_database %>.SAL.LNK_SA_RV_MAP_MDM_ACCOUNT
    ADD DATA METRIC FUNCTION <% database %>.DQ.DV_DMF_LNK_SKEY_DUPE_err
        ON (DV_HASHKEY_LNK_SA_RV_MAP_MDM_ACCOUNT)
        EXPECTATION lnk_map_mdm_acct_skey_no_dupes (VALUE = 0);

ALTER TABLE <% edw_database %>.SAL.LNK_SA_RV_MAP_MDM_ACCOUNT
    ADD DATA METRIC FUNCTION <% database %>.DQ.DV_DMF_LNK_2HKEY_DUPE_err
        ON (DV_HASHKEY_HUB_ACCOUNT_SOURCE_ACCOUNT, DV_HASHKEY_HUB_ACCOUNT_TARGET_ACCOUNT)
        EXPECTATION lnk_map_mdm_acct_2hkey_no_dupes (VALUE = 0);

ALTER TABLE <% edw_database %>.SAL.LNK_SA_RV_MAP_MDM_ACCOUNT
    ADD DATA METRIC FUNCTION <% database %>.DQ.DV_DMF_LNK_SKEY_ORPH_ERR
        ON (DV_HASHKEY_HUB_ACCOUNT_SOURCE_ACCOUNT,
            TABLE(<% edw_database %>.SAL.HUB_ACCOUNT(DV_HASHKEY_HUB_ACCOUNT)))
        EXPECTATION lnk_sarvmma_source_orph (VALUE = 0);
ALTER TABLE <% edw_database %>.SAL.LNK_SA_RV_MAP_MDM_ACCOUNT
    ADD DATA METRIC FUNCTION <% database %>.DQ.DV_DMF_LNK_SKEY_ORPH_ERR
        ON (DV_HASHKEY_HUB_ACCOUNT_TARGET_ACCOUNT,
            TABLE(<% edw_database %>.SAL.HUB_ACCOUNT(DV_HASHKEY_HUB_ACCOUNT)))
        EXPECTATION lnk_sarvmma_target_orph (VALUE = 0);

ALTER TABLE <% edw_database %>.SAL.LNK_SA_RV_MAP_MDM_ACCOUNT
    SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';

/* ═══════════════════════════════════════════════════════════════════════════ */
/* SAT_RV_HUB_SAPBW_COMM_CUSTOMER                                              */
/* ═══════════════════════════════════════════════════════════════════════════ */

ALTER TABLE <% edw_database %>.SAL.SAT_RV_HUB_SAPBW_COMM_CUSTOMER
    ADD DATA METRIC FUNCTION <% database %>.DQ.DMF_DV_SAT_DUPE
        ON (DV_HASHKEY_HUB_PARTY, DV_LOAD_TIMESTAMP, DV_TENANT_ID, DV_HASHDIFF)
        EXPECTATION sat_sapbw_comm_customer_no_dupes (VALUE = 0);

ALTER TABLE <% edw_database %>.SAL.SAT_RV_HUB_SAPBW_COMM_CUSTOMER
    ADD DATA METRIC FUNCTION <% database %>.DQ.DV_DMF_SAT_SKEY_ORPH_ERR
        ON (DV_HASHKEY_HUB_PARTY, DV_RECORDSOURCE,
            TABLE(<% edw_database %>.SAL.HUB_PARTY(DV_HASHKEY_HUB_PARTY)))
        EXPECTATION sat_sapbw_comm_customer_no_orphans (VALUE = 0);

ALTER TABLE <% edw_database %>.SAL.SAT_RV_HUB_SAPBW_COMM_CUSTOMER
    SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';

/* ═══════════════════════════════════════════════════════════════════════════ */
/* SAT_ST_RV_HUB_ACCOUNT_SAPBW_CUST_ACCT_MAST (SAL_EXT)                       */
/* ═══════════════════════════════════════════════════════════════════════════ */

ALTER TABLE <% edw_database %>.SAL_EXT.SAT_ST_RV_HUB_ACCOUNT_SAPBW_CUST_ACCT_MAST
    ADD DATA METRIC FUNCTION <% database %>.DQ.DMF_DV_SAT_DUPE
        ON (DV_HASHKEY_HUB_ACCOUNT, DV_LOAD_TIMESTAMP, DV_TENANT_ID, DV_HASHDIFF)
        EXPECTATION sat_ext_sapbw_cust_acct_no_dupes (VALUE = 0);

ALTER TABLE <% edw_database %>.SAL_EXT.SAT_ST_RV_HUB_ACCOUNT_SAPBW_CUST_ACCT_MAST
    ADD DATA METRIC FUNCTION <% database %>.DQ.DV_DMF_SAT_SKEY_ORPH_ERR
        ON (DV_HASHKEY_HUB_ACCOUNT, DV_RECORDSOURCE,
            TABLE(<% edw_database %>.SAL.HUB_ACCOUNT(DV_HASHKEY_HUB_ACCOUNT)))
        EXPECTATION sat_ext_sapbw_cust_acct_no_orphans (VALUE = 0);

ALTER TABLE <% edw_database %>.SAL_EXT.SAT_ST_RV_HUB_ACCOUNT_SAPBW_CUST_ACCT_MAST
    SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';
