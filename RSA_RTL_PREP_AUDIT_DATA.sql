create or replace PROCEDURE RSA_RTL_PREP_AUDIT_DATA
(
    irtlrsaseq  IN  INT
    )
    IS
        
-----------------------------------------------------------------------------------------------------------
--  EXECUTE RTL_RSA_PREP_AUDIT_DATA(get_rtl_seq_current_fnc);
--  Module  :   
--  Procedure:  RTL_RSA_PREP_AUDIT_DATA
--  Purpose :   
--              
--              
--
--   DATE         MODIFIED BY     NOTES
--  11/10/2013    Dwashick        Initial
--  11/18/2013    Dwashick        added logic to verify if an RSA species is the 
--                                target species relative to the RSA Program Code 
--                                used to report the spp catch
--  11/26/2013    Dwashick        Functions to determine Northeast Regional Fishery Management Plans were
--                                modified and required additional parameters added for vessel permit number.
--  04/27/2014    Dwashick        Added new table insert for test_rsa_vms
--  06/27/2014    Dwashick        Added new condition to AMS select for the activity code 13th character = R for RSA trips
--  07/09/2014    Ngouw           Added project_status_valid, prev_rsa_ivr_projectid, date_rsa_started, date_rsa_ended into rsa_rtl_dealer
--  07/09/2014    Ngouw           Added sailing record into rsa_rtl_vms
--  09/25/2014    Dwashick        Renamed procedure from TEST_RSA_INSERT to RTL_RSA_PREP_AUDIT_DATA
--  11/17/2014    Dwashick        Added vms_apps.GET_VMS_RSA_SFD_PROGRAM_FNC(project_code) for rsa program in vms start hails
--  01/21/2015    Dwashick        Added code to delete vessel permit number in 111111 and 888888 from IVR data per Alison Ferguson.
--  01/27/2015    Dwashick        Added new matching columns docid-permit-spp, vtr-permit-spp using both nespp3 and species_itis
--  02/02/2015    Dwashick        Added new field and code for RTL_TRIP_ID
--  02/08/2015    Dwashick        Added new matching columns that match only on the first three numbers of a species itis value
--                                                this is a method to address the need to roll-up common species such as skates
--                                               and prevent false orphans
--  02/13/2015    Dwashick       Changed parameter for GET_RSA_VALID_PROJECT_FNC in IVR data 
--                                                to use NVL(rsa_date_trip_ended,rsa_date_trip_started) versus rsa_date_de
--                                                which created false positive as some trips were entered a year after landing
--  03/02/2015    Dwashick         Added new condition to dealer load - selecting record when vtr in vms or rsa
--  03/13/2015    Dwashick        Added additional condition to IVR and VMS data to exclude using VTR=0 when assigning rtl_trip_id
--                                               Added species itis column to IVR and Vessel
--  04/01/2015    Dwashick        Added line to omit legacy data when pulling in IVR data.
--  09/12/2015    Dwashick        Added procedure to be executed to update the research_code table in fso_admin.
--                                               Changed reference in SQL from averry.research_code to fso_admin.research_code.
--   07/08/2016   Dwashick        Changed irtlseq to irtlrsaseq
--   03/04/2017   Dwashick        Added herring project code to AMS insert
--   06/23/2023   Ilya Taystlin   Excluded from the INSERT records where VESSEL_ID is NULL. Also fixed vprocedurename.
-----------------------------------------------------------------------------------------------------------
    VBATCHPROCESS                  VARCHAR2 (150)  := 'RTL RSA';
    VMODULENAME                    VARCHAR2 (150)  := 'RSA AUDIT';
    vprocedurename                 VARCHAR2 (255)  := 'RSA_RTL_PREP_AUDIT_DATA';
    vcronjob                       VARCHAR2 (100)  := NULL;
    vtablename                     VARCHAR2 (50)   := NULL;	    
    ilogid                         INT             := 0;
    errmsg                         VARCHAR2 (2000);
    vsql                           VARCHAR2 (4000);
    v_maxVTR                       NUMBER          ;
    v_minVTR                       NUMBER          ;
    v_FYStart                      DATE           ;
    v_cutoff_date                  DATE            ;
    v_startMonth                   VARCHAR2(2)     ;
    v_DiscardDate                  DATE           ;
    v_fishingYear                  VARCHAR2(4)     ;
    v_out_dup_t_pc_mri_permit_cnt  NUMBER :=0;
    v_out_dup_t_pc_mri_permit_msg  VARCHAR2(1000) := NULL;
    v_check_before_cnt             NUMBER :=0;
    v_check_after_cnt               NUMBER :=0;
    v_sector_year                  NUMBER := 2012;
 BEGIN
 FSO_ADMIN.log_event (vbatchprocess, vmodulename, vprocedurename, irtlrsaseq, NULL, vprocedurename ||' -- currently executing',NULL,NULL,NULL,NULL,ilogid);
 -- log_event (vbatchprocess, vmodulename, vprocedurename, irtlrsaseq, NULL, vprocedurename ||' -- currently executing',NULL,NULL,NULL,NULL,ilogid);

-- Run these procedures to populate support tables
      AS_PREPARE_SUPPORT_DATA_PRC1(irtlrsaseq);
      FSO_ADMIN.RSA_SUPPORT_TABLE_UPDATES_PRC(irtlrsaseq);
  -- The procedure above populates table needed by    GET_PERMIT_STATUS_FNC(vessel_permit_number,landing_date);
 -- Clean out table from previous load.
 -- LINE 75 uncomment
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RSA_RTL_AMS';
 -- 
 -- uncomment line 80
		INSERT  /*+ APPEND */ INTO rsa_rtl_ams
				( rtl_seq_no
					--,rtl_as_rec_id
					,date_downloaded
					,source_data
					,docid
					,das_id
					,trip_id
					,trip_source
					,permit_nbr
					,activity_code
          ,rsa_program
					,date_sail
					,date_land
					,fy_jan_dec
					,fy_mar_feb
					,fy_may_apr
					,FY_NOV_OCT	
          ,source_level1_key_id
          ,source_level2_key_id
          ,source_level3_key_id
          ,date_time_sail
          ,DATE_TIME_LAND
          ,RTL_TRIP_ID
          ,rtl_trip_id_source
					)
		SELECT 		irtlrsaseq AS rtl_seq_no
				,TO_CHAR(SYSDATE, 'DD-MON-YY') AS date_downloaded 
				,'AMS'
				,get_docid_for_ams_trip_fnc(permit_nbr,(date_sail + ((date_land - date_sail) / 2)))  AS docid
				,das_id
				,trip_id
				,trip_source
				,permit_nbr
				,activity_code
        ,DECODE(substr(activity_code,1,3),'SES', 'SCALLOP'
                                         ,'MNK', 'MONKFISH'
                                         ,'MID', 'MID-ATLANTIC'
                                        , 'HER', 'HERRING'
                                         ,NULL
                                         )
                                         AS rsa_program
				,date_sail
				,date_land
				,GET_FISHING_YEAR_FNC(date_land,'JAN TO DEC') AS fy_jan_dec
				,GET_FISHING_YEAR_FNC(date_land,'MAR TO FEB') AS fy_mar_feb
				,GET_FISHING_YEAR_FNC(date_land,'MAY TO APR') AS fy_may_apr
				,get_fishing_year_fnc(date_land,'NOV TO OCT') as fy_nov_oct	
        ,trip_id AS  source_level1_key_id
        ,null as source_level2_key_id
        ,NULL AS source_level3_key_id
        ,TO_CHAR(date_sail,'DD-MON-YY HH24:MI:SS') AS date_time_sail
        ,TO_CHAR(DATE_LAND,'DD-MON-YY HH24:MI:SS') as DATE_TIME_LAND
        ,case when (get_docid_for_ams_trip_fnc(permit_nbr,(date_sail + ((date_land - date_sail) / 2))) is not null) then get_docid_for_ams_trip_fnc(permit_nbr,(date_sail + ((date_land - date_sail) / 2)))
               when (trip_source = 'IVR' and (get_rsa_vtr_for_ams_fnc(trip_id) is not null)) then get_rsa_vtr_for_ams_fnc(trip_id)
              else TRIP_ID
         end RTL_TRIP_ID
         ,case when (get_docid_for_ams_trip_fnc(permit_nbr,(date_sail + ((date_land - date_sail) / 2))) is not null) then 'DOCID'
               when (trip_source = 'IVR' and (get_rsa_vtr_for_ams_fnc(trip_id) is not null)) then 'VTRSERNO'
          else 'AMS-TRIP_ID-'||trip_source
         END rtl_trip_id_source
				FROM ams.trip
			where  (substr(activity_code,5,3)= 'RSA'
			AND TO_DATE(date_land) >= TO_DATE(SYSDATE - 730))
       or (substr(activity_code,13,1)= 'R'
			AND TO_DATE(date_land) >= TO_DATE(SYSDATE - 730));
		COMMIT;
-------------------------------------
--  VIEW TO LIST SET OF RSA TRIPS REQUIRED TO PULL DEALER DATA
			/* CREATE or replace view VW_RSA_TRIP_DOCIDS
			AS SELECT DOCID
			,'VESSEL' AS source
			FROM NOAA.DOCUMENT WHERE TRIPCATG=4
			MINUS
			SELECT DOCID
			,'AMS_RSA' AS SOURCE
			FROM test_rsa_ams;  */
      
     /*    CREATE OR REPLACE VIEW vw_test_rsa_program_spp
        AS
        SELECT DISTINCT rsa_spp
        ,nespp3
        ,rsa_program
        FROM ref_ivr_species_nespp3
        WHERE rsa_spp = 1
        ORDER BY rsa_program
        ,nespp3;     */
------------------------------------------------
 /*   CREATE OR REPLACE VIEW VW_RSA_DOCIDS
    AS 
    SELECT TRIP_ID
    , DOCID 
    ,0 AS vtrserno
    FROM TEST_RSA_AMS
    UNION ALL
    SELECT DISTINCT RSA_TRIPID AS TRIP_ID
    ,GET_DOCID_FNC(RSA_VTRNBR) AS DOCID
    ,RSA_VTRNBR AS VTRSERNO
    FROM  ivr_apps.rsa_trip_stg;*/
-----------------------------------------

 -- Clean out table from previous load.
   EXECUTE IMMEDIATE 'TRUNCATE TABLE RSA_RTL_DEALER';
 -- 
 -- uncomment line 182 and 186
	INSERT  /*+ APPEND */ INTO rsa_rtl_dealer
				( rtl_seq_no
				--,rtl_as_rec_id
				,date_downloaded
				,source_data
				,docid_dnum_permit_spp_to_match
				,docid
				,year
				,drid_landseq_src
				,port
				,state
				,port_name
				,port_state
				,port2
				,county
				,link
				,month
				,docn
				,day
				,dealnum
				,dealer_name
				,vessel_name
				,permit
				,hullnum
				,nespp4
				,RSA_SPP
                ,rsa_program_spp
				,rsa_program
				,nespp3
				,spplndlb
				,spplivlb
				,sppvalue
				,vtrserno
				,reported_quantity
				,disposition_code
				,disposition_desc
				,species_itis
				,uom
				,grade_code
				,market_code
				,gear_code
				,dersource
				,cf_license
				,partner_id
				,partner_code
				,partner_affiliation
				,state_dnum
				,negear_vtr
				,dealer_rpt_id
				,landing_seq
				,doe
				,entry_date
				,ddate
				,spp_common_name
				,fishing_year_range
				,fmm
				,fmp_cat_code_list
				,fmp_mort_code_list
				,vessel_rpt_freq_list
				,fmp_mri_list
				,fmp_mri_list_wo_hull
				,fishery_type
				,grade_desc
				,market_desc
				,landing_date_safis
				,fmp_year
				,fy_jan_dec
				,fy_mar_feb
				,fy_may_apr
				,fy_nov_oct
         ,catch_source
         ,source_level1_key_id
          ,source_level2_key_id
          ,SOURCE_LEVEL3_KEY_ID
          ,DOCID_PERMIT_SPP_TO_MATCH
          ,DOCID_PERMIT_SPPITIS_TO_MATCH
          ,VTR_PERMIT_SPP_TO_MATCH
          ,vtr_permit_sppitis_to_match
          ,docid_permit_sppitis3_to_match
          ,vtr_permit_sppitis3_to_match
          ,RTL_TRIP_ID
          ,rtl_trip_id_source
					)
		SELECT 		irtlrsaseq AS rtl_seq_no
				,TO_CHAR(SYSDATE, 'DD-MON-YY') AS DATE_DOWNLOADED 
				,'DEALER'  AS source_data
				,NVL(docid,0)||'-'||NVL(dealnum,0)||'-'||permit||'-'||nespp3 AS docid_dnum_permit_spp_to_match
				,docid
				,year
				,drid_landseq_src
				,port
				,state
				,port_name
				,port_state
				,port2
				,county
				,link
				,month
				,docn
				,day
				,dealnum
				,dealer_name
				,vessel_name
				,permit
				,hullnum
				,nespp4
				,GET_RSA_VERIFY_SPP_FNC(NESPP3) as RSA_SPP
                ,get_rsa_verify_program_spp_fnc(nespp3,GET_RSA_PROGRAM_FOR_SPP_FNC(nespp3)) AS rsa_program_spp
				,GET_RSA_PROGRAM_FOR_SPP_FNC(nespp3) AS RSA_PROGRAM
				,nespp3
				,spplndlb
				,spplivlb
				,sppvalue
				,vtrserno
				,reported_quantity
				,disposition_code
				,disposition_desc
				,species_itis
				,uom
				,grade_code
				,market_code
				,gear_code
				,dersource
				,cf_license
				,partner_id
				,partner_code
				,partner_affiliation
				,state_dnum
				,negear_vtr
				,dealer_rpt_id
				,landing_seq
				,doe
				,entry_date
				,ddate		
				,SPP_COMMON_NAME
				--,GET_PLAN_NER_FNC(fso_admin.get_docid_dateland_fnc(docid,ddate),NESPP3,PERMIT) as PLAN_NER
				--,get_plan_ner_cat_fnc(permit,get_plan_ner_fnc(fso_admin.get_docid_dateland_fnc(docid,ddate),nespp3,permit),ddate) AS ner_plan_cat
				--,GET_PLAN_NER_YR_FNC(fso_admin.get_docid_dateland_fnc(docid,ddate),NESPP3) as PLAN_NER_YEAR
				--,get_plan_ner_fy_range_fnc(get_plan_ner_fnc(fso_admin.get_docid_dateland_fnc(docid,ddate),nespp3,permit)) AS plan_ner_fy_range
				--,0 AS plan_ner_valid
				,GET_FMM_FY_RANGE_FNC(GET_FMM_NO_PERMIT_FNC(nespp3))  AS FISHING_YEAR_RANGE
				,GET_FMM_NO_PERMIT_FNC(nespp3) AS fmm
				,GET_FMP_CAT_CODE_LIST_ND_PO(PERMIT,fso_admin.get_docid_dateland_fnc(docid,ddate),SPECIES_ITIS) FMP_CAT_CODE_LIST
				,GET_FMP_MORT_CODE_LIST_ND_FNC(PERMIT,HULLNUM,fso_admin.get_docid_dateland_fnc(docid,ddate),SPECIES_ITIS) FMP_MORT_CODE_LIST
				, NULL AS VESSEL_RPT_FREQ_LIST
				--, GET_FMP_MRI_LIST_NOHULL_FNC(VESSEL_PERMIT_NUMBER,fishery_moratorium_code,fso_admin.get_docid_dateland_fnc(docid,ddate),SPECIES_ITIS) AS  fmp_mri_list
				, NULL AS fmp_mri_list
				, NULL AS FMP_MRI_LIST_WO_HULL
				, NULL AS FISHERY_TYPE
				,grade_desc
				,market_desc
				,landing_date_safis
				,GET_FMP_YR_FNC(fso_admin.get_docid_dateland_fnc(docid,ddate),nespp3) AS fmp_year
				,GET_FISHING_YEAR_FNC(ddate,'JAN TO DEC') AS fy_jan_dec
				,GET_FISHING_YEAR_FNC(ddate,'MAR TO FEB') AS fy_mar_feb
				,GET_FISHING_YEAR_FNC(ddate,'MAY TO APR') AS fy_may_apr
				,GET_FISHING_YEAR_FNC(ddate,'NOV TO OCT') AS fy_nov_oct	
        ,get_safis_catch_source_fnc(landing_seq) catch_source
        ,dealer_rpt_id as source_level1_key_id
        ,landing_seq as source_level2_key_id
        ,null as SOURCE_LEVEL3_KEY_ID  
        ,NVL(DOCID,0)||'-'||PERMIT||'-'||NESPP3 as DOCID_PERMIT_SPP_TO_MATCH
        ,NVL(DOCID,0)||'-'||PERMIT||'-'||(to_char(trim(leading 0 from species_itis))) as DOCID_PERMIT_SPPITIS_TO_MATCH
        ,NVL(VTRSERNO,0)||'-'||PERMIT||'-'||NESPP3 as VTR_PERMIT_SPP_TO_MATCH
        ,nvl(vtrserno,0)||'-'||permit||'-'||(to_char(trim(leading 0 from species_itis))) as vtr_permit_sppitis_to_match
        ,case when (nespp3 in (select distinct nespp3 from ref_ivr_species_nespp3 where spp_common_name like 'SKATE%'))
          then  nvl(docid,0)||'-'||permit||'-'||'564' 
          else  nvl(docid,0)||'-'||permit||'-'||substr(to_char(trim(leading 0 from species_itis)),1,3)
          end docid_permit_sppitis3_to_match
        ,case when (nespp3 in (select distinct nespp3 from ref_ivr_species_nespp3 where spp_common_name like 'SKATE%'))
          then  nvl(vtrserno,0)||'-'||permit||'-'||'564' 
          else  nvl(vtrserno,0)||'-'||permit||'-'||substr(to_char(trim(leading 0 from species_itis)),1,3)
          end vtr_permit_sppitis3_to_match 
        ,case when (DOCID is not null) then DOCID
             when (VTRSERNO is not null and regexp_like(TRIM(VTRSERNO), '[[:digit:]]')) then TO_NUMBER(VTRSERNO)
             else TO_NUMBER(DEALER_RPT_ID||PERMIT)            
         end RTL_TRIP_ID
         ,case when (DOCID is not null) then 'DOCID'
              when (VTRSERNO is not null and regexp_like(VTRSERNO, '[[:digit:]]')) then 'VTRSERNO'
              else 'DEALER_RPT_ID-PERMIT'
         END rtl_trip_id_source
    FROM fso_admin.cfders_all_years
    where (docid in (select distinct docid from vw_rsa_trip_docids)
	and to_date(ddate) >= to_date(sysdate - 730))
    or (vtrserno in (select distinct vtrserno from vw_rsa_trip_vtrs)
	and to_date(ddate) >= to_date(sysdate - 730))
     or (GET_SAFIS_CATCH_SOURCE_FNC(LANDING_SEQ) ='R'
       and TO_DATE(DDATE) >= TO_DATE(sysdate - 730));
    -- or (DOCID is null and VTRSERNO in (select distinct RSA_VTRNBR from IVR_APPS.RSA_TRIP_STG) and TO_DATE(DDATE) >= TO_DATE(sysdate - 730))
    -- OR (DOCID IS NULL AND vtrserno in (select distinct VTR_NUMBER from vms_apps.vms_rsa_efp_end_hail_stg)AND TO_DATE(ddate) >= TO_DATE(SYSDATE - 730));
	COMMIT;
  
	UPDATE rsa_rtl_dealer
    SET RSA_SPP = 'N'
    WHERE rsa_spp IS NULL;
    COMMIT;

-- 21-JAN-2014  Dwashick  changed rsa_program to UNKNOWN versus NONE as NONE is a valid entry in
--   the sfd_doc_tracking.efp_form_data table.
    UPDATE rsa_rtl_dealer
    SET rsa_program = 'UNKNOWN'
    WHERE rsa_program IS NULL;
    COMMIT;
-----------------------------------------------

------------------------------------------------------------------------------
-- Clean out table from previous load.
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RSA_RTL_IVR';
    COMMIT;
 -- 
 
--		INSERT  /*+ APPEND */ --INTO  test_rsa_ivr
/*				(	rtl_seq_no
				--,rtl_as_rec_id
				,date_downloaded
				,source_data
				,docid_dnum_permit_spp_to_match
				,docid
				,rsa_tripid
				,rsa_trip_landingid			
				,rsa_trip_status
				,rsa_trip_de
				,rsa_trip_confirmnbr
				,rsa_trip_dc
				,rsa_trip_close_confirmnbr
				,rsa_trip_uc
				,rsa_trip_du
				,permit_projectid
				,vessel_permit_number
				,VESSEL_NAME
			--	,vessel_jurisdiction
				,rsa_program
				,rsa_ivr_project_id
				,rsa_sfd_project_id
				,rsa_sfd_project_title
				,stateid
				,state
				,rsa_fishing_areaid
				,speciesid
				,rsa_spp
				,rsa_project_spp
				,nespp3
				,spp_common_name
				,rsa_trip_landing_lbs
				,rsa_trip_landing_de
				,plan_ner
				,plan_ner_year
				,plan_ner_fy_range
				,plan_ner_valid
				,rsa_trip_landing_loligolbs
				,rsa_trip_landing_totallbs
				,fy_jan_dec
				,fy_mar_feb
				,fy_may_apr
				,fy_nov_oct
				,rsa_trip_landing_uc
				,rsa_trip_landing_dc
            )
SELECT  irtlrsaseq AS rtl_seq_no
				--,rtl_as_rec_id
				,TO_CHAR(SYSDATE, 'DD-MON-YY') AS date_downloaded 
				,'IVR' AS source_data
				,NVL(get_rsa_docid_fnc(a.rsa_tripid),0)||'-0-'||get_rsa_nivr_vpnum_fnc(a.permit_projectid)||'-'||get_rsa_nespp3_fnc(b.speciesid) AS docid_dnum_permit_spp_to_match
				,get_rsa_docid_fnc(a.rsa_tripid) AS docid
				,a.rsa_tripid
				,B.RSA_TRIP_LANDINGID		
				,a.rsa_trip_status
				,a.rsa_trip_de
				,a.rsa_trip_confirmnbr
				,a.rsa_trip_dc
				,a.rsa_trip_close_confirmnbr
				,a.rsa_trip_uc
				,a.rsa_trip_du
				,a.permit_projectid
				,get_rsa_nivr_vpnum_fnc(a.permit_projectid) AS vessel_permit_number
				,get_vessel_name_fnc(get_rsa_nivr_vpnum_fnc(a.permit_projectid)) AS vessel_name
				--,get federal jurisdication later
				,get_rsa_sfd_program_fnc(get_rsa_sfd_project_number_fnc(get_rsa_nivr_project_num_fnc(a.permit_projectid)))  AS rsa_program
				,get_rsa_nivr_project_num_fnc(a.permit_projectid) AS rsa_ivr_project_id
				--,get_nivr_rsa_project_name_fnc(get_nivr_rsa_project_num_fnc(a.permit_projectid)) AS rsa_ivr_project_name
				,get_rsa_sfd_project_number_fnc(get_rsa_nivr_project_num_fnc(a.permit_projectid))  AS rsa_sfd_project_id
				,get_rsa_sfd_project_name_fnc(get_rsa_sfd_project_number_fnc(get_rsa_nivr_project_num_fnc(a.permit_projectid))) AS rsa_sfd_project_title
				,b.stateid
				,(select c.state_name from neroivr.nivr_state c where c.stateid=b.stateid) AS state
				--,b.rsa_tripid
				,b.rsa_fishing_areaid
				,b.speciesid
				,GET_RSA_VERIFY_SPP_FNC(GET_RSA_NESPP3_FNC(B.SPECIESID)) as RSA_SPP
				,get_rsa_verify_program_spp_fnc(get_rsa_nespp3_fnc(b.speciesid),get_rsa_sfd_program_fnc(get_rsa_sfd_project_number_fnc(get_rsa_nivr_project_num_fnc(a.permit_projectid)))) AS rsa_program_spp
				,get_rsa_nespp3_fnc(b.speciesid) AS nespp3
				,fso_admin.get_species_common_name_nespp4(get_rsa_nespp4_fnc(b.speciesid)) AS spp_common_name
				--,get_rsa_sppname_fnc(b.speciesid) AS spp_common_name
				,b.rsa_trip_landing_lbs
				,B.RSA_TRIP_LANDING_DE
				,get_plan_ner_fnc(b.rsa_trip_landing_de,get_rsa_nespp3_fnc(b.speciesid),get_rsa_nivr_vpnum_fnc(a.permit_projectid)) AS plan_ner
				,GET_PLAN_NER_YR_FNC(B.RSA_TRIP_LANDING_DE,GET_RSA_NESPP3_FNC(B.SPECIESID)) as PLAN_NER_YEAR
				,get_plan_ner_fy_range_fnc(get_plan_ner_fnc(b.rsa_trip_landing_de,get_rsa_nespp3_fnc(b.speciesid),get_rsa_nivr_vpnum_fnc(a.permit_projectid))) AS plan_ner_fy_range
				,0 AS plan_ner_valid
				,b.rsa_trip_landing_loligolbs
				,b.rsa_trip_landing_totallbs
				,GET_FISHING_YEAR_FNC(b.rsa_trip_landing_de,'JAN TO DEC') AS fy_jan_dec
				,GET_FISHING_YEAR_FNC(b.rsa_trip_landing_de,'MAR TO FEB') AS fy_mar_feb
				,GET_FISHING_YEAR_FNC(b.rsa_trip_landing_de,'MAY TO APR') AS fy_may_apr
				,GET_FISHING_YEAR_FNC(b.rsa_trip_landing_de,'NOV TO OCT') AS fy_nov_oct	
				,b.rsa_trip_landing_uc
				,b.rsa_trip_landing_dc
		FROM neroivr.nivr_rsa_trip a
		,neroivr.nivr_rsa_trip_landing  b
		WHERE a.rsa_tripid = b.rsa_tripid
		AND TO_DATE(b.rsa_trip_landing_de) >= TO_DATE(SYSDATE - 548); */


INSERT  /*+ APPEND */ INTO  rsa_rtl_ivr
				(	
        rtl_seq_no
				--,rtl_as_rec_id
				,date_downloaded
				,source_data
				,docid_dnum_permit_spp_to_match
				,docid
				,rsa_tripid
				,rsa_trip_landingid			
				,rsa_trip_status
			--	,open_trip
				,rsa_trip_de
				,rsa_trip_confirmnbr
				,rsa_trip_dc
				,rsa_trip_close_confirmnbr
				,rsa_trip_monkfsh_ovrgflg
				,rsa_trip_uc
				,rsa_trip_du
				,permit_projectid
				,rsa_vtrnbr
				,gear_typeid
				,broadstock_areaid
				,sector_vessel_flag
				,vessel_permit_number
				,VESSEL_NAME
				,vessel_jurisdiction
				,rsa_program
				,rsa_ivr_project_id
				,rsa_sfd_project_id
				,rsa_sfd_project_title
				,rsa_trip_reportid
				,report_type
				,preland_confirmnbr
				,date_estimate_of_landing
				,anticipated_portid
				,stateid
				,state
				,rsa_fishing_areaid
				,preland
				,speciesid
				,rsa_spp
				,rsa_project_spp
				,nespp3
				,spp_common_name
				,rsa_trip_landing_lbs
				,rsa_trip_landing_de
				,fishing_year_range
				,fmp_year
				,fmm
				,fmp_cat_code_list
				,fmp_mort_code_list
				,vessel_rpt_freq_list
				,fmp_mri_list
				,fmp_mri_list_wo_hull
				,fishery_type
				,rsa_trip_landing_loligolbs
				,rsa_trip_landing_totallbs
				,fy_jan_dec
				,fy_mar_feb
				,fy_may_apr
				,fy_nov_oct
				,rsa_trip_landing_uc
				,rsa_trip_landing_dc
        ,date_rsa_trip_started
        ,date_rsa_trip_ended
        ,project_status_valid
        ,previous_rsa_ivr_project_id
        ,portid
        ,date_estimate_landing_preland
        ,date_estimate_landing_pretrip
        ,source_level1_key_id
        ,source_level2_key_id
        ,source_level3_key_id
        ,trip_age_in_days
        ,date_time_rsa_trip_started
        ,DATE_TIME_RSA_TRIP_ENDED
        ,DOCID_PERMIT_SPP_TO_MATCH
        ,DOCID_PERMIT_SPPITIS_TO_MATCH
        ,vtr_permit_spp_to_match      
        ,vtr_permit_sppitis_to_match
        ,docid_permit_sppitis3_to_match
        ,vtr_permit_sppitis3_to_match
        ,rtl_trip_id
        ,RTL_TRIP_ID_SOURCE
        ,species_itis
        )
SELECT  irtlrsaseq AS rtl_seq_no
				,TO_CHAR(SYSDATE, 'DD-MON-YY') AS DATE_DOWNLOADED 
				,'IVR' AS source_data
				,NVL(get_rsa_docid_fnc(a.rsa_tripid),0)||'-0-'||a.vessel_permit_number||'-'||c.nespp3 AS docid_dnum_permit_spp_to_match
				,get_rsa_docid_fnc(a.rsa_tripid) AS docid
				,a.rsa_tripid
				,c.rsa_trip_landingid		
				,a.rsa_trip_status
				--,a.open_trip
				,a.rsa_trip_de
				,a.rsa_trip_confirmnbr
				,a.date_modified as rsa_trip_dc
				,a.rsa_trip_close_confirmnbr
				,a.rsa_trip_monkfsh_ovrgflg
				,a.modified_by as rsa_trip_uc
				,a.rsa_trip_du
				,a.permit_projectid
				,a.rsa_vtrnbr
				,a.gear_typeid
				,a.broadstock_areaid
				,a.sector_vessel_flag				
				,a.vessel_permit_number
				,GET_VESSEL_NAME_FNC(a.VESSEL_PERMIT_NUMBER) as VESSEL_NAME
				,CASE WHEN (a.vessel_permit_number IN (SELECT DISTINCT permit FROM fso_admin.research_code WHERE STATE_FED='STATE'))  THEN 'STATE'
            WHEN (a.vessel_permit_number IN (SELECT DISTINCT permit FROM fso_admin.research_code WHERE STATE_FED='FED'))  THEN 'FEDERAL'
            ELSE NULL
            END vessel_jurisdiction
				,a.rsa_program
				,a.rsa_ivr_project_id
				--,get_nivr_rsa_project_name_fnc(get_nivr_rsa_project_num_fnc(a.permit_projectid)) AS rsa_ivr_project_name
				,a.rsa_sfd_project_id
				,A.RSA_SFD_PROJECT_TITLE
        ,b.rsa_trip_reportid
				,b.report_type
				,b.preland_confirmnbr
				,a.date_estimate_of_landing
				,a.anticipated_portid
				,c.stateid
				,c.state
				--,c.rsa_tripid
				,c.rsa_fishing_areaid
				,c.preland
				,c.speciesid
				,C.RSA_SPP
				,c.rsa_project_spp
				,c.nespp3
				,c.spp_common_name				
				,C.RSA_TRIP_LANDING_LBS
				--,c.date_created as rsa_trip_landing_de
        ,a.date_rsa_trip_ended as rsa_trip_landing_de
				,GET_FMM_FY_RANGE_FNC(GET_FMM_NO_PERMIT_FNC(c.nespp3))  AS FISHING_YEAR_RANGE
				,GET_FMP_YR_FNC(a.date_rsa_trip_ended,c.nespp3) AS fmp_year
				,GET_FMM_NO_PERMIT_FNC(c.nespp3) AS fmm
				,GET_FMP_CAT_CODE_LIST_ND_PO(a.vessel_permit_number,c.date_created,GET_SPP_ITIS_NESPP3_FNC(c.nespp3)) FMP_CAT_CODE_LIST
				,NULL AS FMP_MORT_CODE_LIST
				,NULL AS VESSEL_RPT_FREQ_LIST
				,NULL AS FMP_MRI_LIST
				,NULL AS FMP_MRI_LIST_WO_HULL
				,NULL AS FISHERY_TYPE				
				,c.rsa_trip_landing_loligolbs
				,c.rsa_trip_landing_totallbs
				,c.fy_jan_dec
				,c.fy_mar_feb
				,c.fy_may_apr
				,c.fy_nov_oct	
				,c.modified_by as rsa_trip_landing_uc
				,c.date_modified as rsa_trip_landing_dc
        ,a.date_rsa_trip_started
        ,a.date_rsa_trip_ended
       -- ,GET_RSA_VALID_PROJECT_FNC((GET_RSA_SFD_PROJECT_NUMBER_FNC(GET_RSA_NIVR_PROJECT_NUM_FNC(a.permit_projectid))), a.date_rsa_trip_ended)
      --   ,get_rsa_valid_project_fnc((get_rsa_sfd_project_number_fnc(get_rsa_nivr_project_num_fnc(a.permit_projectid))), to_date(a.rsa_trip_de))
        ,GET_RSA_VALID_PROJECT_FNC(a.rsa_sfd_project_id, (NVL(TO_DATE(a.date_rsa_trip_ended),TO_DATE(a.date_rsa_trip_started))) ) AS project_status_valid
--        ,GET_PREV_RSA_IVR_PROJECTID_FNC(a.vessel_permit_number)
        ,CASE WHEN LAG(a.vessel_permit_number,1) OVER (ORDER BY a.vessel_permit_number,c.rsa_trip_landingid) = a.vessel_permit_number
            THEN LAG(a.rsa_ivr_project_id,1) OVER (ORDER BY a.vessel_permit_number,c.rsa_trip_landingid)
         ELSE NULL
         END AS previous_rsa_ivr_project_id
         ,c.portid
         ,b.date_estimated_landing_preland  as  date_estimate_landing_preland
         ,a.date_estimate_of_landing AS date_estimate_landing_pretrip
          ,a.rsa_tripid as  source_level1_key_id
          ,b.rsa_trip_reportid as  source_level2_key_id
          ,c.rsa_trip_landingid	AS source_level3_key_id 
        ,(SYSDATE - a.date_rsa_trip_started) AS trip_age_in_days
        ,TO_CHAR(a.date_rsa_trip_started, 'DD-MON-YY HH24:MI:SS') AS date_time_rsa_trip_started
        ,to_char(a.date_rsa_trip_ended, 'DD-MON-YY HH24:MI:SS') as date_time_rsa_trip_ended
	      ,nvl(get_rsa_docid_fnc(a.rsa_tripid),0)||'-'||a.vessel_permit_number||'-'||c.nespp3 as docid_permit_spp_to_match
        ,nvl(get_rsa_docid_fnc(a.rsa_tripid),0)||'-'||a.vessel_permit_number||'-'||get_spp_itis_nespp3_fnc(c.nespp3) as docid_permit_sppitis_to_match
        ,nvl(a.rsa_vtrnbr,0)||'-'||a.vessel_permit_number||'-'||c.nespp3 as vtr_permit_spp_to_match
        ,nvl(a.rsa_vtrnbr,0)||'-'||a.vessel_permit_number||'-'||get_spp_itis_nespp3_fnc(c.nespp3)   as vtr_permit_sppitis_to_match
        ,case when (c.speciesid in (select distinct r.speciescd from ref_ivr_species_nespp3 r where r.spp_common_name like 'SKATE%'))
          then  nvl(get_rsa_docid_fnc(a.rsa_tripid),0)||'-'||a.vessel_permit_number||'-'||'564' 
          else nvl(get_rsa_docid_fnc(a.rsa_tripid),0)||'-'||a.vessel_permit_number||'-'||substr(get_spp_itis_nespp3_fnc(c.nespp3),1,3) 
          end docid_permit_sppitis3_to_match
        ,case when (c.speciesid in (select distinct r.speciescd from ref_ivr_species_nespp3 r where r.spp_common_name like 'SKATE%'))
            then  nvl(a.rsa_vtrnbr,0)||'-'||a.vessel_permit_number||'-'||'564' 
            else nvl(a.rsa_vtrnbr,0)||'-'||a.vessel_permit_number||'-'||substr(get_spp_itis_nespp3_fnc(c.nespp3),1,3) 
            end vtr_permit_sppitis3_to_match 
        ,case when (GET_RSA_DOCID_FNC(a.RSA_TRIPID) is not null) then GET_RSA_DOCID_FNC(a.RSA_TRIPID)
             when ((a.RSA_VTRNBR is not null AND a.rsa_vtrnbr <>0) and regexp_like(TRIM(a.RSA_VTRNBR), '[[:digit:]]')) then TO_NUMBER(a.RSA_VTRNBR)
             else TO_NUMBER(a.RSA_TRIPID)            
         end RTL_TRIP_ID
         ,case when (GET_RSA_DOCID_FNC(a.RSA_TRIPID) is not null) then 'DOCID'
              when ((a.RSA_VTRNBR is not null and a.rsa_vtrnbr <>0) and regexp_like(a.RSA_VTRNBR, '[[:digit:]]')) then 'VTRSERNO'
              else 'RSA_TRIPID'
         end RTL_TRIP_ID_SOURCE 
         ,get_spp_itis_nespp3_fnc(c.nespp3)  species_itis
    FROM ivr_apps.rsa_trip_stg a
		,ivr_apps.rsa_trip_report_stg b		
		,ivr_apps.rsa_trip_landing_stg  c
		where a.rsa_tripid = b.rsa_tripid(+) 
		AND B.RSA_TRIP_REPORTID = C.RSA_TRIP_REPORTID(+)
	--	AND TO_DATE(C.DATE_CREATED) >= TO_DATE(SYSDATE - 548);
  	AND TO_DATE(A.RSA_TRIP_DE) >= TO_DATE(SYSDATE - 720)
    AND b.report_type <>'LEGACY'   -- 01-APR-15 DW -- omitted legacy data from audit
    ;
    COMMIT;
    
    -- 07/14/2014 Added by Netty to insert IVR trip without landing
--    INSERT  /*+ APPEND */ INTO  rsa_rtl_ivr
--				(	
--        rtl_seq_no
--				--,rtl_as_rec_id
--				,date_downloaded
--				,source_data
--				,docid_dnum_permit_spp_to_match
--				,docid
--				,rsa_tripid
--				,rsa_trip_landingid			
--				,rsa_trip_status
--				,open_trip
--				,rsa_trip_de
--				,rsa_trip_confirmnbr
--				,rsa_trip_dc
--				,rsa_trip_close_confirmnbr
--				,rsa_trip_monkfsh_ovrgflg
--				,rsa_trip_uc
--				,rsa_trip_du
--				,permit_projectid
--				,rsa_vtrnbr
--				,gear_typeid
--				,broadstock_areaid
--				,sector_vessel_flag
--				,vessel_permit_number
--				,VESSEL_NAME
--			--	,vessel_jurisdiction
--				,rsa_program
--				,rsa_ivr_project_id
--				,rsa_sfd_project_id
--				,rsa_sfd_project_title
--				,rsa_trip_reportid
--				,report_type
--				,preland_confirmnbr
--				,date_estimate_of_landing
--				,anticipated_portid
--				,stateid
--				,state
--				,rsa_fishing_areaid
--				,preland
--				,speciesid
--				,rsa_spp
--				,rsa_project_spp
--				,nespp3
--				,spp_common_name
--				,rsa_trip_landing_lbs
--				,rsa_trip_landing_de
--				,fishing_year_range
--				,fmp_year
--				,fmm
--				,fmp_cat_code_list
--				,fmp_mort_code_list
--				,vessel_rpt_freq_list
--				,fmp_mri_list
--				,fmp_mri_list_wo_hull
--				,fishery_type
--				,rsa_trip_landing_loligolbs
--				,rsa_trip_landing_totallbs
--				,fy_jan_dec
--				,fy_mar_feb
--				,fy_may_apr
--				,fy_nov_oct
--				,rsa_trip_landing_uc
--				,rsa_trip_landing_dc
--        ,date_rsa_trip_started
--        ,date_rsa_trip_ended
--        ,project_status_valid
--        ,previous_rsa_ivr_project_id
--        ,portid
--        ,source_level1_key_id
--        ,trip_age_in_days
--        ,date_time_rsa_trip_started
--        ,date_time_rsa_trip_ended
--        )
--		SELECT  irtlrsaseq AS rtl_seq_no
--				,TO_CHAR(SYSDATE, 'DD-MON-YY') AS DATE_DOWNLOADED 
--				,'IVR' AS source_data
--				,NULL AS docid_dnum_permit_spp_to_match --test
--				,get_rsa_docid_fnc(a.rsa_tripid) AS docid
--				,a.rsa_tripid
--				,NULL AS rsa_trip_landingid	
--				,a.rsa_trip_status
--				,NULL AS open_trip --test
--				,a.rsa_trip_de
--				,a.rsa_trip_confirmnbr
--				,rsa_trip_dc
--				,a.rsa_trip_close_confirmnbr
--				,a.rsa_trip_monkfsh_ovrgflg
--				,rsa_trip_uc
--				,a.rsa_trip_du
--				,a.permit_projectid
--				,a.rsa_vtrnbr
--				,a.gear_typeid
--				,a.broadstock_areaid
--				,a.sector_vessel_flag				
--				,NULL AS vessel_permit_number --test
--				,null as vessel_name --test
--				,IVR_APPS.FNC_FIND_RSA_PROGRAM(a.permit_projectid) AS rsa_program 
--				,get_rsa_nivr_project_num_fnc(a.permit_projectid) as rsa_ivr_project_id 
--				,get_rsa_sfd_project_number_fnc(get_rsa_nivr_project_num_fnc(a.permit_projectid))  AS rsa_sfd_project_id
--				,NULL AS RSA_SFD_PROJECT_TITLE --test
--        ,NULL AS rsa_trip_reportid
--				,NULL AS report_type
--				,a.preland_confirmnbr
--				,a.date_estimate_of_landing
--				,a.anticipated_portid
--				,NULL AS stateid
--				,NULL AS state
--				,NULL AS rsa_fishing_areaid
--				,NULL AS preland
--				,NULL AS speciesid
--				,NULL AS RSA_SPP
--				,NULL AS rsa_project_spp
--				,NULL AS nespp3
--				,NULL AS spp_common_name				
--				,NULL AS RSA_TRIP_LANDING_LBS
--				,NULL AS rsa_trip_landing_de
--				,NULL AS FISHING_YEAR_RANGE
--				,NULL AS fmp_year
--				,NULL AS fmm
--				,NULL AS FMP_CAT_CODE_LIST
--				,NULL AS FMP_MORT_CODE_LIST
--				,NULL AS VESSEL_RPT_FREQ_LIST
--				,NULL AS FMP_MRI_LIST
--				,NULL AS FMP_MRI_LIST_WO_HULL
--				,NULL AS FISHERY_TYPE				
--				,NULL AS rsa_trip_landing_loligolbs
--				,NULL AS rsa_trip_landing_totallbs
--				,NULL AS fy_jan_dec
--				,NULL AS fy_mar_feb
--				,NULL AS fy_may_apr
--				,NULL AS fy_nov_oct	
--				,NULL AS rsa_trip_landing_uc
--				,NULL AS rsa_trip_landing_dc
--				,a.date_rsa_trip_started
--				,a.date_rsa_trip_ended
--        ,GET_RSA_VALID_PROJECT_FNC((GET_RSA_SFD_PROJECT_NUMBER_FNC(GET_RSA_NIVR_PROJECT_NUM_FNC(a.permit_projectid))), a.date_rsa_trip_ended)
--        ,NULL AS previous_rsa_ivr_project_id
--        ,null as port_id
--        ,rsa_tripid AS  source_level1_key_id
--        ,(SYSDATE - a.date_rsa_trip_started) AS trip_age_in_days
--        ,TO_CHAR(a.date_rsa_trip_started, 'DD-MON-YY HH24:MI:SS') AS date_time_rsa_trip_started
--        ,TO_CHAR(a.date_rsa_trip_ended, 'DD-MON-YY HH24:MI:SS') AS date_time_rsa_trip_ended
--		FROM NEROIVR.NIVR_RSA_TRIP a
--  --  WHERE RSA_TRIPID NOT IN (SELECT RSA_TRIPID FROM IVR_APPS.RSA_TRIP_LANDING_STG);
--     WHERE RSA_TRIPID NOT IN (SELECT DISTINCT RSA_TRIPID FROM RSA_RTL_IVR);
    
---- REMOVE TEST IVR ENTRIES
     DELETE FROM RSA_RTL_IVR
     WHERE VESSEL_PERMIT_NUMBER IN ('111111','555555','888888');
     COMMIT;

-- UPDATE SCALLOP RSA PROGRAM - PROJECTS NOT CURRENTLY IN SFD DOC TRACKING DATABASE
-- REMOVE UPDATE WHEN ENTERED IN DATABASE
   /*  UPDATE TEST_RSA_IVR
     SET RSA_PROGRAM = 'SCALLOP'
     WHERE RSA_PROGRAM IS NULL
     AND rsa_ivr_project_id IN ('1107'
            ,'1112'
            ,'1202'
            ,'1203'
            ,'1204'
            ,'1205'
            ,'1206'
            ,'1209'
            ,'1210'
            ,'1212'
            ,'1213'
            ,'1215'
            ,'1216'
            ,'1217'
            ,'1301'
            ,'1302'
            ,'1303'
            ,'1304'
            ,'1306'
            ,'1307'
            ,'1309'
            ,'1310'
            ,'1311'
            ,'1313'
            ,'1314'
            ,'1315'
            ,'1317'
            ,'1318')
            ;*/
            COMMIT; 
------------------------------------------------------------------------------
-- Clean out table from previous load.
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RSA_RTL_VMS';
 -- 
 
		INSERT  /*+ APPEND */ INTO  rsa_rtl_vms
              (RTL_SEQ_NO
            --	,rtl_as_rec_id
              ,date_downloaded
              ,source_data
              ,docid_dnum_permit_spp_to_match
              ,docid
              ,message_id
              ,form_type
              ,rsa_efp_hail_id
              ,time_sent
              ,vessel_permit_number
              ,vessel_name
              ,vessel_jurisdiction
              ,hull_id
              ,vtr_number
              ,rsa_program
              ,project_code
              ,landing_city
              ,landing_state
              ,estimated_arrival
              ,estimated_offload
              ,species_column
              ,species_name
              ,species_code
              ,species_kept
              ,species_discard
              ,herring_area
              ,rsa_spp
              ,rsa_project_spp
              ,nespp3
              ,species_itis
              ,spp_common_name
              ,fishing_year_range
              ,FMM
              ,fmp_year
              ,fmp_cat_code_list
              ,fmp_mort_code_list
              ,vessel_rpt_freq_list
              ,fmp_mri_list
              ,fmp_mri_list_wo_hull
              ,fishery_type
              ,fy_jan_dec
              ,fy_mar_feb
              ,fy_may_apr
              ,FY_NOV_OCT
              ,activity_code
              ,source_level1_key_id
              ,source_level2_key_id
              ,SOURCE_LEVEL3_KEY_ID  
              ,trip_age_in_days
              ,DATE_TIME_SENT
              ,DOCID_PERMIT_SPP_TO_MATCH
              ,DOCID_PERMIT_SPPITIS_TO_MATCH
              ,VTR_PERMIT_SPP_TO_MATCH
              ,vtr_permit_sppitis_to_match
              ,docid_permit_sppitis3_to_match
              ,vtr_permit_sppitis3_to_match
              ,rtl_trip_id
              ,rtl_trip_id_source
              )
          SELECT irtlrsaseq AS rtl_seq_no
          --,rtl_as_rec_id
              ,to_char(sysdate, 'DD-MON-YY') as date_downloaded
                ,'VMS END HAIL' AS SOURCE_DATA
                ,NVL(GET_DOCID_FNC(vtr_number),0)||'-0-'||vessel_permit_number||'-'||get_rsa_nespp3_fnc(species_code) AS docid_dnum_permit_spp_to_match
                ,GET_DOCID_FNC(vtr_number) AS docid
                ,message_id
                ,form_type
                ,rsa_efp_hail_id
                ,time_sent
                ,vessel_permit_number
                ,get_vessel_name_fnc(vessel_permit_number) AS vessel_name
                ,NULL AS vessel_jurisdiction
                ,hull_id
                ,vtr_number
                ,rsa_program
                ,project_code
                ,landing_city
                ,landing_state
                ,estimated_arrival
                ,estimated_offload
                ,species_column
                ,species_name
                ,species_code
                ,species_kept
                ,species_discard
                ,herring_area
                ,GET_RSA_VERIFY_SPP_FNC(GET_RSA_NESPP3_FNC(SPECIES_CODE)) as RSA_SPP
                ,GET_RSA_VERIFY_PROGRAM_SPP_FNC(GET_RSA_NESPP3_FNC(SPECIES_CODE),RSA_PROGRAM) AS RSA_PROGRAM_SPP
                ,GET_RSA_NESPP3_FNC(SPECIES_CODE) AS NESPP3
                ,GET_SPP_ITIS_NESPP3_FNC(GET_RSA_NESPP3_FNC(SPECIES_CODE)) AS species_itis
                ,species_name AS spp_common_name
                ,GET_FMP_FYRANGE_BY_SPP_FNC(GET_RSA_NESPP3_FNC(SPECIES_CODE)) AS fishing_year_range
                ,GET_FMM_NO_PERMIT_FNC(GET_RSA_NESPP3_FNC(SPECIES_CODE)) AS FMM
                ,GET_FMP_YR_FNC(ESTIMATED_OFFLOAD,GET_RSA_NESPP3_FNC(SPECIES_CODE)) AS fmp_year
                ,GET_FMP_CAT_CODE_LIST_FNC(VESSEL_PERMIT_NUMBER,HULL_ID,ESTIMATED_OFFLOAD,ESTIMATED_OFFLOAD,GET_SPP_ITIS_NESPP3_FNC(GET_RSA_NESPP3_FNC(SPECIES_CODE))) FMP_CAT_CODE_LIST
                ,GET_FMP_MORT_CODE_LIST_FNC(VESSEL_PERMIT_NUMBER,HULL_ID,ESTIMATED_OFFLOAD,ESTIMATED_OFFLOAD,GET_SPP_ITIS_NESPP3_FNC(GET_RSA_NESPP3_FNC(SPECIES_CODE))) FMP_MORT_CODE_LIST
                ,GET_VESSEL_RPT_FREQ_LIST_FNC(VESSEL_PERMIT_NUMBER,HULL_ID,estimated_offload,estimated_offload,GET_SPP_ITIS_NESPP3_FNC(GET_RSA_NESPP3_FNC(SPECIES_CODE))) VESSEL_RPT_FREQ_LIST
                --,GET_FMP_MRI_LIST_NOHULL_FNC(VESSEL_PERMIT_NUMBER,b.fishery_moratorium_code,estimated_offload,GET_SPP_ITIS_NESPP3_FNC(GET_RSA_NESPP3_FNC(SPECIES_CODE)))  fmp_mri_list
                --,GET_FMP_MRI_LIST_NOHULL_FNC(VESSEL_PERMIT_NUMBER,b.fishery_moratorium_code,estimated_offload,GET_SPP_ITIS_NESPP3_FNC(GET_RSA_NESPP3_FNC(SPECIES_CODE))) fmp_mri_list_wo_hull
                ,NULL AS fmp_mri_list
                ,NULL AS fmp_mri_list_wo_hull
                ,NULL AS fishery_type
                ,GET_FISHING_YEAR_FNC(estimated_offload,'JAN TO DEC') AS fy_jan_dec
                ,GET_FISHING_YEAR_FNC(estimated_offload,'MAR TO FEB') AS fy_mar_feb
                ,GET_FISHING_YEAR_FNC(estimated_offload,'MAY TO APR') AS fy_may_apr
                ,GET_FISHING_YEAR_FNC(estimated_offload,'NOV TO OCT') AS fy_nov_oct	
                ,activity_code
                ,rsa_efp_hail_id as source_level1_key_id
                ,null as source_level2_key_id
                ,NULL AS source_level3_key_id
                ,(SYSDATE - time_sent) AS trip_age_in_days
                ,to_char(time_sent, 'DD-MON-YY HH24:MI:SS') as date_time_sent
                ,nvl(get_docid_fnc(vtr_number),0)||'-'||vessel_permit_number||'-'||get_rsa_nespp3_fnc(species_code)  as docid_permit_spp_to_match
                ,nvl(get_docid_fnc(vtr_number),0)||'-'||vessel_permit_number||'-'||get_spp_itis_nespp3_fnc(get_rsa_nespp3_fnc(species_code))  as docid_permit_sppitis_to_match
                ,nvl(vtr_number,0)||'-'||vessel_permit_number||'-'||get_rsa_nespp3_fnc(species_code)  as vtr_permit_spp_to_match
                ,nvl(vtr_number,0)||'-'||vessel_permit_number||'-'||get_spp_itis_nespp3_fnc(get_rsa_nespp3_fnc(species_code))  as vtr_permit_sppitis_to_match     
                ,case when (species_code in (select distinct speciescd from ref_ivr_species_nespp3 where spp_common_name like 'SKATE%'))
                      then  nvl(get_docid_fnc(vtr_number),0)||'-'||vessel_permit_number||'-'||'564' 
                      else nvl(get_docid_fnc(vtr_number),0)||'-'||vessel_permit_number||'-'||substr(get_spp_itis_nespp3_fnc(get_rsa_nespp3_fnc(species_code)),1,3) 
                      end docid_permit_sppitis3_to_match
                ,case when (species_code in (select distinct speciescd from ref_ivr_species_nespp3 where spp_common_name like 'SKATE%'))
                      then  nvl(vtr_number,0)||'-'||vessel_permit_number||'-'||'564' 
                      else nvl(vtr_number,0)||'-'||vessel_permit_number||'-'||substr(get_spp_itis_nespp3_fnc(get_rsa_nespp3_fnc(species_code)),1,3) 
                      end vtr_permit_sppitis3_to_match
                ,case when (GET_DOCID_FNC(VTR_NUMBER) is not null) then GET_DOCID_FNC(VTR_NUMBER)
                      when ((vtr_number is not null AND vtr_number <> 0) and regexp_like(trim(vtr_number), '[[:digit:]]')) then to_number(vtr_number)
                      else to_number(rsa_efp_hail_id)            
                end RTL_TRIP_ID
               ,case when (GET_DOCID_FNC(VTR_NUMBER) is not null) then 'DOCID'
                     when ((vtr_number is not null AND vtr_number <> 0) and regexp_like(vtr_number, '[[:digit:]]')) then 'VTRSERNO'
                     else 'RSA_EFP_HAIL_ID'
                END rtl_trip_id_source 
          from  vms_apps.vw_vms_rsa_hail_species_kept
          WHERE TO_DATE(ESTIMATED_OFFLOAD) >= TO_DATE(SYSDATE - 720); 
          COMMIT;  
          
          -- 07/09/2014 Added by Netty to add the sailing information for the VMS
          	INSERT  /*+ APPEND */ INTO  rsa_rtl_vms
              (RTL_SEQ_NO
              ,date_downloaded
              ,source_data
              ,docid_dnum_permit_spp_to_match
              ,docid
              ,message_id
              ,form_type
              ,rsa_efp_hail_id
              ,time_sent
              ,vessel_permit_number
              ,vessel_name
              ,vessel_jurisdiction
              ,hull_id
              ,vtr_number
              ,rsa_program
              ,project_code
              ,landing_city
              ,landing_state
              ,estimated_arrival
              ,estimated_offload
              ,species_column
              ,species_name
              ,species_code
              ,species_kept
              ,species_discard
              ,herring_area
              ,rsa_spp
              ,rsa_project_spp
              ,nespp3
              ,species_itis
              ,spp_common_name
              ,fishing_year_range
              ,FMM
              ,fmp_year
              ,fmp_cat_code_list
              ,fmp_mort_code_list
              ,vessel_rpt_freq_list
              ,fmp_mri_list
              ,fmp_mri_list_wo_hull
              ,fishery_type
              ,fy_jan_dec
              ,fy_mar_feb
              ,fy_may_apr
              ,FY_NOV_OCT
               ,source_level1_key_id
               ,source_level2_key_id
               ,source_level3_key_id 
               ,trip_age_in_days
               ,DATE_TIME_SENT
              -- ,DOCID_PERMIT_SPP_TO_MATCH
             -- ,DOCID_PERMIT_SPPITIS_TO_MATCH
             -- ,VTR_PERMIT_SPP_TO_MATCH
             -- ,vtr_permit_sppitis_to_match
               ,rtl_trip_id
              ,rtl_trip_id_source
              )
          SELECT irtlrsaseq AS rtl_seq_no
              ,TO_CHAR(SYSDATE, 'DD-MON-YY') AS date_downloaded
                ,'VMS_START' AS SOURCE_DATA
                ,NVL(GET_DOCID_FNC(vtr_number),0)||'-0-'||vessel_permit_number||'-' AS docid_dnum_permit_spp_to_match
                ,GET_DOCID_FNC(vtr_number) AS docid
                ,message_id
                ,'SAIL'
                ,rsa_efp_start_hail_id
                ,time_sent
                ,vessel_permit_number
                ,get_vessel_name_fnc(vessel_permit_number) AS vessel_name
                ,NULL AS vessel_jurisdiction
                ,hull_id
                ,vtr_number
                ,vms_apps.GET_VMS_RSA_SFD_PROGRAM_FNC(project_code) AS rsa_program
                ,project_code
                ,landing_city
                ,landing_state
                ,estimated_arrival
                ,NULL
                ,NULL
                ,NULL
                ,NULL
                ,NULL
                ,NULL
                ,NULL
                ,NULL
                ,NULL
                ,NULL
                ,NULL
                ,NULL
                ,NULL
                ,NULL
                ,NULL
                ,NULL
                ,NULL
                ,NULL
                --,GET_FMP_MRI_LIST_NOHULL_FNC(VESSEL_PERMIT_NUMBER,b.fishery_moratorium_code,estimated_offload,GET_SPP_ITIS_NESPP3_FNC(GET_RSA_NESPP3_FNC(SPECIES_CODE)))  fmp_mri_list
                --,GET_FMP_MRI_LIST_NOHULL_FNC(VESSEL_PERMIT_NUMBER,b.fishery_moratorium_code,estimated_offload,GET_SPP_ITIS_NESPP3_FNC(GET_RSA_NESPP3_FNC(SPECIES_CODE))) fmp_mri_list_wo_hull
                ,NULL AS fmp_mri_list
                ,NULL AS fmp_mri_list_wo_hull
                ,NULL AS fishery_type
                ,NULL AS fy_jan_dec
                ,NULL AS fy_mar_feb
                ,NULL AS fy_may_apr
                ,NULL AS fy_nov_oct
                ,rsa_efp_start_hail_id as source_level1_key_id
                ,null as source_level2_key_id
                ,NULL AS source_level3_key_id 
                ,(SYSDATE - time_sent) AS trip_age_in_days
                ,TO_CHAR(TIME_SENT, 'DD-MON-YY HH24:MI:SS') as DATE_TIME_SENT
               -- ,NVL(GET_DOCID_FNC(VTR_NUMBER),0)||VESSEL_PERMIT_NUMBER||'-'||GET_RSA_NESPP3_FNC(SPECIES_CODE)  as DOCID_PERMIT_SPP_TO_MATCH
               -- ,NVL(GET_DOCID_FNC(VTR_NUMBER),0)||VESSEL_PERMIT_NUMBER||'-'||GET_SPP_ITIS_NESPP3_FNC(GET_RSA_NESPP3_FNC(SPECIES_CODE))  as DOCID_PERMIT_SPPITIS_TO_MATCH
               -- ,NVL(VTR_NUMBER,0)||VESSEL_PERMIT_NUMBER||'-'||GET_RSA_NESPP3_FNC(SPECIES_CODE)  as VTR_PERMIT_SPP_TO_MATCH
              --  ,NVL(VTR_NUMBER,0)||VESSEL_PERMIT_NUMBER||'-'||GET_SPP_ITIS_NESPP3_FNC(GET_RSA_NESPP3_FNC(SPECIES_CODE))  as vtr_permit_sppitis_to_match     
                ,case when (GET_DOCID_FNC(vtr_number) is not null) then GET_DOCID_FNC(vtr_number)
                      when ((vtr_number is not null AND vtr_number <> 0) and regexp_like(trim(vtr_number), '[[:digit:]]')) then to_number(vtr_number)
                      else to_number(rsa_efp_start_hail_id)            
                end RTL_TRIP_ID
               ,case when (GET_DOCID_FNC(vtr_number) is not null) then 'DOCID'
                     when ((vtr_number is not null AND vtr_number <> 0) and regexp_like(vtr_number, '[[:digit:]]')) then 'VTRSERNO'
                     else 'RSA_EFP_START_HAIL_ID'
                end rtl_trip_id_source     
          from  vms_apps.vms_rsa_efp_start_hail_rep
          WHERE RECORD_IS_DELETED=0
          AND TO_DATE(DATE_CREATED) >= TO_DATE(SYSDATE - 720); 
          COMMIT;  
          
          
   update  rsa_rtl_vms
   SET project_status_valid =  get_rsa_valid_project_fnc((get_rsa_sfd_project_number_fnc(project_code)), to_date(trunc(time_sent)));
 COMMIT;
------------------------------------------------------------------------------------------------

 -- Clean out table from previous load.
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RSA_RTL_VESSEL';
 -- 
 
		INSERT  /*+ APPEND */ INTO rsa_rtl_vessel
			( rtl_seq_no
			--,rtl_as_rec_id
			,date_downloaded
			,source_data
			,docid_dnum_permit_spp_to_match
			,docid
			,vessel_id
			,date_sail
			,date_land
			,tripcatg
			,tripcatg_desc
			,crew
			,nanglers
			,port1
			,port2
			,port3
			,state1
			,state2
			,state3
			,datelnd2
			,datelnd3
			,operator_num
			,operator_name
			,date_signed
			,FISHED
			,VESSEL_PERMIT_NUM
      ,vessel_name
			,origin
			,trip_activity_type
			,trip_activity_type_desc
			,fy_jan_dec
			,fy_mar_feb
			,fy_may_apr
			,fy_nov_oct
			,imgid
			,date_received
			,serial_num
			,gearcode
			,gearqty
			,gearsize
			,area
			,depth
			,lat_degree
			,lat_minute
			,lat_second
			,lat_dir
			,lon_degree
			,lon_minute
			,lon_second
			,lon_dir
			,ntows
			,towhrs
			,towmin
			,mesh
			,catch_id
			,species_id
			,spp_common_name
			,kept
			,discarded
			,dealer_num
			,date_sold
			,port_landed
			,state_landed
			,port_number
			,RSA_SPP
      ,rsa_project_spp
			,rsa_program
			,nespp3
			,stock_id
			,landed_pounds
			,live_pounds
			,fishing_year_range
			,fmp_year
			,fmm
			,fmp_cat_code_list
			,fmp_mort_code_list
			,vessel_rpt_freq_list
			,fmp_mri_list
			,fmp_mri_list_wo_hull
			,fishery_type
			,dealer_type
			,disposition_of_catch
			,dealer_number
			,dealer_name 
      ,source_level1_key_id
      ,source_level2_key_id
      ,source_level3_key_id  
      ,date_time_sail
      ,DATE_TIME_LAND
      ,DOCID_PERMIT_SPP_TO_MATCH
      ,docid_permit_sppitis_to_match
      ,docid_permit_sppitis3_to_match
      ,rtl_trip_id
      ,RTL_TRIP_ID_SOURCE
      ,species_itis
		)
SELECT  irtlrsaseq AS rtl_seq_no
				--,rtl_as_rec_id
				,TO_CHAR(SYSDATE, 'DD-MON-YY') AS date_downloaded 
				,'VESSEL' AS source_data
				,d.docid||'-'||NVL(c.dealer_num,0)||'-'||d.vessel_permit_num||'-'||GET_NESPP3_FOR_SPPCODE_FNC(c.species_id) AS docid_dnum_permit_spp_to_match
				,d.docid
				,d.vessel_id
				,d.date_sail
				,d.date_land
				,d.tripcatg
				,DECODE(d.tripcatg,1,'COMMERCIAL'
										  ,2,'PARTY'
										  ,3,'CHARTER'
										  ,4,'RSA'
										  ,NULL)  AS tripcatg_desc
				,d.crew
				,d.nanglers
				,d.port1
				,d.port2
				,d.port3
				,d.state1
				,d.state2
				,d.state3
				,d.datelnd2
				,d.datelnd3
				,d.operator_num
				,d.operator_name
				,d.date_signed
				,D.FISHED
				,D.VESSEL_PERMIT_NUM
        ,get_vessel_name_fnc(d.vessel_permit_num) AS vessel_name
				,d.origin
				,d.trip_activity_type
				,DECODE(d.trip_activity_type,0,'EFFORT FISHING TRIP'
													,1,'SET ONLY TRIP'
													,2,'MECHANICAL BREAKDOWN'
													,3,'TURNAROUND DUE TO WEATHER'
													,4,'TRANSITING TRIP'
													,5,'HERRING CARRIER VESSEL'
													,6,'TURNAROUND ILLNESS/INJURY'
													,7,'OTHER'
													,NULL)  AS trip_activity_type_desc
				,GET_FISHING_YEAR_FNC(d.date_land,'JAN TO DEC') AS fy_jan_dec
				,GET_FISHING_YEAR_FNC(d.date_land,'MAR TO FEB') AS fy_mar_feb
				,GET_FISHING_YEAR_FNC(d.date_land,'MAY TO APR') AS fy_may_apr
				,GET_FISHING_YEAR_FNC(d.date_land,'NOV TO OCT') AS fy_nov_oct									
				,i.imgid
				,i.date_received
				,i.serial_num
				,i.gearcode
				,i.gearqty
				,i.gearsize
				,i.area
				,i.depth
				,i.lat_degree
				,i.lat_minute
				,i.lat_second
				,i.lat_dir
				,i.lon_degree
				,i.lon_minute
				,i.lon_second
				,i.lon_dir
				,i.ntows
				,i.towhrs
				,i.towmin
				,i.mesh
				,c.catch_id
				,c.species_id
				-- spp_common_name
				,fso_admin.GET_SPECIES_COMMON_NAME_NESPP4(GET_NESPP4_FOR_SPPCODE_FNC(c.species_id)) AS spp_common_name
				,c.kept
				,c.discarded
				,c.dealer_num
				,c.date_sold
				,c.port_landed
				,c.state_landed
				,c.port_number
				,GET_RSA_VERIFY_SPP_FNC(GET_NESPP3_FOR_SPPCODE_FNC(C.SPECIES_ID)) as RSA_SPP
        ,get_rsa_verify_program_spp_fnc(GET_NESPP3_FOR_SPPCODE_FNC(C.SPECIES_ID),GET_RSA_PROGRAM_FOR_SPP_FNC(GET_NESPP3_FOR_SPPCODE_FNC(C.SPECIES_ID))) AS rsa_project_spp
				,GET_RSA_PROGRAM_FOR_SPP_FNC(GET_NESPP3_FOR_SPPCODE_FNC(C.SPECIES_ID)) AS RSA_PROGRAM
				,GET_NESPP3_FOR_SPPCODE_FNC(c.species_id) AS NESPP3
				,CASE WHEN (GET_NESPP3_FOR_SPPCODE_FNC(c.species_id) IN ('081','120','122','123','124','125','147','153','159','240','250','269','512'))  THEN
						GET_STOCKID_FOR_SPPCODE_FNC(GET_NESPP3_FOR_SPPCODE_FNC(c.species_id),i.area)
							ELSE 'NA'
				END stock_id
				,c.kept AS landed_pounds
				,C.KEPT*(GET_DIS_CONV_VTR_FNC(C.SPECIES_ID,C.DEALER_NUM))  as LIVE_POUNDS
				,GET_FMM_FY_RANGE_FNC(GET_FMM_NO_PERMIT_FNC(GET_NESPP3_FOR_SPPCODE_FNC(C.SPECIES_ID)))  AS FISHING_YEAR_RANGE
				,GET_FMP_YR_FNC(d.date_land,GET_NESPP3_FOR_SPPCODE_FNC(C.SPECIES_ID)) AS fmp_year
				,GET_FMM_NO_PERMIT_FNC(GET_NESPP3_FOR_SPPCODE_FNC(C.SPECIES_ID)) AS fmm
				,GET_FMP_CAT_CODE_LIST_ND_PO(d.VESSEL_PERMIT_NUM,d.date_land,GET_SPP_ITIS_NESPP3_FNC(GET_NESPP3_FOR_SPPCODE_FNC(C.SPECIES_ID))) FMP_CAT_CODE_LIST
				,NULL AS fmp_mort_code_list
				,NULL AS vessel_rpt_freq_list
				,NULL AS fmp_mri_list
				,NULL AS fmp_mri_list_wo_hull
				,NULL AS fishery_type
				--,GET_PLAN_NER_FNC(D.DATE_LAND,GET_NESPP3_FOR_SPPCODE_FNC(C.SPECIES_ID),D.VESSEL_PERMIT_NUM) as PLAN_NER
				--,GET_PLAN_NER_CAT_FNC(d.VESSEL_PERMIT_NUM,GET_PLAN_NER_FNC(d.DATE_LAND,GET_NESPP3_FOR_SPPCODE_FNC(C.SPECIES_ID),d.vessel_permit_num),d.DATE_LAND) AS plan_ner_cat
				--,GET_PLAN_NER_YR_FNC(D.DATE_LAND,GET_NESPP3_FOR_SPPCODE_FNC(C.SPECIES_ID)) as PLAN_NER_YEAR
				--,get_plan_ner_fy_range_fnc(get_plan_ner_fnc(d.date_land,get_nespp3_for_sppcode_fnc(c.species_id),d.vessel_permit_num)) AS plan_ner_fy_range
				--,0 AS plan_ner_valid
				,CASE WHEN c.dealer_num IN (1,2,4,5,7,8,99998) THEN 'NON-DEALER'
						 ELSE 'DEALER'
				 END DEALER_TYPE
				,CASE WHEN (c.species_id = 'NC' AND c.dealer_num IS NULL) THEN 'NO CATCH REPORTED'
					  ELSE DECODE (c.dealer_num, 1, 'SEIZED'
									  ,2, 'BAIT'
									  ,3, '?'
									  ,4, 'RETAINED FOR FUTURE SALE'
									  ,5, 'SOLD TO NON-FEDERAL DEALER'  --ask Mark about these not included in his copy
									 ,7, 'LANDED FOR RESEARCH'
									  ,8, 'LUMF, LEGAL SIZE UNMARKETABLE FISH'
									  ,99998, 'HOME CONSUMPTION'
									  ,'SOLD TO FEDERAL DEALER'
				) 
				END AS DISPOSITION_OF_CATCH					  
				,c.dealer_num AS dealer_number
				,CASE WHEN ((c.dealer_num IN (1,2,4,5,7,8,99998)) OR (c.species_id = 'NC' AND c.dealer_num IS NULL)) THEN 'NA'
				   ELSE
				   GET_DEALER_FROM_DNUM_FNC(c.dealer_num,TO_NUMBER(TO_CHAR(c.date_sold,'YYYY'))) 
				   end as dealer_name
         , d.docid as source_level1_key_id
          ,i.imgid as source_level2_key_id
          ,c.catch_id AS source_level3_key_id 
          ,TO_CHAR(d.date_sail, 'DD-MON-YY HH24:MI:SS') AS date_time_sail
          ,TO_CHAR(D.DATE_LAND, 'DD-MON-YY HH24:MI:SS') as DATE_TIME_LAND
          ,D.DOCID||'-'||D.VESSEL_PERMIT_NUM||'-'||GET_NESPP3_FOR_SPPCODE_FNC(C.SPECIES_ID) as DOCID_PERMIT_SPP_TO_MATCH
          ,d.docid||'-'||d.vessel_permit_num||'-'||get_spp_itis_nespp3_fnc(get_nespp3_for_sppcode_fnc(c.species_id)) as docid_permit_sppitis_to_match
          ,case when (get_nespp3_for_sppcode_fnc(c.species_id) in (select distinct r.nespp3 from ref_ivr_species_nespp3 r where r.spp_common_name like 'SKATE%'))
            then  nvl(d.docid,0)||'-'||d.vessel_permit_num||'-'||'564' 
            else  nvl(d.docid,0)||'-'||d.vessel_permit_num||'-'||substr(get_spp_itis_nespp3_fnc(get_nespp3_for_sppcode_fnc(c.species_id)),1,3)
            end docid_permit_sppitis3_to_match
          ,d.docid as rtl_trip_id
          ,'DOCID' as RTL_TRIP_ID_SOURCE
          ,get_spp_itis_nespp3_fnc(get_nespp3_for_sppcode_fnc(c.species_id)) species_itis
        FROM noaa.document d
				,noaa.images i
				,noaa.catch c
				WHERE d.vessel_id is not null
                AND d.docid=i.docid
				AND i.imgid=c.imgid
				and d.docid in (select distinct v.docid from vw_rsa_trip_docids v)
				AND TO_DATE(d.date_land) >= TO_DATE(SYSDATE - 730);

	COMMIT;

    UPDATE rsa_rtl_vessel
    SET RSA_SPP = 'N'
    WHERE rsa_spp IS NULL;
    COMMIT;

/*UPDATE rsa_rtl_vessel
    set rsa_program = 'NONE'
    where rsa_program is null;
    COMMIT;*/
  
  ---------------
  --  UPDATE PROJECT CODES
  -------------------------
UPDATE RSA_RTL_DEALER
SET RSA_IVR_PROJECT_ID=GET_RSA_IVR_PROJECT_ID_FNC(DOCID,VTRSERNO,NESPP3)
WHERE RSA_IVR_PROJECT_ID IS NULL
AND VTRSERNO IS NOT NULL;
COMMIT;

UPDATE RSA_RTL_DEALER
SET RSA_SFD_PROJECT_ID=GET_RSA_SFD_PROJECT_ID_FNC(DOCID,VTRSERNO,NESPP3)
WHERE RSA_SFD_PROJECT_ID IS NULL
AND VTRSERNO IS NOT NULL;
COMMIT;

UPDATE RSA_RTL_VESSEL
SET RSA_IVR_PROJECT_ID=GET_RSA_IVR_PROJECT_ID_FNC(DOCID,SERIAL_NUM,NESPP3)
WHERE RSA_IVR_PROJECT_ID IS NULL
AND SERIAL_NUM IS NOT NULL;
COMMIT;

UPDATE RSA_RTL_VESSEL
SET RSA_SFD_PROJECT_ID=GET_RSA_SFD_PROJECT_ID_FNC(DOCID,SERIAL_NUM,NESPP3)
where rsa_sfd_project_id is null
and serial_num is not null;
commit;
------------------------------------------------
   FSO_ADMIN.LOG_EVENT (VBATCHPROCESS, VMODULENAME, VPROCEDURENAME, irtlrsaseq,'SUCCESSFUL', '2 of 7 Procedures - '|| VPROCEDURENAME ||'-- Successfully finished procedure.' ,null,null,null,null, ILOGID);		
EXCEPTION
    WHEN OTHERS THEN
    errmsg := errmsg || ' SQL Error: ' || SQLERRM;
    -- rtl_set_run_status(irtlrsaseq, 'ABORT', -1, errmsg);
    FSO_ADMIN.LOG_EVENT (VBATCHPROCESS, VMODULENAME, VPROCEDURENAME, irtlrsaseq,'FAILED', SQLERRM ,null,null,null,null, ILOGID);		
    DBMS_OUTPUT.PUT_LINE(VPROCEDURENAME || 'finished abnormally'||ERRMSG);
-----------------------------------------------  
 --  log_event (vbatchprocess, vmodulename, vprocedurename, irtlrsaseq, NULL, vprocedurename ||' - Match Column Check Errors = '||v_check_after_cnt||' -- Successfully finished procedure.' ,vtablename,NULL,NULL,NULL, ilogid);		
--EXCEPTION
  --  WHEN OTHERS THEN
   --     errmsg := errmsg || ' SQL Error: ' || SQLERRM;
   --     rtl_set_run_status(irtlrsaseq, 'ABORT', -1, errmsg);
    --    log_event ( vbatchprocess, vmodulename, vprocedurename, irtlrsaseq, NULL, vprocedurename || ' finished abnormally - '||errmsg,NULL,NULL,NULL,NULL, ilogid );
    --    DBMS_OUTPUT.PUT_LINE(VPROCEDURENAME || 'finished abnormally'||ERRMSG);
END;