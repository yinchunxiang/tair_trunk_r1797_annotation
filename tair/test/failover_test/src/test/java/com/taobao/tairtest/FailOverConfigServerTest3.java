/**
 * 
 */
package com.taobao.tairtest;

import static org.junit.Assert.*;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class FailOverConfigServerTest3 extends FailOverBaseCase {
	@Test
	public void testFailover_01_restart_master_cs()
	{
		log.info("start config test Failover case 01");
		int waitcnt=0;
		//start cluster
		controlCluster(csList, dsList, start, 0);
		log.info("Start Cluster Successful!");
		log.info("wait system initialize ...");
		waitto(down_time);

		//change test tool's configuration
		if(!modify_config_file(local, test_bin+toolconf, actiontype, put))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, datasize, put_count))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, filename, kv_name))
			fail("modify configure file failed");

		//
		execute_data_verify_tool();

		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}

		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate samll than 90%!",datacnt/put_count_float>0.9);
		log.info("Write data over!");		


		//close master cs
		if(!control_cs(csList.get(0), stop, 0))fail("close master cs failed!");

		log.info("master cs has been closed!");

		waitto(down_time);

		//change test tool's configuration
		if(!modify_config_file(local, test_bin+toolconf, actiontype, get))
			fail("modify configure file failed");	
		//read data from cluster
		execute_data_verify_tool();
		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}	
		log.info("Read data over!");
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;

		//verify get result
		assertEquals("get data verify failed!", datacnt, getVerifySuccessful());
		log.info("Successfully Verified data!");

		//restart master cs
		if(!control_cs(csList.get(0), start, 0))fail("restart master cs failed!");
		log.info("Restart master cs successful!");

		waitto(down_time);

		//read data from cluster
		execute_data_verify_tool();
		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		log.info("Read data over!");
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;

		//verify get result
		assertEquals("get data verify failed!", datacnt, getVerifySuccessful());
		log.info("Successfully verified data!");
		//end test
		log.info("end config test Failover case 01");
	}
	@Test
	public void testFailover_02_restart_slave_cs()
	{
		log.info("start config test Failover case 02");
		int waitcnt=0;
		//start cluster
		controlCluster(csList, dsList, start, 0);
		log.info("Start Cluster Successful!");
		log.info("wait system initialize ...");
		waitto(down_time);

		//change test tool's configuration
		if(!modify_config_file(local, test_bin+toolconf, actiontype, put))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, datasize, put_count))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, filename, kv_name))
			fail("modify configure file failed");

		//
		execute_data_verify_tool();

		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate samll than 90%!",datacnt/put_count_float>0.9);
		log.info("Write data over!");		

		//close slave cs
		if(!control_cs(csList.get(1), stop, 0))fail("close slave cs failed!");

		log.info("slave cs has been closed!");

		waitto(2);

		//change test tool's configuration
		if(!modify_config_file(local, test_bin+toolconf, actiontype, get))
			fail("modify configure file failed");	
		//read data from cluster
		execute_data_verify_tool();
		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}	
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;
		log.info("Read data over!");

		//verify get result
		assertEquals("verify data failed!", datacnt,getVerifySuccessful());
		log.info("Successfully Verified data!");

		//restart slave cs
		if(!control_cs(csList.get(1), start, 0))fail("restart slave cs failed!");
		log.info("Restart slave cs successful!");

		//read data from cluster
		execute_data_verify_tool();
		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		log.info("Read data over!");
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;

		//verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("Successfully verified data!");
		//end test
		log.info("end config test Failover case 02");
	}
	@Test
	public void testFailover_03_restart_all_cs()
	{
		log.info("start config test Failover case 03");
		int waitcnt=0;
		//start cluster
		controlCluster(csList, dsList, start, 0);
		log.info("Start Cluster Successful!");

		log.info("wait system initialize ...");
		waitto(down_time);

		//change test tool's configuration
		if(!modify_config_file(local, test_bin+toolconf, actiontype, put))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, datasize, put_count))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, filename, kv_name))
			fail("modify configure file failed");

		//
		execute_data_verify_tool();

		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate samll than 90%!",datacnt/put_count_float>0.9);
		log.info("Write data over!");		


		//close all cs
		if(!batch_control_cs(csList, stop, 0))fail("close all cs failed!");

		log.info("all cs has been closed! wait for 2 seconds to restart ...");

		waitto(2);

		//restart all cs
		if(!batch_control_cs(csList, start, 0))fail("restart all cs failed!");
		log.info("Restart all cs successful!");

		//change test tool's configuration
		if(!modify_config_file(local, test_bin+toolconf, actiontype, get))
			fail("modify configure file failed");

		//read data from cluster
		execute_data_verify_tool();
		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		log.info("Read data over!");
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;

		//verify get result
		assertEquals("verify data failed", datacnt, getVerifySuccessful());
		log.info("Successfully verified data!");
		//end test
		log.info("end config test Failover case 03");
	}
	@Test
	public void testFailover_04_recover_master_cs_and_ds_before_rebuild()
	{
		log.info("start config test Failover case 04");
		int waitcnt=0;
		//start cluster
		controlCluster(csList, dsList, start, 0);

		log.info("Start Cluster Successful!");

		log.info("wait system initialize ...");
		waitto(down_time);

		//change test tool's configuration
		if(!modify_config_file(local, test_bin+toolconf, actiontype, put))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, datasize, put_count))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, filename, kv_name))
			fail("modify configure file failed");

		//
		execute_data_verify_tool();

		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}

		log.info("Write data over!");		

		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;

		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate samll than 90%!",datacnt/put_count_float>0.9);
		log.info("finish put data!");

		//close master cs
		if(!control_cs(csList.get(0), stop, 0))fail("close master cs failed!");
		log.info("master cs has been closed!");

		waitto(2);

		//close ds
		if(!control_ds(dsList.get(0), stop, 0))fail("close ds failed!");
		log.info("ds has been closed!");
		log.info("wait 5 seconds to restart before rebuild ...");

		waitto(5);

		if(check_keyword(csList.get(1), start_migrate, tair_bin+"logs/config.log")==0)fail("Already migration!");
		//restart ds
		if(!control_ds(dsList.get(0), start, 0))fail("restart ds failed!");
		log.info("Restart ds successful!");	

		//restart master cs
		if(!control_cs(csList.get(0), start, 0))fail("restart master cs failed!");
		log.info("Restart master cs successful!");

		//change test tool's configuration
		if(!modify_config_file(local, test_bin+toolconf, actiontype, get))
			fail("modify configure file failed");	

		//read data from cluster
		execute_data_verify_tool();
		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;	
		log.info("Read data over!");

		//verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("Successfully Verified data!");	

		//end test
		log.info("end config test Failover case 04");
	}
	@Test
	public void testFailover_05_recover_master_cs_when_ds_down_and_rebuild()
	{
		log.info("start config test Failover case 05");
		int waitcnt=0;
		//start cluster
		controlCluster(csList, dsList, start, 0);

		log.info("Start Cluster Successful!");

		log.info("wait system initialize ...");
		waitto(down_time);

		//change test tool's configuration
		if(!modify_config_file(local, test_bin+toolconf, actiontype, put))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, datasize, put_count))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, filename, kv_name))
			fail("modify configure file failed");

		//
		execute_data_verify_tool();

		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;

		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate samll than 90%!",datacnt/put_count_float>0.9);
		log.info("Write data over!");		


		//close master cs
		if(!control_cs(csList.get(0), stop, 0))fail("close master cs failed!");
		log.info("master cs has been closed!");

		waitto(2);

		//close ds
		if(!control_ds(dsList.get(0), stop, 0))fail("close ds failed!");
		log.info("ds has been closed!");

		waitto(down_time);

		//check migration stat
		while(check_keyword(csList.get(1), start_migrate, tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration start on cs "+csList.get(1)+" log");
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("donw time arrived,but no migration started!");
		waitcnt=0;
		log.info("donw time arrived,migration start!");

		//restart master cs
		if(!control_cs(csList.get(0), start, 0))fail("restart master cs failed!");
		log.info("Restart master cs successful!");

		//wait for system restart to work
		waitto(down_time);

		//check migration stat
		while(check_keyword(csList.get(0), finish_migrate, tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finish on cs "+csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("donw time arrived,but no migration finished!");
		waitcnt=0;
		log.info("donw time arrived,migration finished!");

		//change test tool's configuration
		if(!modify_config_file(local, test_bin+toolconf, actiontype, get))
			fail("modify configure file failed");	

		//read data from cluster
		execute_data_verify_tool();
		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		log.info("Read data over!");
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;	

		//verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("Successfully Verified data!");	

		//end test
		log.info("end config test Failover case 05");
	}
	@Test
	public void testFailover_06_recover_master_cs_after_ds_down_and_rebuild()
	{
		log.info("start config test Failover case 06");
		int waitcnt=0;
		//start cluster
		controlCluster(csList, dsList, start, 0);

		log.info("Start Cluster Successful!");

		log.info("wait system initialize ...");
		waitto(down_time);

		//change test tool's configuration
		if(!modify_config_file(local, test_bin+toolconf, actiontype, put))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, datasize, put_count))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, filename, kv_name))
			fail("modify configure file failed");

		//
		execute_data_verify_tool();

		//check verify
		while(check_process(local, toolname)!=2)
		{
			try {
				Thread.sleep(2000);
			} catch (Exception e) {
				log.info("sleep exception", e);
			}
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate samll than 90%!",datacnt/put_count_float>0.9);
		log.info("Write data over!");		

		//close master cs
		if(!control_cs(csList.get(0), stop, 0))fail("close master cs failed!");
		log.info("master cs has been closed!");

		waitto(2);

		//close ds
		if(!control_ds(dsList.get(0), stop, 0))fail("close ds failed!");
		log.info("ds has been closed!");

		waitto(down_time);

		//check migration stat
		while(check_keyword(csList.get(1), finish_migrate, tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finished on cs "+csList.get(1)+" log");
			waitto(2);
			if(++waitcnt>210)break; //add time for 5 minites to 7 minites
		}
		if(waitcnt>210)fail("donw time arrived,but no migration started or finished!");
		waitcnt=0;
		log.info("donw time arrived,migration finished!");

		//restart master cs
		if(!control_cs(csList.get(0), start, 0))fail("restart master cs failed!");
		log.info("Restart master cs successful!");

		//wait for system restart to work
		waitto(down_time);

		//change test tool's configuration
		if(!modify_config_file(local, test_bin+toolconf, actiontype, get))
			fail("modify configure file failed");	

		//read data from cluster
		execute_data_verify_tool();
		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		log.info("Read data over!");
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;	

		//verify get result
		assertEquals("verify data failed!", datacnt,getVerifySuccessful());
		log.info("Successfully Verified data!");	

		//end test
		log.info("end config test Failover case 06");
	}
	@Test
	public void testFailover_07_recover_master_cs_when_ds_down_and_rebuild_then_ds_join_again_generate_second_rebuild()
	{
		log.info("start config test Failover case 07");
		int waitcnt=0;
		//start cluster
		controlCluster(csList, dsList, start, 0);

		log.info("Start Cluster Successful!");

		log.info("wait system initialize ...");
		waitto(down_time);

		//change test tool's configuration
		if(!modify_config_file(local, test_bin+toolconf, actiontype, put))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, datasize, put_count))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, filename, kv_name))
			fail("modify configure file failed");

		//
		execute_data_verify_tool();

		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate samll than 90%!",datacnt/put_count_float>0.9);
		log.info("Write data over!");		

		//close master cs
		if(!control_cs(csList.get(0), stop, 0))fail("close master cs failed!");
		log.info("master cs has been closed!");

		waitto(2);

		//close ds
		if(!control_ds(dsList.get(0), stop, 0))fail("close ds failed!");
		log.info("ds has been closed!");

		waitto(down_time);

		//check migration stat
		while(check_keyword(csList.get(1), finish_migrate, tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finished on cs "+csList.get(1)+" log");
			waitto(2);
			if(++waitcnt>210)break;//add weit time from 5 minites to 7 minites
		}
		if(waitcnt>210)fail("donw time arrived,but no migration started or finished!");
		waitcnt=0;
		log.info("donw time arrived,migration finished!");

		//restart ds
		if(!control_ds(dsList.get(0), start, 0))fail("restart ds failed!");
		log.info("restart ds successful!");
		//touch conf file
		touch_file(csList.get(1), tair_bin+groupconf);
		log.info("touch group.conf");

		waitto(down_time);

		//check second migration stat
		while(check_keyword(csList.get(1), start_migrate, tair_bin+"logs/config.log")!=2)
		{
			log.debug("check if migration start on cs "+csList.get(1)+" log");
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("donw time arrived,but no migration started!");
		waitcnt=0;
		log.info("donw time arrived,migration started!");

		//restart master cs
		if(!control_cs(csList.get(0), start, 0))fail("restart master cs failed!");
		log.info("Restart master cs successful!");

		//wait for system restart to work
		waitto(down_time);

		//change test tool's configuration
		if(!modify_config_file(local, test_bin+toolconf, actiontype, get))
			fail("modify configure file failed");	

		//read data from cluster
		execute_data_verify_tool();
		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;	
		log.info("Read data over!");

		//verify get result
		assertEquals("verify data failed!", datacnt,getVerifySuccessful());
		log.info("Successfully Verified data!");	

		//end test
		log.info("end config test Failover case 07");
	}
	@Test
	public void testFailover_08_recover_master_cs_after_ds_down_and_rebuild_then_ds_join_again_generate_second_rebuild()
	{
		log.info("start config test Failover case 08");
		int waitcnt=0;
		//start cluster
		controlCluster(csList, dsList, start, 0);

		log.info("Start Cluster Successful!");

		log.info("wait system initialize ...");
		waitto(down_time);

		//change test tool's configuration
		if(!modify_config_file(local, test_bin+toolconf, actiontype, put))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, datasize, put_count))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, filename, kv_name))
			fail("modify configure file failed");

		//
		execute_data_verify_tool();

		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate samll than 90%!",datacnt/put_count_float>0.9);		
		log.info("Write data over!");		

		//close master cs
		if(!control_cs(csList.get(0), stop, 0))fail("close master cs failed!");
		log.info("master cs has been closed!");

		waitto(2);

		//close ds
		if(!control_ds(dsList.get(0), stop, 0))fail("close ds failed!");
		log.info("ds has been closed!");

		waitto(down_time);

		//check migration stat
		while(check_keyword(csList.get(1), finish_migrate, tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finished on cs "+csList.get(1)+" log");
			waitto(2);
			if(++waitcnt>210)break;
		}
		if(waitcnt>210)fail("donw time arrived,but no migration started or finished!");
		waitcnt=0;
		log.info("donw time arrived,migration finished!");

		//restart ds
		if(!control_ds(dsList.get(0), start, 0))fail("restart ds failed!");
		log.info("restart ds successful!");
		//touch conf file
		touch_file(csList.get(1), tair_bin+groupconf);
		log.info("touch group.conf");

		//wait down time for touch
		waitto(down_time);

		//check second migration stat
		while(check_keyword(csList.get(1), finish_migrate, tair_bin+"logs/config.log")!=2)
		{
			log.debug("check if migration stop on cs "+csList.get(1)+" log");
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("donw time arrived,but no migration started or stoped!");
		waitcnt=0;
		log.info("donw time arrived,migration stoped!");

		//restart master cs
		if(!control_cs(csList.get(0), start, 0))fail("restart master cs failed!");
		log.info("Restart master cs successful!");

		//wait for system restart to work
		log.info("wait system initialize ...");
		waitto(down_time);

		//change test tool's configuration
		if(!modify_config_file(local, test_bin+toolconf, actiontype, get))
			fail("modify configure file failed");	

		//read data from cluster
		execute_data_verify_tool();
		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		log.info("Read data over!");
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;	

		//verify get result
		assertEquals("verify data failed!", datacnt,getVerifySuccessful());
		log.info("Successfully Verified data!");	

		//end test
		log.info("end config test Failover case 08");
	}
	@Test
	public void testFailover_09_shutdown_master_cs_and_slave_cs_one_by_one_then_recover_one_by_one()
	{
		log.info("start config test Failover case 09");
		int waitcnt=0;
		//start cluster
		controlCluster(csList, dsList, start, 0);
		log.info("start cluster successfull!");

		log.info("wait system initialize ...");
		waitto(down_time);

		//change test tool's configuration
		if(!modify_config_file(local, test_bin+toolconf, actiontype, put))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, datasize, put_count))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, filename, kv_name))
			fail("modify configure file failed");

		//
		execute_data_verify_tool();

		//check verify
		while(check_process(local, toolname)!=2)
		{
			try {
				Thread.sleep(2000);
			} catch (Exception e) {
				log.info("sleep exception", e);
			}
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate samll than 90%!",datacnt/put_count_float>0.9);	
		log.info("Write data over!");		

		//stop master cs
		if(!control_cs(csList.get(0), stop, 0))fail("stop master cs failed!");
		log.info("stop master cs successful!");

		//wait 2x(down time) to restart,make sure original slave cs become new master cs
		waitto(down_time*2);

		//restart master cs
		if(!control_cs(csList.get(0),start,0))fail("restart master cs failed!");
		log.info("restart master cs successful!");
		//wait down time for master cs work
		waitto(down_time);

		//change test tool's configuration
		if(!modify_config_file(local, test_bin+toolconf, actiontype, get))
			fail("modify configure file failed");	

		//read data from cluster
		execute_data_verify_tool();
		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		log.info("Read data over!");
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;	

		//verify get result
		assertEquals("verify data failed!", datacnt,getVerifySuccessful());
		log.info("Successfully Verified data!");	

		//stop slave cs
		if(!control_cs(csList.get(1), stop, 0))fail("stop slave cs failed!");
		log.info("stop slave cs successful!");

		//wait 2x(down time) to restart,make sure original slave cs become new master cs
		waitto(down_time*2);

		//change test tool's configuration
		if(!modify_config_file(local, test_bin+toolconf, actiontype, get))
			fail("modify configure file failed");	

		//read data from cluster
		execute_data_verify_tool();
		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		log.info("Read data over!");
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;	

		//verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("Successfully Verified data!");

		//restart slave cs
		if(!control_cs(csList.get(1),start,0))fail("restart slave cs failed!");
		log.info("restart slave cs successful!");
		//wait down time for master cs work
		waitto(down_time);

		//change test tool's configuration
		if(!modify_config_file(local, test_bin+toolconf, actiontype, get))
			fail("modify configure file failed");	

		//read data from cluster
		execute_data_verify_tool();
		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;	
		log.info("Read data over!");

		//verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("Successfully Verified data!");

		//stop master cs again
		if(!control_cs(csList.get(0), stop, 0))fail("stop master cs failed!");
		log.info("stop master cs successful!");

		//wait 2x(down time) to restart,make sure original slave cs become new master cs
		waitto(down_time*2);

		//stop slave cs again
		if(!control_cs(csList.get(1), stop, 0))fail("stop slave cs failed!");
		log.info("stop slave cs successful!");

		//keep all cs down for down_time seconds
		waitto(down_time);

		//restart slave cs again
		if(!control_cs(csList.get(1),start,0))fail("restart slave cs failed!");
		log.info("restart slave cs successful!");

		//restart master cs again
		if(!control_cs(csList.get(0),start,0))fail("restart master cs failed!");
		log.info("restart master cs successful!");
		//wait down time for master cs work
		waitto(down_time);

		//change test tool's configuration
		if(!modify_config_file(local, test_bin+toolconf, actiontype, get))
			fail("modify configure file failed");	

		//read data from cluster
		execute_data_verify_tool();
		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		log.info("Read data over!");
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;	

		//verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("Successfully Verified data!");	

		log.info("end config test Failover case 09");
	}
	@Test
	public void testFailover_10_recover_slave_cs_and_ds_before_rebuild()
	{
		log.info("start config test Failover case 10");
		int waitcnt=0;
		//start cluster
		controlCluster(csList, dsList, start, 0);
		log.info("Start Cluster Successful!");
		log.info("wait system initialize ...");
		waitto(down_time);

		//change test tool's configuration
		if(!modify_config_file(local, test_bin+toolconf, actiontype, put))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, datasize, put_count))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, filename, kv_name))
			fail("modify configure file failed");

		//
		execute_data_verify_tool();

		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate samll than 90%!",datacnt/put_count_float>0.9);	
		log.info("Write data over!");		

		//close slave cs
		if(!control_cs(csList.get(1), stop, 0))fail("close slave cs failed!");
		log.info("slave cs has been closed!");

		waitto(2);

		//close ds
		if(!control_ds(dsList.get(0), stop, 0))fail("close ds failed!");
		log.info("ds has been closed!");
		log.info("wait 5 seconds to restart before rebuild ...");
		waitto(5);

		if(check_keyword(csList.get(0), start_migrate, tair_bin+"logs/config.log")!=1)fail("Already migration!");
		//restart ds
		if(!control_ds(dsList.get(0), start, 0))fail("restart ds failed!");
		log.info("Restart ds successful!");	

		//restart slave cs
		if(!control_cs(csList.get(1), start, 0))fail("restart slave cs failed!");
		log.info("Restart slave cs successful!");

		//change test tool's configuration
		if(!modify_config_file(local, test_bin+toolconf, actiontype, get))
			fail("modify configure file failed");	

		//read data from cluster
		execute_data_verify_tool();
		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		log.info("Read data over!");
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;	

		//verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("Successfully Verified data!");	

		//end test
		log.info("end config test Failover case 10");
	}
	@Test
	public void testFailover_11_recover_slave_cs_when_ds_down_and_rebuild()
	{
		log.info("start config test Failover case 11");
		int waitcnt=0;
		//start cluster
		controlCluster(csList, dsList, start, 0);
		log.info("Start Cluster Successful!");
		log.info("wait system initialize ...");
		waitto(down_time);

		//change test tool's configuration
		if(!modify_config_file(local, test_bin+toolconf, actiontype, put))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, datasize, put_count))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, filename, kv_name))
			fail("modify configure file failed");

		//
		execute_data_verify_tool();

		//check verify
		while(check_process(local, toolname)!=2)
		{
			try {
				Thread.sleep(2000);
			} catch (Exception e) {
				log.info("sleep exception", e);
			}
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate samll than 90%!",datacnt/put_count_float>0.9);	
		log.info("Write data over!");		


		//close slave cs
		if(!control_cs(csList.get(1), stop, 0))fail("close slave cs failed!");
		log.info("slave cs has been closed!");

		waitto(2);

		//close ds
		if(!control_ds(dsList.get(0), stop, 0))fail("close ds failed!");
		log.info("ds has been closed!");

		waitto(down_time);

		//check migration stat
		while(check_keyword(csList.get(0), start_migrate, tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration start on cs "+csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("donw time arrived,but no migration started!");
		waitcnt=0;
		log.info("donw time arrived,migration start!");

		//restart slave cs
		if(!control_cs(csList.get(1), start, 0))fail("restart slave cs failed!");
		log.info("Restart slave cs successful!");

		//wait for system restart to work
		log.info("wait system initialize ...");
		waitto(down_time);

		//check migration stat
		while(check_keyword(csList.get(0), finish_migrate, tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finish on cs "+csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("donw time arrived,but no migration finished!");
		waitcnt=0;
		log.info("donw time arrived,migration finished!");

		//change test tool's configuration
		if(!modify_config_file(local, test_bin+toolconf, actiontype, get))
			fail("modify configure file failed");	

		//read data from cluster
		execute_data_verify_tool();
		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;	
		log.info("Read data over!");

		//verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("Successfully Verified data!");	

		//end test
		log.info("end config test Failover case 11");
	}
	@Test
	public void testFailover_12_recover_slave_cs_after_ds_down_and_rebuild()
	{
		log.info("start config test Failover case 12");
		int waitcnt=0;
		//start cluster
		controlCluster(csList, dsList, start, 0);
		log.info("Start Cluster Successful!");
		log.info("wait system initialize ...");
		waitto(down_time);

		//change test tool's configuration
		if(!modify_config_file(local, test_bin+toolconf, actiontype, put))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, datasize, put_count))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, filename, kv_name))
			fail("modify configure file failed");

		//
		execute_data_verify_tool();

		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate samll than 90%!",datacnt/put_count_float>0.9);	
		log.info("Write data over!");		

		//close slave cs
		if(!control_cs(csList.get(1), stop, 0))fail("close slave cs failed!");
		log.info("slave cs has been closed!");

		waitto(2);

		//close ds
		if(!control_ds(dsList.get(0), stop, 0))fail("close ds failed!");
		log.info("ds has been closed!");

		waitto(down_time);

		//check migration stat
		while(check_keyword(csList.get(0), finish_migrate, tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finished on cs "+csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("donw time arrived,but no migration started or finished!");
		waitcnt=0;
		log.info("donw time arrived,migration finished!");

		//restart slave cs
		if(!control_cs(csList.get(1), start, 0))fail("restart slave cs failed!");
		log.info("Restart slave cs successful!");

		//wait for system restart to work
		log.info("wait system initialize ...");
		waitto(down_time);

		//change test tool's configuration
		if(!modify_config_file(local, test_bin+toolconf, actiontype, get))
			fail("modify configure file failed");	

		//read data from cluster
		execute_data_verify_tool();
		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		log.info("Read data over!");
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;	

		//verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("Successfully Verified data!");	

		//end test
		log.info("end config test Failover case 12");
	}
	@Test
	public void testFailover_13_recover_slave_cs_when_ds_down_and_rebuild_then_ds_join_again_generate_second_rebuild()
	{
		log.info("start config test Failover case 13");
		int waitcnt=0;
		//start cluster
		controlCluster(csList, dsList, start, 0);
		log.info("Start Cluster Successful!");
		log.info("wait system initialize ...");
		waitto(down_time);

		//change test tool's configuration
		if(!modify_config_file(local, test_bin+toolconf, actiontype, put))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, datasize, put_count))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, filename, kv_name))
			fail("modify configure file failed");

		//
		execute_data_verify_tool();

		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate samll than 90%!",datacnt/put_count_float>0.9);	
		log.info("Write data over!");		

		//close slave cs
		if(!control_cs(csList.get(1), stop, 0))fail("close slave cs failed!");
		log.info("slave cs has been closed!");

		waitto(2);

		//close ds
		if(!control_ds(dsList.get(0), stop, 0))fail("close ds failed!");
		log.info("ds has been closed!");

		waitto(down_time);

		//check migration stat
		while(check_keyword(csList.get(0), finish_migrate, tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finished on cs "+csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>150)break;
		}
		log.info("donw time arrived,migration finished!");
		if(waitcnt>150)fail("donw time arrived,but no migration started or finished!");
		waitcnt=0;

		//restart ds
		if(!control_ds(dsList.get(0), start, 0))fail("restart ds failed!");
		log.info("restart ds successful!");
		//touch conf file
		touch_file(csList.get(0), tair_bin+groupconf);
		log.info("touch group.conf");
		waitto(down_time);

		//check second migration stat
		while(check_keyword(csList.get(0), start_migrate, tair_bin+"logs/config.log")!=2)
		{
			log.debug("check if migration start on cs "+csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("donw time arrived,but no migration started!");
		waitcnt=0;
		log.info("donw time arrived,migration started!");

		//restart slave cs
		if(!control_cs(csList.get(1), start, 0))fail("restart slave cs failed!");
		log.info("Restart slave cs successful!");

		//wait for system restart to work
		log.info("wait system initialize ...");
		waitto(down_time);

		//change test tool's configuration
		if(!modify_config_file(local, test_bin+toolconf, actiontype, get))
			fail("modify configure file failed");	

		//read data from cluster
		execute_data_verify_tool();
		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;	
		log.info("Read data over!");
		//verify get result
		assertEquals("verify data failed!", datacnt,getVerifySuccessful());
		log.info("Successfully Verified data!");	

		//end test
		log.info("end config test Failover case 13");
	}
	@Test
	public void testFailover_14_recover_slave_cs_after_ds_down_and_rebuild_then_ds_join_again_generate_second_rebuild()
	{
		log.info("start config test Failover case 14");
		int waitcnt=0;
		//start cluster
		controlCluster(csList, dsList, start, 0);
		log.info("Start Cluster Successful!");
		log.info("wait system initialize ...");
		waitto(down_time);

		//change test tool's configuration
		if(!modify_config_file(local, test_bin+toolconf, actiontype, put))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, datasize, put_count))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, filename, kv_name))
			fail("modify configure file failed");

		//
		execute_data_verify_tool();

		//check verify
		while(check_process(local, toolname)!=2)
		{
			try {
				Thread.sleep(2000);
			} catch (Exception e) {
				log.info("sleep exception", e);
			}
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate samll than 90%!",datacnt/put_count_float>0.9);	
		log.info("Write data over!");		

		//close slave cs
		if(!control_cs(csList.get(1), stop, 0))fail("close slave cs failed!");
		log.info("slave cs has been closed!");

		waitto(2);

		//close ds
		if(!control_ds(dsList.get(0), stop, 0))fail("close ds failed!");
		log.info("ds has been closed!");

		//change master cs cause down time by 2
		waitto(down_time*2);

		//check migration stat
		while(check_keyword(csList.get(0), finish_migrate, tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finished on cs "+csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("donw time arrived,but no migration started or finished!");
		waitcnt=0;
		log.info("donw time arrived,migration finished!");

		//restart ds
		if(!control_ds(dsList.get(0), start, 0))fail("restart ds failed!");
		log.info("restart ds successful!");
		//touch conf file
		touch_file(csList.get(0), tair_bin+groupconf);
		log.info("touch group.conf");

		//wait down time for touch
		waitto(down_time);

		//check second migration stat
		while(check_keyword(csList.get(0), finish_migrate, tair_bin+"logs/config.log")!=2)
		{
			log.debug("check if migration stop on cs "+csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("donw time arrived,but no migration started or stoped!");
		waitcnt=0;
		log.info("donw time arrived,migration stoped!");

		//restart slave cs
		if(!control_cs(csList.get(1), start, 0))fail("restart slave cs failed!");
		log.info("Restart slave cs successful!");

		//wait for system restart to work
		log.info("wait system initialize ...");
		waitto(down_time);

		//change test tool's configuration
		if(!modify_config_file(local, test_bin+toolconf, actiontype, get))
			fail("modify configure file failed");	

		//read data from cluster
		execute_data_verify_tool();
		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		log.info("Read data over!");
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;	

		//verify get result
		assertEquals("verify data failed!", datacnt,getVerifySuccessful());
		log.info("Successfully Verified data!");	

		//end test
		log.info("end config test Failover case 14");
	}
	@Test
	public void testFailover_15_shutdown_master_cs_ds_and_slave_cs_one_by_one_then_recover_one_by_one()
	{
		log.info("start config test Failover case 15");
		int waitcnt=0;
		//start cluster
		controlCluster(csList, dsList, start, 0);
		log.info("start cluster successfull!");
		log.info("wait system initialize ...");
		waitto(down_time);

		//change test tool's configuration
		if(!modify_config_file(local, test_bin+toolconf, actiontype, put))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, datasize, put_count))
			fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, filename, kv_name))
			fail("modify configure file failed");

		//
		execute_data_verify_tool();

		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate samll than 90%!",datacnt/put_count_float>0.9);	
		log.info("Write data over!");		

		//stop master cs
		if(!control_cs(csList.get(0), stop, 0))fail("stop master cs failed!");
		log.info("stop master cs successful!");
		//stop ds
		if(!control_ds(dsList.get(0), stop, 0))fail("stop ds failed!");
		log.info("stop ds sucessful!");

		//wait migration start
		waitto(down_time);

		//check migration stat
		while(check_keyword(csList.get(1), start_migrate, tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration started on cs "+csList.get(1)+" log");
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("donw time arrived,but no migration started!");
		waitcnt=0;
		log.info("donw time arrived,migration started!");

		//restart master cs
		if(!control_cs(csList.get(0),start,0))fail("restart master cs failed!");
		log.info("restart master cs successful!");

		//stop slave cs
		if(!control_cs(csList.get(1),stop,0))fail("close slave cs failed!");
		log.info("stop slave cs successful!");

		//wait down time for master cs work
		waitto(down_time);

		//check migration stat
		while(check_keyword(csList.get(0), finish_migrate, tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finished on cs "+csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("donw time arrived,but no migration started or finished!");
		waitcnt=0;
		log.info("donw time arrived,migration finished!");

		//change test tool's configuration
		if(!modify_config_file(local, test_bin+toolconf, actiontype, get))
			fail("modify configure file failed");	

		//read data from cluster
		execute_data_verify_tool();
		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		log.info("Read data over!");
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;	

		//verify get result
		assertEquals("verify data failed!", datacnt,getVerifySuccessful());
		log.info("Successfully Verified data!");	

		//restart slave cs
		if(!control_cs(csList.get(1), start, 0))fail("restart slave cs failed!");
		log.info("restart slave cs successful!");

		//restart ds
		if(!control_ds(dsList.get(0), start, 0))fail("restart ds failed!");
		log.info("restart ds successful!");
		//touch for ds join
		touch_file(csList.get(0), tair_bin+groupconf);

		//wait down time
		waitto(down_time);

		//check migration stat
		while(check_keyword(csList.get(0), start_migrate, tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration started on cs "+csList.get(0)+" log");
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("donw time arrived,but no migration started or finished!");
		waitcnt=0;
		log.info("donw time arrived,migration started!");

		//stop master cs
		if(!control_cs(csList.get(0), stop, 0))fail("stop master cs failed!");
		log.info("stop master cs successful!");
		//wait to down time
		waitto(down_time);

		//check migration stat
		while(check_keyword(csList.get(1), finish_migrate, tair_bin+"logs/config.log")!=1)
		{
			log.debug("check if migration finished on cs "+csList.get(1)+" log");
			waitto(3);
			if(++waitcnt>200)break;
		}
		if(waitcnt>200)fail("donw time arrived,but no migration started or finished!");
		waitcnt=0;
		log.info("donw time arrived,migration finished!");

		//restart master cs
		if(!control_cs(csList.get(0), start, 0))fail("restart master cs failed!");
		log.info("restart master cs successful!");
		//wait to down time
		waitto(down_time);

		//change test tool's configuration
		if(!modify_config_file(local, test_bin+toolconf, actiontype, get))
			fail("modify configure file failed");	

		//read data from cluster
		execute_data_verify_tool();
		//check verify
		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		log.info("Read data over!");
		if(waitcnt>150)fail("Read data time out!");
		waitcnt=0;	

		//verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("Successfully Verified data!");

		log.info("end config test Failover case 15");
	}
	@Test
	public void testFailover_16_close_master_cs_restart_it_when_add_a_ds_and_begin_migration()
	{
		log.info("start config test Failover case 16!");

		int waitcnt=0;

		//modify group configuration
		if(!comment_line(csList.get(0), tair_bin+groupconf, dsList.get(dsList.size()-1), "#"))fail("change group.conf failed!");
		if(!comment_line(csList.get(1), tair_bin+groupconf, dsList.get(dsList.size()-1), "#"))fail("change group.conf failed!");
		log.info("group.conf has been changed!");

		//start part cluster for next migration step
		controlCluster(csList, dsList.subList(0, dsList.size()-1), start, 0);
		log.info("start cluster successful!");
		//wait down time for cluster to work
		waitto(down_time);

		//write verify data to cluster
		if(!modify_config_file(local, test_bin+toolconf, actiontype, put))fail("modify configure file failed!");
		if(!modify_config_file(local, test_bin+toolconf, datasize, put_count))fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, filename, kv_name))fail("modify configure file failed");
		execute_data_verify_tool();

		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		//verify get result\
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate samll than 90%!",datacnt/put_count_float>0.9);	
		log.info("put data over!");


		//close master cs
		if(!control_cs(csList.get(0),stop, 0))fail("stop master cs failed!");
		log.info("close master cs successful!");

		//start ds
		if(!control_ds(dsList.get(dsList.size()-1), start, 0))fail("start ds failed!");

		//uncomment cs group.conf
		if(!uncomment_line(csList.get(0), tair_bin+groupconf, dsList.get(dsList.size()-1), "#"))fail("change group.conf failed!");
		if(!uncomment_line(csList.get(1), tair_bin+groupconf, dsList.get(dsList.size()-1), "#"))fail("change group.conf failed!");
		//touch group.conf
		touch_file(csList.get(1), tair_bin+groupconf);
		log.info("change group.conf and touch it");
		//wait down time for migration
		waitto(down_time);

		//check migration stat of start
		while(check_keyword(csList.get(1), start_migrate, tair_bin+"logs/config.log")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("check migrate start time out!");
		waitcnt=0;
		log.info("check migrate started!");

		//restart master cs
		if(!control_cs(csList.get(0), start, 0))fail("restart master cs failed!");
		log.info("restart master cs successful!");

		//wait down time for cs change
		waitto(down_time);

		//check migration stat of finish
		while(check_keyword(csList.get(0), finish_migrate, tair_bin+"logs/config.log")!=1)
		{
			waitto(3);
			if(++waitcnt>200)break;
		}
		if(waitcnt>200)fail("check migrate finish time out!");
		waitcnt=0;
		log.info("check migrate finished!");

		//verify data
		if (!modify_config_file(local, test_bin+toolconf, actiontype, get))fail("modify tool config file failed!");
		execute_data_verify_tool();

		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("get data time out!");
		waitcnt=0;

		//verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("Successfully Verified data!");

		log.info("end config test Failover case 16");
	}
	@Test
	public void testFailover_17_close_master_cs_restart_it_after_add_a_ds_and_migration()
	{
		log.info("start config test Failover case 17!");

		int waitcnt=0;

		//modify group configuration
		if(!comment_line(csList.get(0), tair_bin+groupconf, dsList.get(dsList.size()-1), "#"))fail("change group.conf failed!");
		if(!comment_line(csList.get(1), tair_bin+groupconf, dsList.get(dsList.size()-1), "#"))fail("change group.conf failed!");
		log.info("group.conf has been changed!");

		//start part cluster for next migration step
		controlCluster(csList, dsList.subList(0, dsList.size()-1), start, 0);
		log.info("start cluster successful!");
		//wait down time for cluster to work
		waitto(down_time);

		//write verify data to cluster
		if(!modify_config_file(local, test_bin+toolconf, actiontype, put))fail("modify configure file failed!");
		if(!modify_config_file(local, test_bin+toolconf, datasize, put_count))fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, filename, kv_name))fail("modify configure file failed");
		execute_data_verify_tool();

		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate samll than 90%!",datacnt/put_count_float>0.9);	
		log.info("put data over!");

		//close master cs
		if(!control_cs(csList.get(0),stop, 0))fail("stop master cs failed!");
		log.info("close master cs successful!");

		//start ds
		if(!control_ds(dsList.get(dsList.size()-1), start, 0))fail("start ds failed!");

		//uncomment cs group.conf
		if(!uncomment_line(csList.get(0), tair_bin+groupconf, dsList.get(dsList.size()-1), "#"))fail("change group.conf failed!");
		if(!uncomment_line(csList.get(1), tair_bin+groupconf, dsList.get(dsList.size()-1), "#"))fail("change group.conf failed!");
		//touch group.conf
		touch_file(csList.get(1), tair_bin+groupconf);
		log.info("change group.conf and touch it");
		//wait down time for migration
		waitto(down_time);

		//check migration stat of start
		while(check_keyword(csList.get(1), finish_migrate, tair_bin+"logs/config.log")!=1)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("check migrate start time out!");
		waitcnt=0;
		log.info("check migrate finished!");

		//restart master cs
		if(!control_cs(csList.get(0), start, 0))fail("restart master cs failed!");
		log.info("restart master cs successful!");

		//wait down time for cs change
		waitto(down_time);

		//verify data
		if (!modify_config_file(local, test_bin+toolconf, actiontype, get))fail("modify tool config file failed!");
		execute_data_verify_tool();

		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("get data time out!");
		waitcnt=0;

		//verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("Successfully Verified data!");

		log.info("end config test Failover case 17");
	}
	@Test
	public void testFailover_18_close_slave_cs_restart_it_when_add_a_ds_and_begin_migration()
	{
		log.info("start config test Failover case 18!");

		int waitcnt=0;

		//modify group configuration
		if(!comment_line(csList.get(0), tair_bin+groupconf, dsList.get(dsList.size()-1), "#"))fail("change group.conf failed!");
		if(!comment_line(csList.get(1), tair_bin+groupconf, dsList.get(dsList.size()-1), "#"))fail("change group.conf failed!");
		log.info("group.conf has been changed!");

		//start part cluster for next migration step
		controlCluster(csList, dsList.subList(0, dsList.size()-1), start, 0);
		log.info("start cluster successful!");

		//wait down time for cluster to work
		waitto(down_time);

		//write verify data to cluster
		if(!modify_config_file(local, test_bin+toolconf, actiontype, put))fail("modify configure file failed!");
		if(!modify_config_file(local, test_bin+toolconf, datasize, put_count))fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, filename, kv_name))fail("modify configure file failed");
		execute_data_verify_tool();

		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate samll than 90%!",datacnt/put_count_float>0.9);	
		log.info("put data over!");

		//close slave cs
		if(!control_cs(csList.get(1),stop, 0))fail("stop slave cs failed!");
		log.info("close slave cs successful!");

		//start ds
		if(!control_ds(dsList.get(dsList.size()-1), start, 0))fail("start ds failed!");

		//uncomment cs group.conf
		if(!uncomment_line(csList.get(0), tair_bin+groupconf, dsList.get(dsList.size()-1), "#"))fail("change group.conf failed!");
		if(!uncomment_line(csList.get(1), tair_bin+groupconf, dsList.get(dsList.size()-1), "#"))fail("change group.conf failed!");
		//touch group.conf
		touch_file(csList.get(0), tair_bin+groupconf);
		log.info("change group.conf and touch it");
		//wait down time for migration
		waitto(down_time);

		//check migration stat of start
		while(check_keyword(csList.get(0), start_migrate, tair_bin+"logs/config.log")!=1)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("check migrate start time out!");
		waitcnt=0;
		log.info("check migrate started!");

		//restart slave cs
		if(!control_cs(csList.get(1), start, 0))fail("restart slave cs failed!");
		log.info("restart slave cs successful!");

		//wait down time for cs change
		waitto(down_time);

		//check migration stat of finish
		while(check_keyword(csList.get(0), finish_migrate, tair_bin+"logs/config.log")!=1)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("check migrate finish time out!");
		waitcnt=0;
		log.info("check migrate finished!");

		//verify data
		if (!modify_config_file(local, test_bin+toolconf, actiontype, get))fail("modify tool config file failed!");
		execute_data_verify_tool();

		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("get data time out!");
		waitcnt=0;

		//verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("Successfully Verified data!");

		log.info("end config test Failover case 18");
	}
	@Test
	public void testFailover_19_close_slave_cs_restart_it_after_add_a_ds_and_migration()
	{
		log.info("start config test Failover case 19!");

		int waitcnt=0;

		//modify group configuration
		if(!comment_line(csList.get(0), tair_bin+groupconf, dsList.get(dsList.size()-1), "#"))fail("change group.conf failed!");
		if(!comment_line(csList.get(1), tair_bin+groupconf, dsList.get(dsList.size()-1), "#"))fail("change group.conf failed!");
		log.info("group.conf has been changed!");

		//start part cluster for next migration step
		controlCluster(csList, dsList.subList(0, dsList.size()-1), start, 0);
		log.info("start cluster successful!");

		//wait down time for cluster to work
		waitto(down_time);

		//write verify data to cluster
		if(!modify_config_file(local, test_bin+toolconf, actiontype, put))fail("modify configure file failed!");
		if(!modify_config_file(local, test_bin+toolconf, datasize, put_count))fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, filename, kv_name))fail("modify configure file failed");
		execute_data_verify_tool();

		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		//verify get result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate samll than 90%!",datacnt/put_count_float>0.9);	
		log.info("put data over!");

		//close slave cs
		if(!control_cs(csList.get(1),stop, 0))fail("stop slave cs failed!");
		log.info("close slave cs successful!");

		//start ds
		if(!control_ds(dsList.get(dsList.size()-1), start, 0))fail("start ds failed!");

		//uncomment cs group.conf
		if(!uncomment_line(csList.get(0), tair_bin+groupconf, dsList.get(dsList.size()-1), "#"))fail("change group.conf failed!");
		if(!uncomment_line(csList.get(1), tair_bin+groupconf, dsList.get(dsList.size()-1), "#"))fail("change group.conf failed!");
		//touch group.conf
		touch_file(csList.get(0), tair_bin+groupconf);
		log.info("change group.conf and touch it");
		//wait down time for migration
		waitto(down_time);

		//check migration stat of start
		while(check_keyword(csList.get(0), finish_migrate, tair_bin+"logs/config.log")!=1)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("check migrate start time out!");
		waitcnt=0;
		log.info("check migrate finished!");

		//restart slave cs
		if(!control_cs(csList.get(1), start, 0))fail("restart slave cs failed!");
		log.info("restart slave cs successful!");

		//wait down time for cs change
		waitto(down_time);

		//verify data
		if (!modify_config_file(local, test_bin+toolconf, actiontype, get))fail("modify tool config file failed!");
		execute_data_verify_tool();

		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("get data time out!");
		waitcnt=0;

		//verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("Successfully Verified data!");

		log.info("end config test Failover case 19");
	}

	@Test
	public void testFailover_20_migrate_before_add_new_ds()
	{
		log.info("start config test Failover case 20");

		int waitcnt=0;
		if(!comment_line(csList.get(0), tair_bin+groupconf, dsList.get(dsList.size()-1), "#"))fail("change group.conf failed!");
		if(!comment_line(csList.get(1), tair_bin+groupconf, dsList.get(dsList.size()-1), "#"))fail("change group.conf failed!");
		log.info("change group.conf successful!");

		//start part cluster for next migration step
		controlCluster(csList, dsList, start, 0);
		log.info("start cluster successful!");

		//wait down time for cluster to work
		waitto(down_time);

		//write verify data to cluster
		if(!modify_config_file(local, test_bin+toolconf, actiontype, put))fail("modify configure file failed!");
		if(!modify_config_file(local, test_bin+toolconf, datasize, put_count))fail("modify configure file failed");
		if(!modify_config_file(local, test_bin+toolconf, filename, kv_name))fail("modify configure file failed");
		execute_data_verify_tool();

		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("put data time out!");
		waitcnt=0;
		//save put result
		int datacnt=getVerifySuccessful();
		assertTrue("put successful rate samll than 90%!",datacnt/put_count_float>0.9);	
		log.info("put data over!");


		//close a ds
		if(!control_ds(dsList.get(0), stop, 0))fail("stop ds failed!");
		log.info("stop ds successful1");

		waitto(down_time);

		//check migration stat of start
		while(check_keyword(csList.get(0), finish_migrate, tair_bin+"logs/config.log")!=1)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("check migrate start time out!");
		waitcnt=0;
		log.info("check migrate finished!");

		//verify data
		if (!modify_config_file(local, test_bin+toolconf, actiontype, get))fail("modify tool config file failed!");
		execute_data_verify_tool();

		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("get data time out!");
		waitcnt=0;

		//verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("Successfully Verified data!");

		if(!uncomment_line(csList.get(0), tair_bin+groupconf, dsList.get(dsList.size()-1), "#"))fail("change group.conf failed!");
		if(!uncomment_line(csList.get(1), tair_bin+groupconf, dsList.get(dsList.size()-1), "#"))fail("change group.conf failed!");
		log.info("change group.conf successful!");

		waitto(down_time);

		//check migration stat of start
		while(check_keyword(csList.get(0), finish_migrate, tair_bin+"logs/config.log")!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("check migrate start time out!");
		waitcnt=0;
		log.info("check migrate finished!");

		//verify data
		if (!modify_config_file(local, test_bin+toolconf, actiontype, get))fail("modify tool config file failed!");
		execute_data_verify_tool();

		while(check_process(local, toolname)!=2)
		{
			waitto(2);
			if(++waitcnt>150)break;
		}
		if(waitcnt>150)fail("get data time out!");
		waitcnt=0;

		//verify get result
		assertEquals("verify data failed!", datacnt, getVerifySuccessful());
		log.info("Successfully Verified data!");

		log.info("end config test Failover case 20");
	}

	@BeforeClass
	public static void subBeforeClass() {
		log.info("reset copycount while subBeforeClass!");
		FailOverBaseCase fb = new FailOverBaseCase();
		if (!fb.batch_modify(csList, tair_bin + groupconf, copycount, "3"))
			fb.fail("modify configure file failure");
	}

	@Before
	public void subBefore() {
		log.info("clean tool and cluster while subBefore!");
		if (before.equals(clean_option)) {
			clean_tool(local);
			resetCluster(csList, dsList);
			// execute_shift_tool(local, "conf5");//for kdb
			if(!batch_uncomment(csList, tair_bin + groupconf, dsList, "#"))
				fail("batch uncomment ds in group.conf failed!");
			if (!modify_config_file(local, test_bin + toolconf, proxyflag, "0"))
				fail("modify configure file failed!");
		}
	}

	@After
	public void subAfter() {
		log.info("clean tool and cluster while subAfter!");
		if (after.equals(clean_option)) {
			clean_tool(local);
			resetCluster(csList, dsList);
			if(!batch_uncomment(csList, tair_bin + groupconf, dsList, "#"))
				fail("batch uncomment ds in group.conf failed!");
			if (!modify_config_file(local, test_bin + toolconf, proxyflag, "0"))
				fail("modify configure file failed!");
		}
	}
}
