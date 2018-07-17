#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>
#include <fcntl.h>
#include <unistd.h>

#include "ProcessingUnit.h"

ProcessingUnit::ProcessingUnit(int num_of_cu, string sched_type,
		DataLoader *dataLoader) {
	this->numOfComputeUnits = num_of_cu;
	setSchedApproach(sched_type);
	this->dataLoader = dataLoader;
}

ProcessingUnit::~ProcessingUnit() {
	for (CoreHandler<Query>* curHandler : coreHandlers)
		delete curHandler;
#ifdef __sun
	for(DaxHandler<Query>* daxHandler : daxHandlers)
	delete daxHandler;
#endif

	delete sharedQueue.getHead();
	delete swQueue.getHead();
	delete hwQueue.getHead();

	for (ScanApi *curScan : factScanAPIs)
		delete curScan;
	for (ScanApi *curScan : dimScanAPIs)
		delete curScan;
	for (JoinApi *curJoin : joinAPIs)
		delete curJoin;
	delete aggAPI;
}

void ProcessingUnit::createProcessingUnit(Syncronizer *thr_sync, int dax_queue_size) {
	for (int i = 0; i < numOfComputeUnits; i++) {
		CoreHandler<Query> *handler = new CoreHandler<Query>(thr_sync, sched_approach, i);
		//handler->setId(i);
		handler->setQueues(&sharedQueue, &swQueue, &hwQueue);
		handler->setAPIs(factScanAPIs, dimScanAPIs, joinAPIs, aggAPI);
		coreHandlers.push_back(handler);
	}
//#ifdef __sun
	DaxHandler<Query> *daxHandler = new DaxHandler<Query>(thr_sync, dax_queue_size, true, sched_approach, 63);
	//daxHandler->setId(63);
	daxHandler->setQueues(&sharedQueue, &swQueue, &hwQueue);
	daxHandler->setAPIs(factScanAPIs, dimScanAPIs, joinAPIs, aggAPI);
	daxHandlers.push_back(daxHandler);
//#endif
}

void ProcessingUnit::addScanItems(int total_parts, JOB_TYPE scan_job, int sf,
		WorkQueue<Query> *queue) {
	printf("Adding for table (id: %d), # of parts: %d\n", scan_job,
			total_parts);

	for (int i = 0; i < 1 * sf; i++) {
		for (int p_id = 0; p_id < total_parts; p_id++) {
			Query item(0, p_id, scan_job); //(0: sw - 1:hw, part_id, table_id)
			Node<Query> *newNode = new Node<Query>(item);
			queue->add(newNode);
		}
	}
	queue->printQueue();
}

void ProcessingUnit::initWorkQueues(int sf) {
	JOB_TYPE scan_type;
	if (sched_approach == EXEC_TYPE::SDB || sched_approach == EXEC_TYPE::OAT) {
		for(ScanApi *dimScan : dimScanAPIs){
			int num_of_parts = dimScan->getBaseTable()->NumOfParts();
			scan_type = dimScan->Type();
			this->addScanItems(num_of_parts, scan_type, sf, getSharedQueue());
		}

		for(ScanApi *factScan : factScanAPIs){
			int num_of_parts = factScan->getBaseTable()->NumOfParts();
			scan_type = factScan->Type();
			this->addScanItems(num_of_parts, scan_type, sf, getSharedQueue());
		}

		/*
		DataCompressor *factComp = dataLoader->getFactTableCompressor();
		this->addScanItems(factComp->getNumOfParts(), LO_SCAN, sf, getSharedQueue());

		if(factScanAPIs.size() == 2){
			this->addScanItems(factComp->getNumOfParts(), LO_SCAN_2, sf, getSharedQueue());
		}*/
	}
	else if (sched_approach == EXEC_TYPE::DD) {
		int totalFactScans = this->totalFactParts();
		int totalDimScans = this->totalDimParts();

		int totalScans = totalFactScans + totalDimScans;

		int totalDaxScans = totalScans / (2.56 + 1) * 2.56;
		printf("Dax will do %d scans out of %d in total!\n", totalDaxScans, totalScans);

		//Add all dimension scans to the HWQueue
		for(ScanApi *dimScan : dimScanAPIs){
			int num_of_parts = dimScan->getBaseTable()->NumOfParts();
			scan_type = dimScan->Type();
			this->addScanItems(num_of_parts, scan_type, sf, getHWQueue());
			totalDaxScans -= num_of_parts;
		}

		cout << "    Remaining Dax Scans: " << totalDaxScans << endl;

		for(ScanApi *factScan : factScanAPIs){
			int num_of_parts = factScan->getBaseTable()->NumOfParts();
			scan_type = factScan->Type();

			if(totalDaxScans == 0){ //add all to the SW Queue
				this->addScanItems(num_of_parts, scan_type, sf, getSWQueue());
			}
			else if(totalDaxScans < num_of_parts){
				this->addScanItems(totalDaxScans, scan_type, sf, getHWQueue());

				for (int part_id = totalDaxScans ; part_id < num_of_parts; part_id++) {
					Query item(0, part_id, scan_type);
					Node<Query> *newNode = new Node<Query>(item);
					getSWQueue()->add(newNode);
				}

				totalDaxScans = 0;
			}
			else if(totalDaxScans == num_of_parts){ //add all to the HW Queue
				this->addScanItems(num_of_parts, scan_type, sf, getHWQueue());
				totalDaxScans -= num_of_parts;
			}
			else if(totalDaxScans > num_of_parts){
				this->addScanItems(num_of_parts, scan_type, sf, getHWQueue());
				totalDaxScans -= num_of_parts;
			}
		}

		printf("DAX Scan Queue\n");
		getHWQueue()->printQueue();
		printf("Core Scan Queue\n");
		getSWQueue()->printQueue();
	}
}

void ProcessingUnit::initializeAPI(ParserApi &parser) {
	table *fact_t = dataLoader->getFactTableCompressor()->getTable();
	ssb_table &parsed_fact_t = parser.FactRelation();

	ScanApi *factScanAPI;
	if(parsed_fact_t.hasFilter){
		for(size_t p_id = 0; p_id < parsed_fact_t.q_preds.size(); p_id++){
			query_predicate &cur_pred = parsed_fact_t.q_preds[p_id];
			int selId_f = cur_pred.columnId;
			if(p_id == 0){
				factScanAPI = new ScanApi(fact_t, selId_f, LO_SCAN, cur_pred);
				factScanAPIs.push_back(factScanAPI);
			}
			else if(p_id == 1){
				factScanAPI = new ScanApi(fact_t, selId_f, LO_SCAN_2, cur_pred);
				factScanAPIs.push_back(factScanAPI);

				factScanAPIs[0]->setSubScan(factScanAPIs[1]);
			}
		}
	}
	else{
		factScanAPI = new ScanApi(fact_t, LO_SCAN);
		factScanAPIs.push_back(factScanAPI);
	}

	table *dim_t;
	int dim_t_id;
	int join_colId;
	JOB_TYPE scan_type, join_type;

	ScanApi *dimScan;
	JoinApi *join;
	for(auto &curPair : parser.DimRelations()){
		ssb_table &parsed_dim_t = curPair.second;
		dim_t = dataLoader->getDimTableCompressors()[curPair.first]->getTable();

		dim_t_id = parsed_dim_t.t_id;
		scan_type = parser.ScanType(dim_t_id);

		if(parsed_dim_t.hasFilter){
			for(size_t p_id = 0; p_id < parsed_dim_t.q_preds.size(); p_id++){
				query_predicate &cur_pred = parsed_dim_t.q_preds[p_id];
				int selId_d = cur_pred.columnId;
				if(p_id == 0){
					dimScan = new ScanApi(dim_t, selId_d, scan_type, cur_pred);
					dimScanAPIs.push_back(dimScan);
				}
				else if(p_id == 1){ //ssb query 1.3
					scan_type = D_SCAN_2;
					dimScan = new ScanApi(dim_t, selId_d, scan_type, cur_pred);
					dimScanAPIs.push_back(dimScan);
			    }
			}
		}
		else{
			dimScan = new ScanApi(dim_t, scan_type);
			dimScanAPIs.push_back(dimScan);
		}

		join_type = parser.JoinType(dim_t_id);
		join_colId = parsed_fact_t.joinColumnIds[dim_t_id];
		join = new JoinApi(fact_t, dimScan, join_colId, join_type);
		joinAPIs.push_back(join);
	}
	aggAPI = new AggApi(factScanAPIs[0], joinAPIs);
}

void ProcessingUnit::startThreads(TCPStream* connection) {
	for (CoreHandler<Query> *curHandler : coreHandlers) {
		curHandler->setStream(connection);
		curHandler->start(curHandler->getId(), false);
	}
#ifdef __sun
	for(DaxHandler<Query> *daxHandler : daxHandlers) {
		daxHandler->setStream(connection);
		daxHandler->start(daxHandler->getId(), true);
	}
#endif
}

void ProcessingUnit::joinThreads() {
#ifdef __sun
	for(DaxHandler<Query>* daxHandler : daxHandlers) {
		daxHandler->join();
	}
#endif
	for (CoreHandler<Query>* curHandler : coreHandlers) {
		curHandler->join();
	}
}

void ProcessingUnit::writeResults() {
	FILE *sscan_f = fopen("logs/sw_scan.txt", "w+");
	FILE *dscan_f = fopen("logs/dax_scan.txt", "w+");
	FILE *sjoin_f = fopen("logs/sw_join.txt", "w+");
	FILE *djoin_f = fopen("logs/dax_join.txt", "w+");
	FILE *agg_rt_f = fopen("logs/agg_runtime.txt", "w+");

	//FILE *count_f = fopen("count.txt", "a");

	std::unordered_map<std::string, FILE*> all_files { { "SW_SCAN", sscan_f }, {
			"DAX_SCAN", dscan_f }, { "SW_JOIN", sjoin_f },
			{ "DAX_JOIN", djoin_f }, { "AGG", agg_rt_f } };

	map<int, int> count_results {
		{ LO_SCAN, 0 }, { LO_SCAN_2, 0 }, { S_SCAN, 0 }, { C_SCAN, 0 }, { P_SCAN, 0 }, { D_SCAN_2, 0 }, { D_SCAN, 0 },
		{ LS_JOIN, 0 }, { LC_JOIN, 0 }, { LP_JOIN, 0 }, { LD_JOIN, 0 } };
	map<int, uint64_t> agg_results;

	for (CoreHandler<Query>* curHandler : coreHandlers) {
		Result result = curHandler->getResult();

		result.writeRuntimeResultsToFile(all_files);
		result.mergeCountResults(count_results);
		result.mergeAggResults(agg_results);
	}

	Result result = daxHandlers[0]->getResult();
	result.writeRuntimeResultsToFile(all_files);
	result.mergeCountResults(count_results);

	Result::writeCountsToFile(count_results);
	Result::writeAggResultsToFile(agg_results);

	fclose(sscan_f);
	fclose(dscan_f);
	fclose(sjoin_f);
	fclose(sjoin_f);
	fclose(agg_rt_f);
}
