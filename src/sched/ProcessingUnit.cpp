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

	delete factScanAPI;
	for (ScanApi *curScan : dimScanAPIs)
		delete curScan;
	for (JoinApi *curJoin : joinAPIs)
		delete curJoin;
	delete aggAPI;
}

/*
 void ProcessingUnit::addCompressedTables(std::vector<DataCompressor*> &compTables) {
 for(DataCompressor *compTable : compTables){
 dataArr.push_back(compTable);
 printf("comptable id %d\n", compTable->getTable()->t_meta.t_id);
 }
 }*/

void ProcessingUnit::createProcessingUnit(Syncronizer *thr_sync, int dax_queue_size) {
	for (int i = 0; i < numOfComputeUnits; i++) {
		CoreHandler<Query> *handler = new CoreHandler<Query>(thr_sync, sched_approach);
		handler->setId(i);
		handler->setQueues(&sharedQueue, &swQueue, &hwQueue);
		handler->setAPIs(factScanAPI, dimScanAPIs, joinAPIs, aggAPI);
		coreHandlers.push_back(handler);
	}
//#ifdef __sun
	DaxHandler<Query> *daxHandler = new DaxHandler<Query>(thr_sync, dax_queue_size, true, sched_approach);
	daxHandler->setId(63);
	daxHandler->setQueues(&sharedQueue, &swQueue, &hwQueue);
	daxHandler->setAPIs(factScanAPI, dimScanAPIs, joinAPIs, aggAPI);
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
		int t_id = 0;
		for (DataCompressor *dimComp : dataLoader->getDimTableCompressors()) {
			scan_type = dataLoader->ScanType(t_id);
			this->addScanItems(dimComp->getNumOfParts(), scan_type, sf,
					getSharedQueue());
			t_id++;
		}
		DataCompressor *factComp = dataLoader->getFactTableCompressor();
		this->addScanItems(factComp->getNumOfParts(), JOB_TYPE::LO_SCAN, sf,
				getSharedQueue());
	} else if (sched_approach == EXEC_TYPE::DD) {
		int totalScans = 0;
		for (DataCompressor *dimComp : dataLoader->getDimTableCompressors()) {
			totalScans += dimComp->getNumOfParts();
		}
		DataCompressor *factComp = dataLoader->getFactTableCompressor();
		totalScans += factComp->getNumOfParts();

		int totalDaxScans = totalScans / (2.56 + 1) * 2.56;
		printf("Dax will do %d scans out of %d in total!\n", totalDaxScans,
				totalScans);

		//Add all dimension scans to the HWQueue

		int t_id = 0;
		for (DataCompressor *dimComp : dataLoader->getDimTableCompressors()) {
			scan_type = dataLoader->ScanType(t_id);
			this->addScanItems(dimComp->getNumOfParts(), scan_type, sf,
					getHWQueue());
			t_id++;
			totalDaxScans--;
		}

		//Add some fact scans to the HWQueue
		this->addScanItems(totalDaxScans, JOB_TYPE::LO_SCAN, sf, getHWQueue());

		//Finally add all remaining fact scans to the SWQueue
		for (int part_id = totalDaxScans; part_id < factComp->getNumOfParts();
				part_id++) {
			Query item(0, part_id, JOB_TYPE::LO_SCAN);
			Node<Query> *newNode = new Node<Query>(item);
			getSWQueue()->add(newNode);
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
	int selId_f = parsed_fact_t.q_pred.columnId;
	factScanAPI = new ScanApi(fact_t, selId_f, LO_SCAN, parsed_fact_t.q_pred);

	table *dim_t;
	int dim_t_id;
	int selId_d;
	int join_colId;
	JOB_TYPE scan_type, join_type;

	ScanApi *dimScan;
	JoinApi *join;
	for(auto &curPair : parser.DimRelations()){
		ssb_table &parsed_dim_t = curPair.second;
		dim_t = dataLoader->getDimTableCompressors()[curPair.first]->getTable();

		selId_d = curPair.second.q_pred.columnId;
		dim_t_id = curPair.second.t_id;
		scan_type = parser.ScanType(dim_t_id);

		dimScan = new ScanApi(dim_t, selId_d, scan_type, parsed_dim_t.q_pred);
		dimScanAPIs.push_back(dimScan);

		join_type = parser.JoinType(dim_t_id);
		join_colId = parsed_fact_t.joinColumnIds[dim_t_id];
		join = new JoinApi(fact_t, dimScan, join_colId, join_type);
		joinAPIs.push_back(join);
	}
	aggAPI = new AggApi(fact_t, joinAPIs);
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

/*Move this to another class*/
void ProcessingUnit::writeResults() {
	FILE *sscan_f = fopen("sw_scan.txt", "a");
	FILE *dscan_f = fopen("dax_scan.txt", "a");
	FILE *sjoin_f = fopen("sw_join.txt", "a");
	FILE *djoin_f = fopen("dax_join.txt", "a");
	FILE *agg_rt_f = fopen("agg_runtime.txt", "a");

	//FILE *count_f = fopen("count.txt", "a");

	std::unordered_map<std::string, FILE*> all_files { { "SW_SCAN", sscan_f }, {
			"DAX_SCAN", dscan_f }, { "SW_JOIN", sjoin_f },
			{ "DAX_JOIN", djoin_f }, { "AGG", agg_rt_f } };

	map<int, int> count_results { { LO_SCAN, 0 }, { S_SCAN, 0 }, { C_SCAN, 0 },
			{ D_SCAN, 0 }, { LS_JOIN, 0 }, { LC_JOIN, 0 }, { LD_JOIN, 0 } };
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
