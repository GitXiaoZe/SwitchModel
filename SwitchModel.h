#ifndef SWITCHMODEL_H
#define SWITCHMODEL_H

#include <memory>
#include <map>
#include <utility>
#include <cstdio>
#include <cassert>
#include <algorithm>
#include <arpa/inet.h>
#include <sys/wait.h>
#include <linux/netfilter.h>
#include <libnetfilter_queue/libnetfilter_queue.h>

#include "myType.h"
#include "Job.h"
#include "CPQueue.h"
#include "StrMatcher.h"

#include "tinxml/tinyxml.h"

class SwitchModel{
    public :
        SwitchModel() : job_index(0) {
            iport2idx = std::make_shared< std::map<ul, ui> >();
            iport2nextSeq = std::make_shared< std::map<ul ,ui> >();
            idx2JobPtr = std::make_shared< std::map<ui, Job*> >();
            jobId2idx = std::make_shared< std::map<ul, ui> >();

            waitingToFetch = std::make_shared< CP_Queue<ui> >();
            waitingToConfigure = std::make_shared< CP_Queue<ui> >();
            waitingMapResult = 
                std::make_shared< CP_Queue< std::pair<ul, ui> > >();
            waitingToSet = std::make_shared< CP_Queue< std::pair<ul, ui> > >();
            waitingToSchedule = std::make_shared< CP_Queue<ui> >();
            strmatcher = std::make_shared<StrMatcher>();
        }
        ~SwitchModel(){}
        ui getJobIdx(ul iport_, char* job_id_, ui job_id_length_);
        ui createJob(ul iport_, char* job_id_, ui job_id_length_, ul jobId_);
        void parsePacket(struct nfq_q_handle* qh_, struct nfq_data* nfa_, unsigned char* pkt_, int pkt_length_);
        void fetchCongiureFileForJob();
        void configureForJob();
        void fetchMapTaskResult();
        void setReducerSize();
        void schedule();

        int sendPkt(struct nfq_q_handle* qh_, struct nfq_data* data_);
        int rejectPkt(struct nfq_q_handle* qh_, struct nfq_data* data_);
        ui job_index;
        //std::map<ul, ui> iport2idx; //???
        //std::map<ui, Job*> idx2JobPtr;
        //std::map<ul, ui> iport2nextSeq; //???
        std::shared_ptr< std::map<ul, ui> > iport2idx;
        std::shared_ptr< std::map<ul, ui> > iport2nextSeq;
        std::shared_ptr< std::map<ui, Job*> > idx2JobPtr;
        std::shared_ptr< std::map<ul, ui> > jobId2idx;

        std::shared_ptr< CP_Queue<ui> > waitingToFetch;
        std::shared_ptr< CP_Queue<ui> > waitingToConfigure;

        std::shared_ptr< CP_Queue< std::pair<ul, ui> > > waitingMapResult;
        std::shared_ptr< CP_Queue< std::pair<ul, ui> > > waitingToSet;
        std::shared_ptr< CP_Queue<ui> > waitingToSchedule;

        std::shared_ptr<StrMatcher> strmatcher;
};

#endif // !SWITCHMODEL_H