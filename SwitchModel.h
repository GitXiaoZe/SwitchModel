#ifndef SWITCHMODEL_H
#define SWITCHMODEL_H

#include <unistd.h>
#include <memory>
#include <map>
#include <utility>
#include <cstdio>
#include <cassert>
#include <cerrno>
#include <cstring>
#include <algorithm>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <net/if.h>
#include <net/ethernet.h>
#include <sys/wait.h>
#include <linux/netfilter.h>
#include <linux/if_ether.h>
#include <linux/if_packet.h>
#include <libnetfilter_queue/libnetfilter_queue.h>


#include "myType.h"
#include "Job.h"
#include "Packet.h"
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
            job2TaskSet =  std::make_shared< std::map<ul, std::map<ui, ui>* > >();
            iport2Packet = std::make_shared< std::map<ul, Packet*> >();
            waitingToProcess = 
                std::make_shared< CP_Queue< std::pair<uc*, int>* > >();
            waitingToFetch = std::make_shared< CP_Queue<ui> >();
            waitingToConfigure = std::make_shared< CP_Queue<ui> >();
            waitingMapResult = 
                std::make_shared< CP_Queue< std::pair<ul, ui>* > >();
            waitingToSet = 
                std::make_shared< CP_Queue< std::pair<ul, ui>* > >();
            waitingToSchedule = std::make_shared< CP_Queue<ui> >();
            strmatcher = std::make_shared<StrMatcher>();
        }
        ~SwitchModel(){}
        ul str2JobId(char* job_id_, ui job_id_length_);
        ui getJobIdx(char* job_id_, ui job_id_length_, bool& create);
        ui createJob(char* job_id_, ui job_id_length_, ul jobId_);
        Packet* getPacket(ul iport_);
        void removePacket(ul iport_);
        void parsePacket(uc* pkt_, int pkt_length_);
        void sniff_socket();
        void insertPkt(uc* pkt, int pkt_length_);
        void processPkt();
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
        std::shared_ptr< std::map<ul, Packet*> > iport2Packet;
        std::shared_ptr< std::map<ul, ui> > iport2nextSeq;
        std::shared_ptr< std::map<ui, Job*> > idx2JobPtr;
        std::shared_ptr< std::map<ul, ui> > jobId2idx;
        std::shared_ptr< std::map<ul, std::map<ui, ui>* > > job2TaskSet;

        std::shared_ptr< CP_Queue< std::pair<uc*, int>* > > waitingToProcess;
        std::shared_ptr< CP_Queue<ui> > waitingToFetch;
        std::shared_ptr< CP_Queue<ui> > waitingToConfigure;

        std::shared_ptr< CP_Queue< std::pair<ul, ui>* > > waitingMapResult;
        std::shared_ptr< CP_Queue< std::pair<ul, ui>* > > waitingToSet;
        std::shared_ptr< CP_Queue<ui> > waitingToSchedule;

        std::shared_ptr<StrMatcher> strmatcher;
};

#endif // !SWITCHMODEL_H