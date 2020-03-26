#include <sys/types.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <linux/types.h>
#include <linux/netfilter.h>
#include <libnetfilter_queue/libnetfilter_queue.h>

#include <cstdlib>
#include <thread>

#include "SwitchModel.h"




#define QUEUE_NUM 0
#define BUFSIZE 4096

SwitchModel* swm = NULL;

void fetchConfigureFileForJob_thread(SwitchModel* swm_){
    swm_->fetchCongiureFileForJob();
}

void configureForJob_thread(SwitchModel* swm_){
    swm_->configureForJob();
}

void fetchMapTaskResult_thread(SwitchModel* swm_){
    swm_->fetchMapTaskResult();
}

void setReducerSize_thread(SwitchModel* swm_){
    swm_->setReducerSize();
}

void schedule_thread(SwitchModel* swm_){
    swm_->schedule();
}

void process_thread(SwitchModel* swm_){
    swm->processPkt();
}

void sniff_thread(SwitchModel* swm_){
    swm_->sniff();
}

int cb(struct nfq_q_handle* qh, struct nfgenmsg* nfmsg, struct nfq_data* nfa, void* data){
    //printf("call back\n");
    int payload_len;
    unsigned char* payload;
    payload_len = nfq_get_payload(nfa, &payload);
    if(payload_len > 0){
        swm->insertPkt(payload, payload_len);
    }
    swm->sendPkt(qh, nfa);
    return 0;
}

void start(){
    struct nfq_handle* h;
    struct nfq_q_handle* qh;
    int fd;
    int rv;
    char buf[BUFSIZE];
    h = nfq_open();
    if(nfq_unbind_pf(h, AF_INET) < 0){
        printf("error during nfq_unbind\n");
        exit(-1);
    }

    if(nfq_bind_pf(h, AF_INET) < 0){
        printf("error during nfq_bind\n");
        exit(-1);
    }

    qh = nfq_create_queue(h, QUEUE_NUM, &cb, NULL);
    if(!qh){
        printf("error during nfq_create_queue\n");
        exit(-1);
    }

    if(nfq_set_mode(qh, NFQNL_COPY_PACKET, 0xFFFF) < 0){
        printf("error during nfq_set_mode\n");
        exit(-1);
    }
    
    fd = nfq_fd(h);
    printf("begin to receive\n");
    while((rv = recv(fd, buf, sizeof(buf), 0))){
        nfq_handle_packet(h, buf, rv);
    }
    printf("ending!!!!!!!!! rv = %d\n\n", rv);
    nfq_destroy_queue(qh);
    nfq_close(h);
}


int main(){
    printf("begin to initial swm\n");
    swm = new SwitchModel();
    printf("begin to start()\n");
    std::thread subThread_1(schedule_thread, swm);
    std::thread subThread_2(setReducerSize_thread, swm);
    std::thread subThread_3(fetchMapTaskResult_thread, swm);
    std::thread subThread_4(configureForJob_thread, swm);
    std::thread subThread_5(fetchConfigureFileForJob_thread, swm);
    std::thread subThread_6(process_thread, swm);
    std::thread subThread_7(sniff_thread, swm);
    start();

    return 0;
}