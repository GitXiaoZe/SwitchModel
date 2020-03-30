#include <sys/types.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <linux/types.h>
#include <linux/netfilter.h>
#include <libnetfilter_queue/libnetfilter_queue.h>
#include <pcap.h>

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


//interception (nfqueue)
int nfq_cb(struct nfq_q_handle* qh, struct nfgenmsg* nfmsg, struct nfq_data* nfa, void* data){
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

    qh = nfq_create_queue(h, QUEUE_NUM, &nfq_cb, NULL);
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

//sniffing (pcap)


#define DEVICE "eth1"
#define PKT_LENGTH 65536 
#define BUFSIZE 4096


void pcap_cb(u_char* args, const struct pcap_pkthdr* header, const u_char* packet){
    uc* pkt = (uc*)(packet + 14);
    if(header->len > 84){
        swm->insertPkt(pkt, header->len - 14);
    }
}

int pcap_sniff(){
    char errbuf[BUFSIZE];
    pcap_t* handle;
    struct bpf_program fp;
    bpf_u_int32 mask, net;
    if(pcap_lookupnet(DEVICE, &net, &mask, errbuf) < 0){
        printf("pcap lookup error: %s\n", errbuf);
        exit(-1);
    }else{
        struct in_addr netaddr, maskaddr;
        netaddr.s_addr = net;
        maskaddr.s_addr = mask;
        printf("net : %s, mask : %s\n", (char *)inet_ntoa(netaddr), (char*)inet_ntoa(maskaddr));
    }
    if( (handle = pcap_open_live(DEVICE, PKT_LENGTH, true, 0, errbuf)) == NULL){
        printf("Error : %s\n", errbuf);
        exit(-1);
    }

    char* filterExp = "greater 100";
    if(pcap_compile(handle, &fp, filterExp, 0, net) < 0){
        printf("Error pcap compile\n");
        exit(-1);
    }

    if(pcap_setfilter(handle, &fp) < 0){
        printf("Error set filter\n");
        exit(-1);
    }

    printf("begin to sniff\n");
    pcap_loop(handle, -1, pcap_cb, NULL);
    pcap_close(handle);
}


#include <iostream>

#define CAPFILE "/home/hzh/Documents/NFQueue/filecap.txt.3"
#define PKT_LENGTH 65536
char filepkt[PKT_LENGTH << 1];
char PKT[PKT_LENGTH];
int file_sniff(){
    printf("start file sniff\n");
    freopen(CAPFILE, "r", stdin);
    struct pcap_pkthdr header;
    while(std::cin.getline(filepkt, PKT_LENGTH)){
        int len = strlen(filepkt);
        char v;
        for(int i=0; i < len; i+=2){
            v = 0;
            if(filepkt[i] >= '0' && filepkt[i] <= '9'){
                v += filepkt[i] - '0';
            }else if(filepkt[i] >= 'a' && filepkt[i] <= 'f'){
                v += filepkt[i] - 'a' + 10;
            }
            v <<= 4;
            int j = i+1;
            if(filepkt[j] >= '0' && filepkt[j] <= '9'){
                v += filepkt[j] - '0';
            }else if(filepkt[j] >= 'a' && filepkt[j] <= 'f'){
                v += filepkt[j] - 'a' + 10;
            }
            PKT[i>>1] = v;
        }
        header.len = header.caplen = len >> 1;
        const u_char* pointer = (const u_char*)PKT;
        pcap_cb(NULL, &header, pointer);
        sleep(1);
    }
    printf("Press Ctrl + C to exist\n");
    while(true);
}



int main(){
    printf("begin to initial swm\n");
    swm = new SwitchModel();
    if(swm == NULL){
        printf("swm is null\n");
        exit(-1);
    }
    if(1){
        printf("begin to start()\n");
        sleep(1);
        std::thread subThread_1(schedule_thread, swm);
        std::thread subThread_2(setReducerSize_thread, swm);
        std::thread subThread_3(fetchMapTaskResult_thread, swm);
        std::thread subThread_4(configureForJob_thread, swm);
        std::thread subThread_5(fetchConfigureFileForJob_thread, swm);
        std::thread subThread_6(process_thread, swm);
        sleep(1);
        pcap_sniff();
        //file_sniff();
    }else{
        swm->processPkt();
    }
    return 0;
}