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



void outputH(IPHeader* iph_, TCPHeader* tcph_){
    struct in_addr srcaddr, destaddr;
    srcaddr.s_addr = iph_->src_ip;
    destaddr.s_addr = iph_->dest_ip;
    printf("%s(%d) --> %s(%d)\n", (char *)inet_ntoa(srcaddr), ntohs(tcph_->src_port), 
        (char*)inet_ntoa(destaddr), ntohs(tcph_->dest_port));
}


void pcap_cb(u_char* args, const struct pcap_pkthdr* header, const u_char* packet){
    uc* pkt_ = (uc*)(packet + 14);
    if(header->len > 84){
        //swm->insertPkt(pkt, header->len - 14);
        int pkt_length_ = header->len - 14;
        IPHeader* iph = (IPHeader*)pkt_;
        if(iph->protocol != 17){
            return;
        }
        us ip_header_length = ((us)(iph->ver_ihl & 0xF)) << 2;
        us ip_total_length = ntohs(iph->total_len);

        TCPHeader * tcph = (TCPHeader*)(pkt_ + ip_header_length);
        us tcp_header_length = ((tcph->offset >> 4) & 0x0F) << 2;
        if(tcp_header_length + ip_header_length == ip_total_length){ // this tcp has no payload
            return;
        }

        us payload_length = ip_total_length - ip_header_length - tcp_header_length;
        char* payload = (char*)(tcph + tcp_header_length);
        outputH(iph, tcph);
        printf("pkt len = %d\n", pkt_length_);
        for(int i=0; i < payload_length; i++){
            if(payload[i] > 0 && payload[i] < 128) printf("%c", payload[i]);
            else printf(".");
        }
        printf("\n\n");
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
    printf("begin to sniff\n");
    pcap_loop(handle, -1, pcap_cb, NULL);
    pcap_close(handle);
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
    }else{
        swm->processPkt();
    }
    return 0;
}