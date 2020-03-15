#include <sys/types.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <linux/types.h>
#include <linux/netfilter.h>
#include <libnetfilter_queue/libnetfilter_queue.h>

#include <cstdlib>

#include "SwitchModel.h"




#define QUEUE_NUM 0
#define BUFSIZE 4096

SwitchModel* swm = NULL;

int cb(struct nfq_q_handle* qh, struct nfgenmsg* nfmsg, struct nfq_data* nfa, void* data){
    printf("call back\n");
    int payload_len;
    unsigned char* payload;
    payload_len = nfq_get_payload(nfa, &payload);
    if(payload_len > 0){
        swm->parsePacket(qh, nfa, payload, payload_len);
    }
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
    start();

    return 0;
}