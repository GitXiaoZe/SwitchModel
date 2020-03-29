#include "SwitchModel.h"

#define BUFSIZE 4096

ul SwitchModel::str2JobId(char* job_id_, ui job_id_length_){
    int first = 0, second = 0;
    for(int i=0; i < job_id_length_; i++){
        if(*(job_id_ + i) == '_'){
            if(!first) first = i + 1;
            else second = i + 1;
        }
    }
    return std::atol(job_id_ + first) * 10000 + (ul)std::atol(job_id_ + second);
}

ui SwitchModel::createJob(char* job_id_, ui job_id_length_, ul jobId_){
    std::map<ul, ui>::iterator ite = jobId2idx->find(jobId_);
    ui idx;
    if(jobId2idx->end() == ite){
        idx = job_index++;
        //iport2idx->insert( std::pair<ul, ui>(iport_, idx) );
        idx2JobPtr->insert( std::pair<ui, Job*>(idx, new Job(job_id_, job_id_length_, idx, jobId_)) );
        jobId2idx->insert( std::pair<ul, ui>(jobId_, idx) );
    }else
        idx = ite->second;
    
    return idx;
}

ui SwitchModel::getJobIdx(char* job_id_, ui job_id_length_, bool& create){
    ul jobId = str2JobId(job_id_, job_id_length_);
    create = false;
    std::map<ul, ui>::iterator ite = jobId2idx->find(jobId);
    ui idx;
    if(ite == jobId2idx->end()){
        idx = createJob(job_id_, job_id_length_, jobId);
        create = true;
    }else idx = ite->second;
    return idx;
}

 Packet* SwitchModel::getPacket(ul iport_){
    std::map<ul, Packet*>::iterator ite = iport2Packet->find(iport_);
    Packet* pkt = NULL;
    if(ite == iport2Packet->end()){
        pkt = new Packet();
        iport2Packet->insert(std::pair<ul, Packet*>(iport_, new Packet()));
    }else pkt = ite->second;
    return pkt;
 }

 void SwitchModel::removePacket(ul iport_){
    iport2Packet->erase(iport_);
 }

#define FETCH_COMMAND "sudo -u hzh /home/hzh/Documents/hadoop-3.1.2/bin/hdfs dfs -get /tmp/hadoop-yarn/staging/hzh/.staging/%s/job.xml /home/hzh/Documents/NFQueue/conf/%s_job.xml"

void SwitchModel::fetchCongiureFileForJob(){
    char buf[BUFSIZE];
    ui job_index;
    while(true){
        waitingToFetch->get(job_index);
        Job* job = (*idx2JobPtr)[job_index];
        snprintf(buf, BUFSIZE, FETCH_COMMAND, job->job_id, job->job_id);
        printf("begin to fetch %s in %s\n", job->job_id, buf);
        //system(buf);
        //int res = 0;
        //wait(&res);
        waitingToConfigure->add(job_index);
    }
}

#define CONF_FILE_PATH "/home/hzh/Documents/NFQueue/conf/%s_job.xml"
#define CONF_MAPS "mapreduce.job.maps"
#define CONF_REDUCES "mapreduce.job.reduces"
#define TASKRATIO 0.6

void SwitchModel::configureForJob(){
    char buf[BUFSIZE];
    unsigned int job_index;
    int map, reduce;
    while(true){
        waitingToConfigure->get(job_index);
        Job* job = (*idx2JobPtr)[job_index];
        snprintf(buf, BUFSIZE, CONF_FILE_PATH, job->job_id);
        printf("begin to conf %s in %s\n", job->job_id, buf);
        job->setTask(map, reduce, TASKRATIO);
        /*
        TiXmlDocument* doc = new TiXmlDocument(CONF_FILE_PATH);
        if(!doc->LoadFile()){
            printf("Error msg %s\n", doc->ErrorDesc());
            printf("load file error\n");
        }else{
            TiXmlElement *root = doc->RootElement();
            TiXmlNode *item;
            for(item = root->FirstChild(); item; item = item->NextSibling()){
                TiXmlNode* child = item->FirstChild("name");
                if(child != NULL){
                    if( !std::strcmp(CONF_MAPS, child->FirstChild()->Value()) )
                        map = atoi(child->NextSibling("value")->FirstChild()->Value());
                    else if( !std::strcmp(CONF_REDUCES, child->FirstChild()->Value()) )
                        reduce = atoi(child->NextSibling("value")->FirstChild()->Value());
                }
            }
            job->setTask(map, reduce, TASKRATIO);
        }
        */
    }
}


//%s: password; %s: username;  %s: host;  %s:app id;  %s:task id;  %s:task_id;
//#define FETCH_MAP_RESULT_COMMAND "sshpass -p %s scp %s@%s:/home/tian/Ho/hadoop-3.1.1-src/hadoop-dist/target/hadoop-3.1.1/tmp/nm-local-dir/tian/appcache/%s/output/%s/file.out.index /home/hzh/Documents/NFQueue/conf/%s.file.index.out"
#define FETCH_MAP_RESULT_COMMAND "sshpass -p %s scp %s@%s:/home/hzh/Documents/file.out.index /home/hzh/Documents/NFQueue/conf/%s_%06u.file.out.index"
#define LEN 3

void SwitchModel::fetchMapTaskResult(){
    char buf[BUFSIZE];
    char* password = "..xiao";
    char* username = "hzh";
    char* host="192.168.217.145";
    std::pair<ul, ui> p;
    while(true){
        waitingMapResult->get(p);
        ul job_id = p.first;
        ui task_id = p.second;
        Job *job = (*idx2JobPtr)[(*jobId2idx)[job_id]];
        //snprintf(buf, BUFSIZ, FETCH_MAP_RESULT_COMMAND, password, username, host, app_id, task_id, task_id);
        snprintf(buf, BUFSIZE, FETCH_MAP_RESULT_COMMAND, password, username, host, job->job_id, task_id);
        printf("begin to fetch %s\n", buf);
        //system(buf);
        //int r = 0;  
        //wait(&r);
        waitingToSet->add(p);
    }
}

#define MAP_RESULT_FILE_PATH "/home/hzh/Documents/NFQueue/conf/%s_%06u.file.out.index"
unsigned long convert(unsigned long d){
    unsigned char *str = (unsigned char*)&d;
    return ((unsigned long)str[0] << 56) + ((unsigned long)str[1] << 48) + ((unsigned long)str[2] << 40) + ((unsigned long)str[3] << 32) 
            + ((unsigned long)str[4] << 24) + ((unsigned long)str[5] << 16) + ((unsigned long)str[6] << 8) + ((unsigned long)str[7]);
}

void SwitchModel::setReducerSize(){
    char buf[BUFSIZE];
    std::pair<ul, ui> p;
    while(true){
        waitingToSet->get(p);
        ul job_id = p.first;
        ul task_id = p.second;
        ui idx = (*jobId2idx)[job_id];
        Job *job = (*idx2JobPtr)[idx];
        snprintf(buf, BUFSIZE, MAP_RESULT_FILE_PATH, job->job_id, task_id);
        printf("begin to set task : %s\n" ,buf);
        //FILE* inFile = fopen(buf, "rb");
        //if(inFile == NULL){
        //    printf("Error occurs when opening files %s \n", buf);
        //    continue;//???
        //}
        //ul data[3];
        //for(int i=0; i < job->reduce; i++){
        //    int v = fread(data, sizeof(unsigned long), 3, inFile);
        //    assert(v==3);
        //    job->mapTask_size[i] += convert(data[1]); 
        //}
        //fclose(inFile);
        //job->hasReceived++;
        //if(job->hasReceived == job->map_ratio){
            waitingToSchedule->add(idx);
        //}
    }
}


#define MAX_REDUCER 100
#define ORDER_FILE_PATH "/home/hzh/Documents/NFQueue/conf/%s.file.order"

//%s: password; %s: username;  %s: host;  %s:app id;  %s:task id;  %s:task_id;
//#define FETCH_MAP_RESULT_COMMAND "sshpass -p %s scp %s@%s:/home/tian/Ho/hadoop-3.1.1-src/hadoop-dist/target/hadoop-3.1.1/tmp/nm-local-dir/tian/appcache/%s/output/%s/file.out.index /home/hzh/Documents/NFQueue/conf/%s.file.index.out"
#define SEND_ORDER_COMMAND "sshpass -p %s scp /home/hzh/Documents/NFQueue/conf/%s.file.order %s@%s:/home/hzh/Documents/%s.file.order"


bool compare(const std::pair<ui, ul> &p1, const std::pair<ui, ul> &p2){
    return p1.second > p2.second;
}

void SwitchModel::schedule(){
    char buf[BUFSIZE];
    std::pair<ui, ul> pairs[MAX_REDUCER];
    ui idx;
    char* password;
    char* username;
    char* host;
    while(true){
        waitingToSchedule->get(idx);
        Job* job = (*idx2JobPtr)[idx];
        for(int i=1; i <= job->reduce; i++){
            pairs[i].first = i;
            pairs[i].second = (job->mapTask_size)[i];
        }
        std::sort(pairs + 1, pairs + job->reduce + 1, compare);
        snprintf(buf, BUFSIZE, ORDER_FILE_PATH, job->job_id);
        printf("begin to schedule, write results to %s\n", buf);
        //FILE* outFile = fopen(buf, "w");
        //if(outFile == NULL){
        //    printf("Error when opening file when schedule\n");
        //    continue;
        //}
        //for(int i=1; i <= job->reduce; i++){
        //    fprintf(outFile, "%d ", pairs[i].first);
        //}
        //fclose(outFile);
        snprintf(buf, BUFSIZE, SEND_ORDER_COMMAND, password, job->job_id, username, host, job->job_id);
        printf("transfer results : %s\n", buf);
        //system(buf);
        //int res = 0;
        //wait(&res);
    }
}

int SwitchModel::rejectPkt(struct nfq_q_handle* qh_, struct nfq_data* nfa_){
    int id = 0;
    struct nfqnl_msg_packet_hdr* ph = nfq_get_msg_packet_hdr(nfa_);
    if(ph){
        id = ntohl(ph->packet_id);
    }else{
        printf("Error during reject packet\n");
        return -1;
    }
    return nfq_set_verdict(qh_, id, NF_DROP, 0, NULL);
}

int SwitchModel::sendPkt(struct nfq_q_handle* qh_, struct nfq_data* nfa_){
    int id = 0;
    struct nfqnl_msg_packet_hdr* ph = nfq_get_msg_packet_hdr(nfa_);
    if(ph){
        id = ntohl(ph->packet_id);
    }else{
        printf("Error during send packet\n");
        return -1;
    }
    return nfq_set_verdict(qh_, id, NF_ACCEPT, 0, NULL);
}


#define NICNAME "eth1"

void SwitchModel::sniff_socket(){
    int sock;

    if( (sock = socket(PF_PACKET, SOCK_RAW, htons(ETH_P_IP))) < 0){
        printf("Error-0\n");
        printf("%s\n", strerror(errno));
        exit(-1);
    }

    struct ifreq stlf;
    strcpy(stlf.ifr_name, NICNAME);
    if(ioctl(sock, SIOCGIFFLAGS, &stlf) < 0){
        printf("Error-1\n");
        printf("%s\n", strerror(errno));
        exit(-1);
    }

    stlf.ifr_flags |= IFF_PROMISC;
    if(ioctl(sock, SIOCGIFFLAGS, &stlf) < 0){
        printf("Error-2\n");
        printf("%s\n", strerror(errno));
        exit(-1);
    }
    
    strcpy(stlf.ifr_name, NICNAME);
    if(ioctl(sock, SIOCGIFINDEX, &stlf) < 0){
        printf("Error-3\n");
        printf("%s\n", strerror(errno));
        exit(-1);
    }

    struct sockaddr_ll addr = {0};
    addr.sll_family = PF_PACKET;
    addr.sll_ifindex = stlf.ifr_ifindex;
    addr.sll_protocol = htons(ETH_P_IP);
    if( bind(sock, (struct sockaddr*)&addr, sizeof(addr)) < 0 ){
        printf("Error-4\n");
        printf("%s\n", strerror(errno));
        exit(-1);
    }

    uc buf[BUFSIZE];
    uc* pkt = NULL;
    while(1){
        int cnt = recvfrom(sock, buf, BUFSIZE, 0, NULL, NULL);
        if(cnt < 0) continue;
        if(cnt < 46) continue;
        pkt = new uc[cnt - 14 + 1];
        assert(pkt != NULL);
        std::memcpy(pkt, buf + 14, cnt - 14);
        waitingToProcess->add(std::pair<uc*, int>(pkt, cnt-14));
    }
    close(sock);
}



void SwitchModel::insertPkt(uc* pkt_, int pkt_length_){
    uc* pkt = new uc[pkt_length_];
    if(pkt == NULL) return;
    std::memcpy(pkt, pkt_, pkt_length_);
    waitingToProcess->add(std::pair<uc*, int>(pkt, pkt_length_));
}

#define RPC_UNKNOWN_TYPE 0

#define RPC_SUBMIT_TYPE 1
#define RPC_SUBMIT "submitApplication"

#define RPC_ALLOCATE_TYPE 2
#define RPC_ALLOCATE "allocate"

#define RPC_FINISH_TYPE 3
#define RPC_FINISH "finishApplicationMaster"

#define RPC_GETNEWAPPLICATION_TYPE 4
#define RPC_GETNEWAPPLICATION "getNewApplication"

#define RPC_GETAPPLICATIONREPORT_TYPE 5
#define RPC_GETAPPLICATIONREPORT "getApplicationReport"

#define RPC_STARTCONTAINERS_TYPE 6
#define RPC_STARTCONTAINERS "startContainers"

#define RPC_REGISTERAPPLICATIONMASTER_TYPE 7
#define RPC_REGISTERAPPLICATIONMASTER "registerApplicationMaster"

#define RPC_DONE_TYPE 8
#define RPC_DONE "done"

#define JOB_ID_PREFIX "job_"
#define TASK_ID_PREFIX "attempt_"
#define MAPFLAG "MAP"
#define REDUCEFLAG "REDUCE"

#define MRAPPMASTER "MRAppMaster"
#define MRAPPMASTER_TYPE 1 

#define YARNCHILD "YarnChild"
#define YARNCHILD_TYPE 2
#define TCP 0x6


/*
    unsigned char ip_header_length = (iph->ver_ihl) & 0xF;
    printf("version = %u\n", ((ui)(iph->ver_ihl) & 0xF0) >> 4);
    printf("ihl = %u\n", (ui)(iph->ver_ihl) & 0xF);
    printf("tos = %u\n", (ui)(iph->tos));
    printf("total_len = %u\n", (ui)ntohs((iph->total_len)));
    printf("identification = %u\n", (ui)ntohs((iph->identification)));
    printf("flag-offset = %u\n", (ui)ntohs((iph->flag_offset)));
    printf("ttl = %u\n", (ui)(iph->ttl));
    printf("protocol = %u\n", (ui)(iph->protocol));
    printf("checksum = %u\n", (ui)(iph->checksum));
    printf("source ip = %u\n", (ui)ntohl((iph->src_ip)));
    printf("dest ip = %u\n", (ui)ntohl((iph->dest_ip)));
    printf("\n\n");
    
    uc ip_header_length = ((iph->ver_ihl) & 0x0F) << 2;
    us ip_total_length = ntohs(iph->total_len);
    printf("header-len = %u total-len = %u\n\n", (ui)ip_header_length, (ui)ip_total_length);
    if(ip_header_length == ip_total_length){
        printf("No TCPHeader\n");
        printf("---------------------------\n");
    }else{
        TCPHeader * tcph = (TCPHeader*)(pkt_ + ip_header_length);
        printf("src-port = %u\n", (ui)ntohs((tcph->src_port)) );
        printf("dest-port = %u\n", (ui)ntohs((tcph->dest_port)) );
        printf("seq_num = %u\n", (ui)ntohl((tcph->seq_num)) );
        printf("ack_num = %u\n", (ui)ntohl((tcph->ack_num)) );
        printf("offset = %u\n", (ui)((tcph->offset >> 4) & 0x0F)  );
        printf("flag = %u\n", (ui)(tcph->flag) );
        printf("window_size = %u\n", (ui)ntohs((tcph->window_size)) );
        printf("checksum = %u\n", (ui)(tcph->checksum) );
        printf("urgent_ptr = %u\n", (ui)(tcph->urgent_ptr) );
        printf("---------------------------\n");
    }
*/

void SwitchModel::parsePacket(uc* pkt_, int pkt_length_){

    IPHeader* iph = (IPHeader*)pkt_;
    if(iph->protocol != TCP){
        //sendPkt(qh_, nfa_);
        return;
    }
    us ip_header_length = ((us)(iph->ver_ihl & 0xF)) << 2;
    us ip_total_length = ntohs(iph->total_len);
    
    TCPHeader * tcph = (TCPHeader*)(pkt_ + ip_header_length);
    us tcp_header_length = ((tcph->offset >> 4) & 0x0F) << 2;
    if(tcp_header_length + ip_header_length == ip_total_length){ // this tcp has no payload
        //sendPkt(qh_, nfa_);
        return;
    }

    us payload_length = ip_total_length - ip_header_length - tcp_header_length;
    char* payload = (char*)(tcph + tcp_header_length);

    int msg_type;
    if( strmatcher->kmp_matcher(payload, payload_length, (char*)RPC_SUBMIT, std::strlen(RPC_SUBMIT)) != NULL ) msg_type = RPC_SUBMIT_TYPE;
    else if( strmatcher->kmp_matcher(payload, payload_length, (char*)RPC_ALLOCATE, std::strlen(RPC_ALLOCATE)) != NULL ) msg_type = RPC_ALLOCATE_TYPE;
    else if( strmatcher->kmp_matcher(payload, payload_length, (char*)RPC_FINISH, std::strlen(RPC_FINISH)) != NULL ) msg_type = RPC_FINISH_TYPE;
    else if( strmatcher->kmp_matcher(payload, payload_length, (char*)RPC_GETNEWAPPLICATION, std::strlen(RPC_GETNEWAPPLICATION)) != NULL ) msg_type = RPC_GETNEWAPPLICATION_TYPE;
    else if( strmatcher->kmp_matcher(payload, payload_length, (char*)RPC_GETAPPLICATIONREPORT, std::strlen(RPC_GETAPPLICATIONREPORT)) != NULL ) msg_type = RPC_GETAPPLICATIONREPORT_TYPE;
    else if( strmatcher->kmp_matcher(payload, payload_length, (char*)RPC_STARTCONTAINERS, std::strlen(RPC_STARTCONTAINERS)) != NULL ) msg_type = RPC_STARTCONTAINERS_TYPE;
    else if( strmatcher->kmp_matcher(payload, payload_length, (char*)RPC_REGISTERAPPLICATIONMASTER, std::strlen(RPC_REGISTERAPPLICATIONMASTER)) != NULL  ) msg_type = RPC_REGISTERAPPLICATIONMASTER_TYPE;
    else if(strmatcher->kmp_matcher(payload, payload_length, (char*)RPC_DONE, std::strlen(RPC_DONE)) != NULL  ) msg_type = RPC_DONE_TYPE;
    else msg_type = RPC_UNKNOWN_TYPE;

    switch(msg_type){
        case RPC_SUBMIT_TYPE : {
            char* job_id_ptr = strmatcher->kmp_matcher(payload, payload_length, (char*)JOB_ID_PREFIX, strlen(JOB_ID_PREFIX));
            assert(job_id_ptr != NULL);
            bool create = false;
            ui idx = getJobIdx(job_id_ptr, JOB_ID_LENGTH, create);
            if(create){
                waitingToFetch->add(idx);
            }
            job_id_ptr[JOB_ID_LENGTH] = '\0';
            printf("A client submit a job with job-id = %s\n", job_id_ptr);
            //sendPkt(qh_, nfa_);
            break;
        }
        case RPC_ALLOCATE_TYPE : {
            //sendPkt(qh_, nfa_);
            break;
        }
        case RPC_FINISH_TYPE : {
            //sendPkt(qh_, nfa_);
            break;
        }
        case RPC_GETNEWAPPLICATION_TYPE : {
            //sendPkt(qh_, nfa_);
            break;
        }
        case RPC_GETAPPLICATIONREPORT_TYPE : {
            //sendPkt(qh_, nfa_);
            break;
        }
        case RPC_STARTCONTAINERS_TYPE : {
            uc psh_flag = tcph->flag & 0x08;
            ul iport = (ntohl(iph->src_ip) << 16)  + ntohs(tcph->src_port);

            bool startTask = false;
            char *task_id_ptr = NULL, *job_id_ptr = NULL;
            if( (task_id_ptr = strmatcher->kmp_matcher(payload, payload_length, (char*)TASK_ID_PREFIX, std::strlen(TASK_ID_PREFIX))) != NULL ){
                startTask = true;
            }
            if(!startTask){ // start AM, we don't need to keep the whole packet
                bool create = false;
                char* job_id_ptr = strmatcher->kmp_matcher(payload, payload_length, (char*)JOB_ID_PREFIX, strlen(JOB_ID_PREFIX));
                ui idx = getJobIdx(job_id_ptr, JOB_ID_LENGTH, create);
                assert(!create);
                (*idx2JobPtr)[idx]->setHostIP(ntohl(iph->dest_ip));


                struct in_addr destip;
                destip.s_addr = iph->dest_ip;
                job_id_ptr[JOB_ID_LENGTH] = '\0';
                printf("%s begin to start AM in %s\n", job_id_ptr, (char*)inet_ntoa(destip));
            }else{ // start Task ,we need to keep the whole packet
                if(psh_flag){
                    char* yarnchild_flag = strmatcher->kmp_matcher(payload, payload_length, (char*)YARNCHILD, std::strlen(YARNCHILD));
                    int len = payload_length - (yarnchild_flag - payload);
                    task_id_ptr = strmatcher->kmp_matcher(yarnchild_flag, len, (char*)TASK_ID_PREFIX, std::strlen(TASK_ID_PREFIX));
                    job_id_ptr = task_id_ptr + std::strlen(TASK_ID_PREFIX);
                    int first, second, third;
                    int i = 0;
                    while(job_id_ptr[i] != '_') i++;
                    first = ++i;
                    while(job_id_ptr[i] != '_') i++;
                    i++;
                    if(job_id_ptr[i] == 'm'){
                        i+=2;
                        second = i;
                        while(job_id_ptr[i] != '_') i++;
                        third = ++i;

                        ul jobId = std::atol(job_id_ptr) * 10000 + (ul)std::atol(job_id_ptr + first);
                        ui taskId = std::atoi(job_id_ptr + second) * 1000000 + (job_id_ptr[third] - '0');
                        std::map<ul, std::map<ui, ui>* >::iterator ite = job2TaskSet->find(jobId);
                        std::map<ui, ui>* taskset;
                        if(ite == job2TaskSet->end()){
                            taskset = new std::map<ui, ui>();
                            job2TaskSet->insert(std::pair<ul, std::map<ui, ui>* >(jobId, taskset));
                        }else taskset = ite->second;
                        taskset->insert(std::pair<ui, ui>(taskId, ntohl(iph->dest_ip)));
                    }
                    
                    struct in_addr destip;
                    destip.s_addr = iph->dest_ip;
                    job_id_ptr[JOB_ID_LENGTH] = '\0';
                    printf("%s begin to start a Task %s in %s\n", job_id_ptr, task_id_ptr, (char*)inet_ntoa(destip));
                }else{
                    Packet* pkt = getPacket(iport);
                    pkt->addPayload(payload, payload_length);
                    ui seq_num = ntohl(tcph->seq_num);
                    std::map<ul, ui>::iterator ite = iport2nextSeq->find(iport);
                    if(ite == iport2nextSeq->end() || seq_num == ite->second){
                        (*iport2nextSeq)[iport] = seq_num + payload_length;
                    }
                }
            }
            //sendPkt(qh_, nfa_);
            break;
        }
        case RPC_REGISTERAPPLICATIONMASTER_TYPE: {
            //sendPkt(qh_, nfa_);
            break;
        }
        case RPC_DONE_TYPE : {
            if( strmatcher->kmp_matcher(payload, payload_length, (char*)MAPFLAG, strlen(MAPFLAG)) != NULL ){
                char* task_id_ptr = strmatcher->kmp_matcher(payload, payload_length, (char*)TASK_ID_PREFIX, std::strlen(TASK_ID_PREFIX));
                char* job_id_ptr = task_id_ptr + std::strlen(TASK_ID_PREFIX);
                int first, second, third;
                int i = 0;
                while(job_id_ptr[i] != '_') i++;
                first = ++i;
                while(job_id_ptr[i] != '_') i++;
                i++;
                if(job_id_ptr[i] == 'm'){
                    i+=2;
                    second = i;
                    while(job_id_ptr[i] != '_') i++;
                    third = ++i;
                    ul jobId = std::atol(job_id_ptr) * 10000 + (ul)std::atol(job_id_ptr + first);
                    ui taskId = std::atoi(job_id_ptr + second) * 1000000 + (job_id_ptr[third] - '0');
                    waitingMapResult->add(std::pair<ul, ui>(jobId, taskId));

                    struct in_addr destip;
                    destip.s_addr = iph->dest_ip;
                    job_id_ptr[JOB_ID_LENGTH] = '\0';
                    printf("%s finish a Task %s\n", job_id_ptr, task_id_ptr);
                }
            }
            //sendPkt(qh_, nfa_);
            break; 
        }
        default : {
            uc psh_flag = tcph->flag & 0x08;
            ul iport = (ntohl(iph->src_ip) << 16)  + ntohs(tcph->src_port);
            ui seq_num = ntohl(tcph->seq_num);
            std::map<ul, ui>::iterator ite = iport2nextSeq->find(iport);
            if(seq_num == ite->second){
                Packet* pkt = getPacket(iport);
                pkt->addPayload(payload, payload_length);
                if(psh_flag){
                    ui buf_len;
                    char* buf = pkt->getPayload(&buf_len);
                    char* yarnchild_flag = strmatcher->kmp_matcher(buf, buf_len, (char*)YARNCHILD, std::strlen(YARNCHILD));
                    int len = payload_length - (yarnchild_flag - payload);
                    char* task_id_ptr = strmatcher->kmp_matcher(yarnchild_flag, len, (char*)TASK_ID_PREFIX, std::strlen(TASK_ID_PREFIX));
                    char* job_id_ptr = task_id_ptr + std::strlen(TASK_ID_PREFIX);
                    int first, second, third;
                    int i = 0;
                    while(job_id_ptr[i] != '_') i++;
                    first = ++i;
                    while(job_id_ptr[i] != '_') i++;
                    i++;
                    if(job_id_ptr[i] == 'm'){
                        i+=2;
                        second = i;
                        while(job_id_ptr[i] != '_') i++;
                        third = ++i;

                        ul jobId = std::atol(job_id_ptr) * 10000 + (ul)std::atol(job_id_ptr + first);
                        ui taskId = std::atoi(job_id_ptr + second) * 1000000 + (job_id_ptr[third] - '0');
                        std::map<ul, std::map<ui, ui>* >::iterator ite = job2TaskSet->find(jobId);
                        std::map<ui, ui>* taskset;
                        if(ite == job2TaskSet->end()){
                            taskset = new std::map<ui, ui>();
                            job2TaskSet->insert(std::pair<ul, std::map<ui, ui>* >(jobId, taskset));
                        }else taskset = ite->second;
                        taskset->insert(std::pair<ui, ui>(taskId, ntohl(iph->dest_ip)));
                    }

                    struct in_addr destip;
                    destip.s_addr = iph->dest_ip;
                    job_id_ptr[JOB_ID_LENGTH] = '\0';
                    printf("%s begin to start a Task %s in %s\n", job_id_ptr, task_id_ptr, (char*)inet_ntoa(destip));

                    
                    removePacket(iport);
                    delete pkt;
                }else{
                    (*iport2nextSeq)[iport] = seq_num + payload_length;
                }
            }
            //sendPkt(qh_, nfa_);
        }
    }
    delete [] pkt_;
}

void SwitchModel::processPkt(){
    std::pair<uc*, int> p;
    while(true){
        waitingToProcess->get(p);
        parsePacket(p.first, p.second);
    }
}