#include "SwitchModel.h"

#define BUFSIZE 4096

ui SwitchModel::createJob(ul iport_, char* job_id_, ui job_id_length_, ul jobId_){
    std::map<ul, ui>::iterator ite = jobId2idx->find(jobId_);
    ui idx;
    if(jobId2idx->end() == ite){
        idx = job_index++;
        iport2idx->insert( std::pair<ul, ui>(iport_, idx) );
        idx2JobPtr->insert( std::pair<ui, Job*>(idx, new Job(job_id_, job_id_length_, idx, jobId_)) );
        jobId2idx->insert( std::pair<ul, ui>(jobId_, idx) );
    }else
        idx = ite->second;
    
    return idx;
}

ui SwitchModel::getJobIdx(ul iport_, char* job_id_, ui job_id_length_){
    int first = 0, second = 0;
    for(int i=0; i < job_id_length_; i++){
        if(*(job_id_ + i) == '_'){
            if(!first) first = i + 1;
            else second = i + 1;
        }
    }
    ul jobId = std::atol(job_id_ + first) * 10000 + std::atol(job_id_ + second);
    std::map<ul, ui>::iterator ite = jobId2idx->find(jobId);
    ui idx;
    if(ite == jobId2idx->end()){
        idx = createJob(iport_, job_id_, job_id_length_, jobId);
    }else idx = ite->second;
    return idx;
}

#define FETCH_COMMAND "sudo -u hzh /home/hzh/Documents/hadoop-3.1.2/bin/hdfs dfs -get /tmp/hadoop-yarn/staging/hzh/.staging/%s/job.xml /home/hzh/Documents/NFQueue/conf/%s_job.xml"

void SwitchModel::fetchCongiureFileForJob(){
    char buf[BUFSIZE];
    unsigned int job_index;
    while(true){
        waitingToFetch->get(job_index);
        Job* job = (*idx2JobPtr)[job_index];
        snprintf(buf, BUFSIZE, FETCH_COMMAND, job->job_id, job->job_id);
        //printf("begin to fetch %s in %s\n", job->job_id, buf);
        system(buf);
        int res = 0;
        wait(&res);
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
    while(true){
        waitingToConfigure->get(job_index);
        Job* job = (*idx2JobPtr)[job_index];
        snprintf(buf, BUFSIZE, CONF_FILE_PATH, job->job_id);
        printf("begin to conf %s in %s\n", job->job_id, buf);
        TiXmlDocument* doc = new TiXmlDocument(CONF_FILE_PATH);
        if(!doc->LoadFile()){
            printf("Error msg %s\n", doc->ErrorDesc());
            printf("load file error\n");
        }else{
            TiXmlElement *root = doc->RootElement();
            TiXmlNode *item;
            int map, reduce;
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
        system(buf);
        int r = 0;  
        wait(&r);
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
        FILE* inFile = fopen(buf, "rb");
        if(inFile == NULL){
            printf("Error occurs when opening files %s \n", buf);
            continue;//???
        }
        ul data[3];
        for(int i=0; i < job->reduce; i++){
            int v = fread(data, sizeof(unsigned long), 3, inFile);
            assert(v==3);
            job->mapTask_size[i] += convert(data[1]); 
        }
        fclose(inFile);
        job->hasReceived++;
        if(job->hasReceived == job->map_ratio){
            waitingToSchedule->add(idx);
        }
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
        FILE* outFile = fopen(buf, "w");
        if(outFile == NULL){
            printf("Error when opening file when schedule\n");
            continue;
        }
        for(int i=1; i <= job->reduce; i++){
            fprintf(outFile, "%d ", pairs[i].first);
        }
        fclose(outFile);
        snprintf(buf, BUFSIZE, SEND_ORDER_COMMAND, password, job->job_id, username, host, job->job_id);
        system(buf);
        int res = 0;
        wait(&res);
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
void SwitchModel::parsePacket(struct nfq_q_handle* qh_, struct nfq_data* nfa_, unsigned char* pkt_, int pkt_length_){

    IPHeader* iph = (IPHeader*)pkt_;
    if(iph->protocol != TCP){
        sendPkt(qh_, nfa_);
        return;
    }
    us ip_header_length = ((us)(iph->ver_ihl & 0xF)) << 2;
    us ip_total_length = ntohs(iph->total_len);
    
    TCPHeader * tcph = (TCPHeader*)(pkt_ + ip_header_length);
    us tcp_header_length = ((tcph->offset >> 4) & 0x0F) << 2;
    if(tcp_header_length + ip_header_length == ip_total_length){ // this tcp has no payload
        sendPkt(qh_, nfa_);
        return;
    }
    ui src_ip = ntohl(iph->src_ip);
    us src_port = ntohs(tcph->src_port);
    uc psh_flag = tcph->flag & 0x08;

    int msg_type;
    
    sendPkt(qh_, nfa_);
}
