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
#define FETCH_MAP_RESULT_COMMAND "sshpass -p %s scp %s@%s:/home/hzh/Documents/file.out.index /home/hzh/Documents/NFQueue/conf/%s_%04u.file.out.index"
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




#define MAP_RESULT_FILE_PATH "/home/hzh/Documents/NFQueue/conf/%s_%04u.file.out.index"
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

int main(){
    return 0;
}
