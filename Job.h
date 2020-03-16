#ifndef JOB_H
#define JOB_H

#include <cstring>
#include <algorithm>
#include <cmath>
#include "myType.h"

#define JOB_ID_LENGTH 22
class Job{
  public :
    Job(char *job_id_, ui job_id_length_, ui index_, ul jobId_)
        : index(index_), jobId(jobId_), hasReceived(0) {
      std::memcpy(job_id, job_id_, job_id_length_);
      job_id[JOB_ID_LENGTH] = '\0';
    }

    ~Job(){
        if(reduce) delete [] mapTask_size;
    }

    //void insertPkt(struct nfq_q_handle* qh_, struct nfq_data* pkt_){
    //  if( submitRequest == nullptr ){
    //    submitRequest = std::make_shared<Tcp_Packet>();
    //  }
    //  submitRequest->insert(qh_, pkt_);
    //}
    bool hasReduce(){
        return reduce == 0;
    }
    ui MapRatio(){ return map_ratio; }
    void setTask(int map_, int reduce_, float ratio_){
        map = map_;
        reduce = reduce_;
        map_ratio = std::max(1, (int)std::ceil(map_ * ratio_));
        if(reduce_){
            mapTask_size = new ul[reduce_];
            std::fill(mapTask_size, mapTask_size + reduce_, 0);
        }
    }

    void setHostIP(ui host_ip_){ host_ip = host_ip_; }

    char job_id[JOB_ID_LENGTH + 1];
    ul jobId;
    ui host_ip;
    ui index;
    //std::shared_ptr<Tcp_Packet> submitRequest = nullptr;

    ui map;
    ui reduce;
    ui map_ratio;
    ul* mapTask_size;
    ui hasReceived;

    ui hasSentMapRequest;
    ui hasSentReduceRequest;
    ui currentRequest;
};

#endif // !JOB_H