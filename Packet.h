#ifndef PACKET_H
#define PACKET_H
#include <cstring>
#include "myType.h"

#define PACKET_LEN 0xFFFF
class Packet{
public :
    Packet(){
        buffer = new char[PACKET_LEN];
        len = 0;
    }
    ~Packet(){  if(buffer) delete [] buffer; }
    void addPayload(char* payload_, ui payload_len_){
        std::memcpy(buffer + len, payload_, payload_len_);
        len += payload_len_;
    }
    char* getPayload(ui* len_){
        *len_ = len;
        return buffer;
    }
    char* buffer = 0;
    ui len;
};
#endif // !PACKET_H