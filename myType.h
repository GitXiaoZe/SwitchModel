#ifndef MYTYPE_H
#define MYTYPE_H
typedef long long ll;
typedef unsigned long ul;
typedef unsigned int ui;
typedef unsigned short us;
typedef unsigned char uc;

typedef struct{
    uc ver_ihl;
    uc tos;
    us total_len;

    us identification;
    us flag_offset;
    
    uc ttl;
    uc protocol;
    us checksum;
    
    ui src_ip;
    ui dest_ip;
} IPHeader;

typedef struct{
    us src_port;
    us dest_port;
    ui seq_num;
    ui ack_num;
    uc offset;
    uc flag;
    us window_size;
    us checksum;
    us urgent_ptr;
} TCPHeader;


#endif // !MYTYPE_H