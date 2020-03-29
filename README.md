# SwitchModel

## Dependency
sudo apt-get install libnetfilter-queue-dev

## Compile command
g++ main.cpp SwitchModel.h SwitchModel.cpp StrMatcher.h StrMatcher.cpp myType.h CPQueue.h tinxml/* -lnetfilter_queue -lpthread -lpcap -std=c++11 -o main

## Set iptables
sudo iptables -A INPUT/OUTPUT  -j NFQUEUE --queue-num 0
(you can also modify queue-num in the Macro "QUEUE_NUM" in main.cpp)
