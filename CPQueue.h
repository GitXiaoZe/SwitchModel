#ifndef CPQUEUE_H
#define CPQUEUE_H

#include <mutex>
#include <queue>
#include <condition_variable>
#define QUEUE_SIZE 1024

template<typename T>
class CP_Queue{ //Consumer-Producer Queue
  public :
    CP_Queue() : maxSize(QUEUE_SIZE){} 
    CP_Queue(int size_) : maxSize(size_){}
    ~CP_Queue(){}
    bool isFull() const { return q.size() >= maxSize; }
    bool isEmpty() const { return q.size() == 0; }
    int length() const { return q.size(); }
    void add(T request){
      std::unique_lock<std::mutex> lock(mut);
      cond.wait(lock, [this](){ return !isFull(); });
      q.push(request);
      lock.unlock();
      cond.notify_all();
    }

    void get(T &result){
      std::unique_lock<std::mutex> lock(mut);
      cond.wait(lock, [this](){ return !isEmpty(); });
      result = q.front();
      q.pop();
      lock.unlock();
      cond.notify_all();
    }

  private :
    std::condition_variable cond;
    std::mutex mut;
    std::queue<T> q;
    int maxSize;
};

#endif // !CPQUEUE_H
