#ifndef STRMATCHER_H
#define STRMATCHER_H
#include <cstdio>
#include <cstddef>
#include "myType.h"

class StrMatcher{
  public :
    StrMatcher(){}
    ~StrMatcher(){
        if(pi != NULL) delete[] pi;
    }
    char* kmp_matcher(char* text_, int text_length_, char* pattern_, int pattern_length_){
        compute_prefix(pattern_, pattern_length_);
        int i = 0, j = 0;
        while( i < text_length_ && j < pattern_length_){
            if(j == -1 || text_[i] == pattern_[j]){
                i++;
                j++;
            }else j = pi[j];
        }
        if(j == pattern_length_)
            return text_ + i - j;
        else
            return NULL;
    }
  private : 
    void compute_prefix(char* str_, int length_){
      if(length_ > pi_len){
          pi_len = length_;
          if(pi != NULL){ 
              delete [] pi;
              pi = NULL;
          }
      }
      if(pi == NULL) pi = new int[sizeof(int) * pi_len + 1];
      pi_effective_len = length_;
      pi[0] = -1;
      int i = 0, j = -1;
      while(i < length_){
          if(j == -1 || str_[i] == str_[j] ){
              ++i;
              ++j;
              pi[i] = j;
          }else
              j = pi[j];
      }
    }

    int pi_effective_len;
    int pi_len;
    int *pi = NULL;
};

#endif // !STRMATCHER_H