#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages):cache(num_pages, victims.end()){
  num_pages_ = num_pages;
}

LRUReplacer::~LRUReplacer() = default;

/**
 * TODO: Student Implement
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  /*这个函数看上去和下面那个函数做的事情很相似，实际上关键的区别在于它在删除元素后把它返回了，这样就可以在主调函数里面把这个页替换*/
  if(victims.empty()){
    return false;
  }else{
    *frame_id = victims.back();
    cache[*frame_id] = victims.end();
    victims.pop_back();
    return true;
  }
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
  auto tmp = cache[frame_id];
  if(tmp != victims.end()){
    /*从victims里面把它删除，这样它就不会被替换*/
    victims.erase(tmp);
    cache[frame_id] = victims.end();
  }
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
  /*把页加入victims，这是一个待删除的列表；更新cache*/
  if(victims.size() >= num_pages_ || cache[frame_id] != victims.end()){
    return;
  }else{
    victims.push_front(frame_id);
    cache[frame_id] = victims.begin();
  }
}

/**
 * TODO: Student Implement
 */
size_t LRUReplacer::Size() {
  return victims.size();
  //return 0;
}