#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages):cache(num_pages, victims.end()){
  num_pages_ = num_pages;
}

LRUReplacer::~LRUReplacer() = default;

/**
 * TODO: Student Implement
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
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
    victims.erase(tmp);
    cache[frame_id] = victims.end();
  }
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
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