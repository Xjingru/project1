/**
 * LRU implementation
 */
#include <cassert>

#include "buffer/lru_replacer.h"
#include "page/page.h"

namespace scudb {

template <typename T> LRUReplacer<T>::LRUReplacer() : size(0) {
  head = new node();
  tail = head; 
}

template <typename T> LRUReplacer<T>::~LRUReplacer() {}

/*
 * Insert value into LRU
 */
template <typename T> void LRUReplacer<T>::Insert(const T &value) {
  std::lock_guard<std::mutex> guard(mutex1);  //在构造函数中自动绑定它的互斥体并加锁，在析构函数中解锁

  auto it = LRUmap.find(value);    //在容器中寻找值为k的元素，返回该元素的迭代器。否则，返回map.end()
  if(it == LRUmap.end()) {                 
      tail->next = new node(value, tail);    
      tail = tail->next;                       //放到队尾 
      LRUmap.emplace(value, tail); //将一个新元素插入到在给定的容器中构造的容器中。
      ++size;          //将lru链表长度+1 
  } 
  else {
     // 该页面是新插入的，如果本来就在队列尾就不用重新操作指针了
    if(it->second != tail) {
        // 先从原位置移除
      node *pre = it->second->pre;
      node *cur = pre->next;
      pre->next = std::move(cur->next); //将一个左值强制转化为右值引用，继而可以通过右值引用使用该值 基本等同于一个类型转换 
      pre->next->pre = pre;

      // 再放到尾部
      cur->pre = tail;
      tail->next = std::move(cur);
      tail = tail->next;
      }
  }
}

/* If LRU is non-empty, pop the head member from LRU to argument "value", and
 * return true. If LRU is empty, return false
 */
template <typename T> bool LRUReplacer<T>::Victim(T &value) {
  std::lock_guard<mutex> guard(mutex1);
  if(size == 0) {
    return false;
  }

  value = head->next->data;
  head->next = head->next->next;    //pop the head member from LRU to argument "value"
  if(head->next != nullptr) {
      head->next->pre = head;
  }

  LRUmap.erase(value);     //用来从一个map中删除特定的节点
  if(--size == 0) {    //然后链表长度-1 若为零则该双向链表为空 
      tail = head;
  }

  return true;
}

/*
 * Remove value from LRU. If removal is successful, return true, otherwise
 * return false
 */
template <typename T> bool LRUReplacer<T>::Erase(const T &value) {
  std::lock_guard<mutex> guard(mutex1);
  auto it = LRUmap.find(value);
  if(it != LRUmap.end()) {     //链表中存在该结点
    if(it->second != tail) {   //如果不是尾结点 
      node *pre = it->second->pre;       //从原位置移除
      node *cur = pre->next;        
      pre->next = std::move(cur->next); 
      pre->next->pre = pre;
    } 
    else {      //若是尾结点 直接删除 
      tail = tail->pre;
      delete tail->next;
    }

    LRUmap.erase(value);   //用来从一个map中删除特定的节点
    if(--size == 0) {
        tail = head;
    }
    return true;
  }

  return false;
}

template <typename T> size_t LRUReplacer<T>::Size() {
  std::lock_guard<mutex> guard(mutex1);
   return size; 
}

template class LRUReplacer<Page *>;
// test only
template class LRUReplacer<int>;

} // namespace scudb
