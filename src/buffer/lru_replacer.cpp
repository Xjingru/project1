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
  std::lock_guard<std::mutex> guard(mutex1);  //�ڹ��캯�����Զ������Ļ����岢�����������������н���

  auto it = LRUmap.find(value);    //��������Ѱ��ֵΪk��Ԫ�أ����ظ�Ԫ�صĵ����������򣬷���map.end()
  if(it == LRUmap.end()) {                 
      tail->next = new node(value, tail);    
      tail = tail->next;                       //�ŵ���β 
      LRUmap.emplace(value, tail); //��һ����Ԫ�ز��뵽�ڸ����������й���������С�
      ++size;          //��lru������+1 
  } 
  else {
     // ��ҳ�����²���ģ�����������ڶ���β�Ͳ������²���ָ����
    if(it->second != tail) {
        // �ȴ�ԭλ���Ƴ�
      node *pre = it->second->pre;
      node *cur = pre->next;
      pre->next = std::move(cur->next); //��һ����ֵǿ��ת��Ϊ��ֵ���ã��̶�����ͨ����ֵ����ʹ�ø�ֵ ������ͬ��һ������ת�� 
      pre->next->pre = pre;

      // �ٷŵ�β��
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

  LRUmap.erase(value);     //������һ��map��ɾ���ض��Ľڵ�
  if(--size == 0) {    //Ȼ��������-1 ��Ϊ�����˫������Ϊ�� 
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
  if(it != LRUmap.end()) {     //�����д��ڸý��
    if(it->second != tail) {   //�������β��� 
      node *pre = it->second->pre;       //��ԭλ���Ƴ�
      node *cur = pre->next;        
      pre->next = std::move(cur->next); 
      pre->next->pre = pre;
    } 
    else {      //����β��� ֱ��ɾ�� 
      tail = tail->pre;
      delete tail->next;
    }

    LRUmap.erase(value);   //������һ��map��ɾ���ض��Ľڵ�
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
