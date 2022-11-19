/**
 * lru_replacer.h
 *
 * Functionality: The buffer pool manager must maintain a LRU list to collect
 * all the pages that are unpinned and ready to be swapped. The simplest way to
 * implement LRU is a FIFO queue, but remember to dequeue or enqueue pages when
 * a page changes from unpinned to pinned, or vice-versa.
 */

#pragma once


#include <unordered_map>
#include <mutex>
#include <memory>

#include "buffer/replacer.h"
#include "hash/extendible_hash.h"
using namespace std;
namespace scudb {

template <typename T> class LRUReplacer : public Replacer<T> {
  struct node {   //˫��������ṹ�� 
    node() = default;
    explicit node(T d, node *p = nullptr) : data(d), pre(p) {}  //���캯����explicit���κ�, �Ͳ����ٱ���ʽ������
    T data;
    node *pre = nullptr;
    node *next = nullptr;
  };

public:
  // do not change public interface
  LRUReplacer();

  ~LRUReplacer();

  void Insert(const T &value);

  bool Victim(T &value);

  bool Erase(const T &value);

  size_t Size();

private:

  std::mutex mutex1;     //������ 
  size_t size;       //������ 
  std::unordered_map<T, node *> LRUmap;  //��map��¼�Ѿ��ڶ����е�Ԫ�ص�����ڵ�ļ�ֵ��
  node *head;     //ͷ��� 
  node *tail;      //β���                                                                       
  // add your member variables here
};

} // namespace scudb
