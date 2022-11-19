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
  struct node {   //双向链表结点结构体 
    node() = default;
    explicit node(T d, node *p = nullptr) : data(d), pre(p) {}  //构造函数被explicit修饰后, 就不能再被隐式调用了
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

  std::mutex mutex1;     //互斥量 
  size_t size;       //链表长度 
  std::unordered_map<T, node *> LRUmap;  //用map记录已经在队列中的元素到链表节点的键值对
  node *head;     //头结点 
  node *tail;      //尾结点                                                                       
  // add your member variables here
};

} // namespace scudb
