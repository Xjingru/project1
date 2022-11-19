/*
 * extendible_hash.h : implementation of in-memory hash table using extendible
 * hashing
 *
 * Functionality: The buffer pool manager must maintain a page table to be able
 * to quickly map a PageId to its corresponding memory location; or alternately
 * report that the PageId does not match any currently-buffered page.
 */

#pragma once

#include <cstdlib>
#include <vector>
#include <string>
#include <map>
#include <memory>
#include <mutex>

#include "hash/hash_table.h"
using std::map;
using std::vector;
using std::mutex;

namespace scudb {

template <typename K, typename V>
class ExtendibleHash : public HashTable<K, V> {
  struct Bucket {
    Bucket(int depth) : localDepth(depth) {};
    int localDepth;      //局部深度 每个桶有一个本地位深度，表示当前桶中元素的低几位是一样的。
    map<K, V> items;     //根据key而直接进行访问的数据结构，可以通过把关键字映射到表中一个位置来访问记录
  };                  //桶是存记录的地方
public:
  // constructor
  ExtendibleHash(size_t size);
  // helper function to generate hash addressing
  size_t HashKey(const K &key);
  // helper function to get global & local depth
  int GetGlobalDepth() const;
  int GetLocalDepth(int bucket_id) const;
  int GetNumBuckets() const;
  // lookup and modifier
  bool Find(const K &key, V &value) override;
  bool Remove(const K &key) override;
  void Insert(const K &key, const V &value) override;
  

private:
  // add your own member variables here
  int getBucketIndex(const K &key);     //输入是一个关键字，输出是关键字对应的bucket位置
  int globalDepth;         //全局深度表示取哈希值的低几位作为索引, 而且哈希表索引项索引项数始终等于2^Global Depth
  size_t bucketMaxSize;    //每个bucket能装的键值对的最大值为bucketMaxSize   size_t 类型表示C中任何对象所能达到的最大长度，它是无符号整型。
  int numBucket;            //哈希表数组的长度
  vector<std::shared_ptr<Bucket>> bucketTable;   //哈希表  
  mutex mutex1;      //互斥量mutex 

};
} // namespace scudb
