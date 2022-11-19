#include <list>

#include "hash/extendible_hash.h"
#include "page/page.h"

namespace scudb {

/*
 * constructor
 * array_size: fixed array size for each bucket
 */
template <typename K, typename V>
ExtendibleHash<K, V>::ExtendibleHash(size_t size)
        :globalDepth(0), bucketMaxSize(size), numBucket(1) {
  bucketTable.push_back(std::make_shared<Bucket>(0));
}

/*
 * helper function to calculate the hashing address of input key
 */
template <typename K, typename V>
size_t ExtendibleHash<K, V>::HashKey(const K &key) {
  return std::hash<K>{}(key);
}

/*
 * helper function to return global depth of hash table
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetGlobalDepth() const {
  return globalDepth;
}

/*
 * helper function to return local depth of one specific bucket
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetLocalDepth(int bucket_id) const {
  if (!bucketTable[bucket_id] || bucketTable[bucket_id]->items.empty()) return -1;
  return bucketTable[bucket_id]->localDepth;
}              //若哈希表存在并且不为空返回局部深度 

/*
 * helper function to return current number of bucket in hash table
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetNumBuckets() const {
  return numBucket;
}

/*
 * lookup function to find value associate with input key
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Find(const K &key, V &value) {
  std::lock_guard<std::mutex> guard(mutex1);     //构造时加锁，析构时解锁

  auto index = getBucketIndex(key);     //首先找到对应的bucket位置
  std::shared_ptr<Bucket> bucket = bucketTable[index];
  if (bucket != nullptr && bucket->items.find(key) != bucket->items.end()) {//map.find(k)函数 在容器中寻找值为k的元素，
    value = bucket->items[key];                                              // 返回该元素的迭代器, 否则, 返回map.end() 
    return true;
  }
  return false;
}

/*
 * delete <key,value> entry in hash table
 * Shrink & Combination is not required for this project
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Remove(const K &key) {
  std::lock_guard<std::mutex> guard(mutex1);

  auto index = getBucketIndex(key);
  std::shared_ptr<Bucket> bucket = bucketTable[index];

  if (bucket == nullptr || bucket->items.find(key) == bucket->items.end()) {
    return false;      //桶为空或未找到有对应的关键字的节点，返回false 
  }
  bucket->items.erase(key);  //map.erase() 可以移除键和参数匹配的元素，然后返回所移除元素的个数
  return true;
}

template <typename K, typename V>
int ExtendibleHash<K, V>::getBucketIndex(const K &key) {
  return HashKey(key) & ((1 << globalDepth) - 1);   //将1左移globalDepth位，相当于2^GlobalDepth 接着减一，后几位就全是1，
}                                                   // 和关键字hashcode进行与操作，就可以得到关键字对应的bucket位置 

/*
 * insert <key,value> entry in hash table
 * Split & Redistribute bucket when there is overflow and if necessary increase
 * global depth
 */
template <typename K, typename V>
void ExtendibleHash<K, V>::Insert(const K &key, const V &value) {
  std::lock_guard<std::mutex> guard(mutex1);

  auto index = getBucketIndex(key);
  std::shared_ptr<Bucket> targetBucket = bucketTable[index];

  while (targetBucket->items.size() == bucketMaxSize) { //发生桶溢出, 需要扩容   
    if (targetBucket->localDepth == globalDepth) {   //如果L==G，此时只有一个指针指向当前桶，则扩展索引，
      size_t length = bucketTable.size();            //本地位深度和全局位深度均+1，索引项翻倍，重组当前桶的元素  vector增大一倍 指针复制 
      for (size_t i = 0; i < length; i++) {            
        bucketTable.push_back(bucketTable[i]);         
      }
      globalDepth++;
    }                 
    int mask = 1 << targetBucket->localDepth;  //如果L<G，此时不止一个指针指向当前桶，故不需要翻倍索引项，
    numBucket++;                               //只需分裂出一个桶，将本地位深度+1，然后重组当前桶元素即可。
    auto zeroBucket = std::make_shared<Bucket>(targetBucket->localDepth + 1);   //二进制，只有两种0 1 
    auto oneBucket = std::make_shared<Bucket>(targetBucket->localDepth + 1);
    for (auto item : targetBucket->items) {    //重组桶元素 
      size_t hashkey = HashKey(item.first);    //取哈希值 
      if (hashkey & mask) {           //进行与操作 若最高位是1 放入onebucket 反之 
        oneBucket->items.insert(item);
      } else { 
        zeroBucket->items.insert(item);
      }
    }

    for (size_t i = 0; i < bucketTable.size(); i++) {
      if (bucketTable[i] == targetBucket) {              //调整指针 
        if (i & mask) {
          bucketTable[i] = oneBucket;
        } else {
          bucketTable[i] = zeroBucket;
        }
      }
    }

    index = getBucketIndex(key);
    targetBucket = bucketTable[index];
  } //end while

  targetBucket->items[key] = value;    //未插入 
}

template class ExtendibleHash<page_id_t, Page *>;
template class ExtendibleHash<Page *, std::list<Page *>::iterator>;
// test purpose
template class ExtendibleHash<int, std::string>;
template class ExtendibleHash<int, std::list<int>::iterator>;
template class ExtendibleHash<int, int>;
} // namespace scudb
