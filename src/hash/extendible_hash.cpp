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
}              //����ϣ����ڲ��Ҳ�Ϊ�շ��ؾֲ���� 

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
  std::lock_guard<std::mutex> guard(mutex1);     //����ʱ����������ʱ����

  auto index = getBucketIndex(key);     //�����ҵ���Ӧ��bucketλ��
  std::shared_ptr<Bucket> bucket = bucketTable[index];
  if (bucket != nullptr && bucket->items.find(key) != bucket->items.end()) {//map.find(k)���� ��������Ѱ��ֵΪk��Ԫ�أ�
    value = bucket->items[key];                                              // ���ظ�Ԫ�صĵ�����, ����, ����map.end() 
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
    return false;      //ͰΪ�ջ�δ�ҵ��ж�Ӧ�Ĺؼ��ֵĽڵ㣬����false 
  }
  bucket->items.erase(key);  //map.erase() �����Ƴ����Ͳ���ƥ���Ԫ�أ�Ȼ�󷵻����Ƴ�Ԫ�صĸ���
  return true;
}

template <typename K, typename V>
int ExtendibleHash<K, V>::getBucketIndex(const K &key) {
  return HashKey(key) & ((1 << globalDepth) - 1);   //��1����globalDepthλ���൱��2^GlobalDepth ���ż�һ����λ��ȫ��1��
}                                                   // �͹ؼ���hashcode������������Ϳ��Եõ��ؼ��ֶ�Ӧ��bucketλ�� 

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

  while (targetBucket->items.size() == bucketMaxSize) { //����Ͱ���, ��Ҫ����   
    if (targetBucket->localDepth == globalDepth) {   //���L==G����ʱֻ��һ��ָ��ָ��ǰͰ������չ������
      size_t length = bucketTable.size();            //����λ��Ⱥ�ȫ��λ��Ⱦ�+1��������������鵱ǰͰ��Ԫ��  vector����һ�� ָ�븴�� 
      for (size_t i = 0; i < length; i++) {            
        bucketTable.push_back(bucketTable[i]);         
      }
      globalDepth++;
    }                 
    int mask = 1 << targetBucket->localDepth;  //���L<G����ʱ��ֹһ��ָ��ָ��ǰͰ���ʲ���Ҫ���������
    numBucket++;                               //ֻ����ѳ�һ��Ͱ��������λ���+1��Ȼ�����鵱ǰͰԪ�ؼ��ɡ�
    auto zeroBucket = std::make_shared<Bucket>(targetBucket->localDepth + 1);   //�����ƣ�ֻ������0 1 
    auto oneBucket = std::make_shared<Bucket>(targetBucket->localDepth + 1);
    for (auto item : targetBucket->items) {    //����ͰԪ�� 
      size_t hashkey = HashKey(item.first);    //ȡ��ϣֵ 
      if (hashkey & mask) {           //��������� �����λ��1 ����onebucket ��֮ 
        oneBucket->items.insert(item);
      } else { 
        zeroBucket->items.insert(item);
      }
    }

    for (size_t i = 0; i < bucketTable.size(); i++) {
      if (bucketTable[i] == targetBucket) {              //����ָ�� 
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

  targetBucket->items[key] = value;    //δ���� 
}

template class ExtendibleHash<page_id_t, Page *>;
template class ExtendibleHash<Page *, std::list<Page *>::iterator>;
// test purpose
template class ExtendibleHash<int, std::string>;
template class ExtendibleHash<int, std::list<int>::iterator>;
template class ExtendibleHash<int, int>;
} // namespace scudb
