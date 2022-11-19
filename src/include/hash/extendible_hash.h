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
    int localDepth;      //�ֲ���� ÿ��Ͱ��һ������λ��ȣ���ʾ��ǰͰ��Ԫ�صĵͼ�λ��һ���ġ�
    map<K, V> items;     //����key��ֱ�ӽ��з��ʵ����ݽṹ������ͨ���ѹؼ���ӳ�䵽����һ��λ�������ʼ�¼
  };                  //Ͱ�Ǵ��¼�ĵط�
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
  int getBucketIndex(const K &key);     //������һ���ؼ��֣�����ǹؼ��ֶ�Ӧ��bucketλ��
  int globalDepth;         //ȫ����ȱ�ʾȡ��ϣֵ�ĵͼ�λ��Ϊ����, ���ҹ�ϣ����������������ʼ�յ���2^Global Depth
  size_t bucketMaxSize;    //ÿ��bucket��װ�ļ�ֵ�Ե����ֵΪbucketMaxSize   size_t ���ͱ�ʾC���κζ������ܴﵽ����󳤶ȣ������޷������͡�
  int numBucket;            //��ϣ������ĳ���
  vector<std::shared_ptr<Bucket>> bucketTable;   //��ϣ��  
  mutex mutex1;      //������mutex 

};
} // namespace scudb