#include "buffer/buffer_pool_manager.h"

namespace scudb {

/*
 * BufferPoolManager Constructor
 * When log_manager is nullptr, logging is disabled (for test purpose)
 * WARNING: Do Not Edit This Function
 */
BufferPoolManager::BufferPoolManager(size_t pool_size,
                                                 DiskManager *disk_manager,
                                                 LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager),
      log_manager_(log_manager) {
  // a consecutive memory space for buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHash<page_id_t, Page *>(BUCKET_SIZE);
  replacer_ = new LRUReplacer<Page *>;
  free_list_ = new std::list<Page *>;

  // put all the pages into free list
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_->push_back(&pages_[i]);
  }
}

/*
 * BufferPoolManager Deconstructor
 * WARNING: Do Not Edit This Function
 */
BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
  delete free_list_;
}


Page *BufferPoolManager::FetchPage(page_id_t page_id) {   ////��page�Ӵ�����ȡ���ڴ��
 	assert(page_id != INVALID_PAGE_ID);      //��������������ش�������ֹ����ִ��
	std::lock_guard<std::mutex> lock(latch_);     //�ڹ��캯�����Զ������Ļ����岢�����������������н���

	Page *res = nullptr;
	if (page_table_->Find(page_id, res))   //����ҳ��pages_�д���, ֱ��pin��
	{
		// mark the Page as pinned
		++res->pin_count_;  //��������е�ĳҳ��һ�̶߳�дʱ, Ӧ�����������߳�Ϊ�˻�ȡ��ҳ����ҳ�ӻ����������, ���ڻ�����й̶�(pin)
		// remove its entry from LRUReplacer
		replacer_->Erase(res); //�øù��߲��ٸ��ٸ�ҳ����������õ�ʹ�����, ֱ�������̶߳Ը�ҳ�Ķ�д����ȫ������.
		return res;
	}
	else                        
	{
		if (!free_list_->empty())   //��������ڣ���ӿ�������(���Ȳ���)���ҵ�һ����ҳ��������ҳ��
		{
			res = free_list_->front();  //list::front()�˺������������б������ĵ�һ��Ԫ�ء�
			free_list_->pop_front();    //pop_front()���ڴ��б������Ŀ�ͷ������ɾ����Ԫ�ء�
		}
		else                       //��LRU�û����Ҿ�ҳ������ҳ���ж��Ƿ���ҳ�ɱ�����, ��replacer����������ҳ���, �����´���
		{
			if (!replacer_->Victim(res))  //˵����ʱ���������κ�һҳ ���е�ҳ״̬��Ϊpin 
			{                             
				return nullptr;      //Replacer����������Ԫ�����������ٷ��ʵĶ���ɾ����
			}                        //����ҳ�Ŵ洢����������в�����True��Ϊ���򷵻�False��
		}
	}

	assert(res->pin_count_ == 0);              //��û�б�pin��ҳ����ҳ�ɱ�����, 
	if (res->is_dirty_)                                             //����ҳ��д��(dirty),
	{                                                               
		disk_manager_->WritePage(res->page_id_, res->GetData());   //����disk_manager_->WritePage()�Ƚ���ҳ����������д�ش���
	}                                                             
	// delete the entry for old page.
	page_table_->Remove(res->page_id_);

	// insert an entry for the new page.
	page_table_->Insert(page_id, res);

	// initial meta data
	res->page_id_ = page_id;
	res->is_dirty_ = false;
	res->pin_count_ = 1;
	disk_manager_->ReadPage(page_id, res->GetData()); //����disk_manager_->ReadPage()�������и�ҳ������д����ҳ����

	return res;
}

/*
 * Implementation of unpin page
 * if pin_count>0, decrement it and if it becomes zero, put it back to
 * replacer if pin_count<=0 before this call, return false. is_dirty: set the
 * dirty flag of this page
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {  //ȡ���̶�ҳ���ʵ�� 
  std::lock_guard<std::mutex> lock(latch_);

	Page *res = nullptr;
	if (!page_table_->Find(page_id, res))
	{
		return false;
	}
	else
	{
		if (res->pin_count_ > 0)            //���Ϊ pin_count>0�� 
		{
			if (--res->pin_count_ == 0)    //��ݼ����������Ϊ�㣬����ҳ���·ŵ�LRU�û�����ȥ����ҳ�ɱ����� 
			{
				replacer_->Insert(res);
			}
		}
		else
		{
			return false;
		}

		if (is_dirty)          //���ô�ҳ������־
		{
			res->is_dirty_ = true;
		}
		return true;
	}
}

/*
 * Used to flush a particular page of the buffer pool to disk. Should call the
 * write_page method of the disk manager
 * if page is not found in page table, return false
 * NOTE: make sure page_id != INVALID_PAGE_ID
 */
bool BufferPoolManager::FlushPage(page_id_t page_id) { //������ص��ض�ҳˢ�µ����� 
  	std::lock_guard<std::mutex> lock(latch_);

	if (page_id == INVALID_PAGE_ID)    //ȷ��page_id ��= INVALID_PAGE_ID
		return false;

	Page *res = nullptr;
	if (page_table_->Find(page_id, res))
	{
		disk_manager_->WritePage(page_id, res->GetData());   //����disk_manager_->WritePage()����������д�ش���
		return true;
	}
	return false;     //�����ҳ�����Ҳ���ҳ�棬�򷵻� false
}

/**
 * User should call this method for deleting a page. This routine will call
 * disk manager to deallocate the page. First, if page is found within page
 * table, buffer pool manager should be reponsible for removing this entry out
 * of page table, reseting page metadata and adding back to free list. Second,
 * call disk manager's DeallocatePage() method to delete from disk file. If
 * the page is found within page table, but pin_count != 0, return false
 */
bool BufferPoolManager::DeletePage(page_id_t page_id) { //ɾ��ҳ�� 
  	std::lock_guard<std::mutex> lock(latch_);

	Page *res = nullptr;
	if(page_table_->Find(page_id, res))
	{
		page_table_->Remove(page_id);     //����ҳ��hash����ɾ��
		res->page_id_ = INVALID_PAGE_ID;
		res->is_dirty_ = false;

		replacer_->Erase(res);      ////����ҳ���û�����ɾ�� 
		disk_manager_->DeallocatePage(page_id); //���ô��̹������� DeallocatePage���������Ӵ����ĵ���ɾ�� 

		free_list_->push_back(res);    //adding back to free list. Second ��ӻؿ������� 

		return true;
	}
	return false; 
}

/**
 * User should call this method if needs to create a new page. This routine
 * will call disk manager to allocate a page.
 * Buffer pool manager should be responsible to choose a victim page either
 * from free list or lru replacer(NOTE: always choose from free list first),
 * update new page's metadata, zero out memory and add corresponding entry
 * into page table. return nullptr if all the pages in pool are pinned
 */
Page *BufferPoolManager::NewPage(page_id_t &page_id) {    //������ҳ��
  	std::lock_guard<std::mutex> lock(latch_);

	Page *res = nullptr;
	if(!free_list_->empty())     //�ӿ�������(���Ȳ���)���ҵ�һ����ҳ��������ҳ��
	{
		res = free_list_->front();
		free_list_->pop_front();
	}                             //��LRU�û����Ҿ�ҳ������ҳ
	else
	{
		if(!replacer_->Victim(res))       //˵����ʱ���������κ�һҳ
		{
			return nullptr;
		}
	}
                                         //��ҳ������ ǰ���Ѿ���replace����������ҳ��� 

	page_id = disk_manager_->AllocatePage();   //���ô��̹�����������ҳ�� ��ȡһ���µ�ҳ��
	if(res->is_dirty_)          //Ϊ��ҳ, ��д�ش���, ��ҳ���������ҳid
	{
		disk_manager_->WritePage(res->page_id_, res->GetData());
	}

	page_table_->Remove(res->page_id_);   //�Ƴ���ҳ 

	page_table_->Insert(page_id, res);   //������ҳ 

	res->page_id_ = page_id;  //page_id��Ϊ��ҳ��
	res->is_dirty_ = false;  //��ҳ��־��Ϊfalse
	res->pin_count_ = 1;    //�̼߳�����Ϊ1
	res->ResetMemory();   //��ո�ҳ����

	return res;
}
} // namespace scudb
