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


Page *BufferPoolManager::FetchPage(page_id_t page_id) {   ////将page从磁盘中取到内存池
 	assert(page_id != INVALID_PAGE_ID);      //如果它的条件返回错误，则终止程序执行
	std::lock_guard<std::mutex> lock(latch_);     //在构造函数中自动绑定它的互斥体并加锁，在析构函数中解锁

	Page *res = nullptr;
	if (page_table_->Find(page_id, res))   //若该页在pages_中存在, 直接pin它
	{
		// mark the Page as pinned
		++res->pin_count_;  //当缓冲池中的某页被一线程读写时, 应不允许其他线程为了获取新页将该页从缓冲池中牺牲, 即在缓冲池中固定(pin)
		// remove its entry from LRUReplacer
		replacer_->Erase(res); //用该工具不再跟踪该页的最近最少用的使用情况, 直到所有线程对该页的读写访问全部结束.
		return res;
	}
	else                        
	{
		if (!free_list_->empty())   //如果不存在，则从空闲链表(首先查找)中找到一个旧页来容纳新页。
		{
			res = free_list_->front();  //list::front()此函数用于引用列表容器的第一个元素。
			free_list_->pop_front();    //pop_front()用于从列表容器的开头弹出（删除）元素。
		}
		else                       //在LRU置换器找旧页容纳新页，判断是否有页可被牺牲, 用replacer决定牺牲的页框号, 并以新代旧
		{
			if (!replacer_->Victim(res))  //说明此时不能牺牲任何一页 所有的页状态均为pin 
			{                             
				return nullptr;      //Replacer跟踪与所有元素相比最近最少访问的对象并删除，
			}                        //将其页号存储在输出参数中并返回True，为空则返回False。
		}
	}

	assert(res->pin_count_ == 0);              //有没有被pin的页，有页可被牺牲, 
	if (res->is_dirty_)                                             //若旧页被写过(dirty),
	{                                                               
		disk_manager_->WritePage(res->page_id_, res->GetData());   //调用disk_manager_->WritePage()先将旧页的最新内容写回磁盘
	}                                                             
	// delete the entry for old page.
	page_table_->Remove(res->page_id_);

	// insert an entry for the new page.
	page_table_->Insert(page_id, res);

	// initial meta data
	res->page_id_ = page_id;
	res->is_dirty_ = false;
	res->pin_count_ = 1;
	disk_manager_->ReadPage(page_id, res->GetData()); //调用disk_manager_->ReadPage()将磁盘中该页的内容写到该页框中

	return res;
}

/*
 * Implementation of unpin page
 * if pin_count>0, decrement it and if it becomes zero, put it back to
 * replacer if pin_count<=0 before this call, return false. is_dirty: set the
 * dirty flag of this page
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {  //取消固定页面的实现 
  std::lock_guard<std::mutex> lock(latch_);

	Page *res = nullptr;
	if (!page_table_->Find(page_id, res))
	{
		return false;
	}
	else
	{
		if (res->pin_count_ > 0)            //如果为 pin_count>0， 
		{
			if (--res->pin_count_ == 0)    //则递减它，如果变为零，则将这页重新放到LRU置换器中去，该页可被牺牲 
			{
				replacer_->Insert(res);
			}
		}
		else
		{
			return false;
		}

		if (is_dirty)          //设置此页面的脏标志
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
bool BufferPoolManager::FlushPage(page_id_t page_id) { //将缓冲池的特定页刷新到磁盘 
  	std::lock_guard<std::mutex> lock(latch_);

	if (page_id == INVALID_PAGE_ID)    //确保page_id ！= INVALID_PAGE_ID
		return false;

	Page *res = nullptr;
	if (page_table_->Find(page_id, res))
	{
		disk_manager_->WritePage(page_id, res->GetData());   //调用disk_manager_->WritePage()将最新内容写回磁盘
		return true;
	}
	return false;     //如果在页表中找不到页面，则返回 false
}

/**
 * User should call this method for deleting a page. This routine will call
 * disk manager to deallocate the page. First, if page is found within page
 * table, buffer pool manager should be reponsible for removing this entry out
 * of page table, reseting page metadata and adding back to free list. Second,
 * call disk manager's DeallocatePage() method to delete from disk file. If
 * the page is found within page table, but pin_count != 0, return false
 */
bool BufferPoolManager::DeletePage(page_id_t page_id) { //删除页面 
  	std::lock_guard<std::mutex> lock(latch_);

	Page *res = nullptr;
	if(page_table_->Find(page_id, res))
	{
		page_table_->Remove(page_id);     //将该页从hash表中删除
		res->page_id_ = INVALID_PAGE_ID;
		res->is_dirty_ = false;

		replacer_->Erase(res);      ////将该页从置换器中删除 
		disk_manager_->DeallocatePage(page_id); //调用磁盘管理器的 DeallocatePage（）方法从磁盘文档中删除 

		free_list_->push_back(res);    //adding back to free list. Second 添加回空闲链表 

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
Page *BufferPoolManager::NewPage(page_id_t &page_id) {    //创建新页面
  	std::lock_guard<std::mutex> lock(latch_);

	Page *res = nullptr;
	if(!free_list_->empty())     //从空闲链表(首先查找)中找到一个旧页来容纳新页。
	{
		res = free_list_->front();
		free_list_->pop_front();
	}                             //在LRU置换器找旧页容纳新页
	else
	{
		if(!replacer_->Victim(res))       //说明此时不能牺牲任何一页
		{
			return nullptr;
		}
	}
                                         //有页可牺牲 前面已经用replace决定牺牲的页框号 

	page_id = disk_manager_->AllocatePage();   //调用磁盘管理器来分配页面 获取一个新的页号
	if(res->is_dirty_)          //为脏页, 则写回磁盘, 在页表中清除旧页id
	{
		disk_manager_->WritePage(res->page_id_, res->GetData());
	}

	page_table_->Remove(res->page_id_);   //移除旧页 

	page_table_->Insert(page_id, res);   //插入新页 

	res->page_id_ = page_id;  //page_id设为新页号
	res->is_dirty_ = false;  //脏页标志置为false
	res->pin_count_ = 1;    //线程计数置为1
	res->ResetMemory();   //清空该页内容

	return res;
}
} // namespace scudb
